use std::collections::HashMap;
use std::sync::Arc;

use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::BytesFastFieldReader,
    schema::Field,
    Result, SegmentReader,
};

use super::features::{BytesVector, Feature, FeatureVector, NUM_FEATURES};

#[derive(Debug)]
pub struct Range(String, u16, u16);

impl Range {
    pub fn contains(&self, item: u16) -> bool {
        let Range(_, start, end) = self;
        // XXX inclusive or exclusive?
        item >= *start && item <= *end
    }
}
type RangeNameToCount = HashMap<String, u16>;
type FeatureToRangeCounts = HashMap<Feature, RangeNameToCount>;

#[derive(Debug)]
pub struct Aggregations(FeatureToRangeCounts);

impl Aggregations {
    pub fn get(&self, feat: &Feature) -> Option<&RangeNameToCount> {
        let Aggregations(inner) = self;
        inner.get(feat)
    }

    pub fn merge(&mut self, other: &Aggregations) {
        let Aggregations(agg) = self;
        let Aggregations(other_agg) = other;

        for (feat, aggs) in other_agg.iter() {
            if !agg.contains_key(&feat) {
                agg.insert(*feat, HashMap::with_capacity(aggs.len()));
            }

            for (name, count) in aggs {
                let range_aggs = agg.get_mut(&feat).unwrap();

                if !range_aggs.contains_key(name) {
                    range_aggs.insert(name.clone(), *count);
                } else {
                    let tally = range_aggs.get_mut(name).unwrap();
                    *tally += count;
                }
            }
        }
    }
}

type WantedAggregations = Vec<(Feature, Vec<Range>)>;

pub struct FeatureAggreagator {
    field: Field,
    agg: Aggregations,
    wanted: Arc<WantedAggregations>,
}

pub struct FeatureSegmentCollector {
    agg: Aggregations,
    reader: BytesFastFieldReader,
    wanted: Arc<WantedAggregations>,
}

impl FeatureAggreagator {
    pub fn for_field(field: Field, wanted: WantedAggregations) -> FeatureAggreagator {
        FeatureAggreagator {
            field,
            agg: Aggregations(HashMap::with_capacity(NUM_FEATURES)),
            wanted: Arc::new(wanted),
        }
    }
}

impl Collector for FeatureAggreagator {
    type Fruit = Aggregations;
    type Child = FeatureSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<FeatureSegmentCollector> {
        Ok(FeatureSegmentCollector {
            agg: Aggregations(HashMap::with_capacity(NUM_FEATURES)),
            wanted: self.wanted.clone(),
            reader: segment_reader
                .fast_fields()
                .bytes(self.field)
                .expect("Field is not a bytes fast field."),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Aggregations>) -> Result<Aggregations> {
        let mut inner = HashMap::with_capacity(NUM_FEATURES);
        let mut result: Aggregations = Aggregations(inner);

        for (feat, ranges) in self.wanted.iter() {
            for child in children.iter() {
                result.merge(child);
            }
        }

        result.merge(&self.agg);

        Ok(result)
    }
}

impl SegmentCollector for FeatureSegmentCollector {
    type Fruit = Aggregations;

    fn collect(&mut self, doc: u32, _score: f32) {
        let mut my_agg = &mut self.agg;
        let Aggregations(inner) = my_agg;

        let mut data = self.reader.get_bytes(doc).to_owned();
        // XXX Am I holding this right?
        let (doc_features, _rest) = BytesVector::parse(&mut data).unwrap();

        for (feat, ranges) in self.wanted.iter() {
            println!("Wanted {}: {:?}", feat, ranges);
            let opt = doc_features.get(feat);

            // Document doesn't have this feature: compute nothing
            if opt.is_none() {
                println!("  But didn't find the feature");
                continue;
            }

            let doc_val = opt.unwrap();
            println!("  The feature has value {}", doc_val);

            for range in ranges.iter() {
                println!("  The range {:?}", range);
                // Range doesn't match with doc's value: do nothing
                if !range.contains(doc_val.get()) {
                    println!("    Doesn't match with the feature");
                    continue;
                }

                if !inner.contains_key(&feat) {
                    println!("    Is new feature-level");
                    inner.insert(*feat, HashMap::new());
                }

                let Range(name, _start, _end) = range;
                let range_aggs = inner.get_mut(&feat).unwrap();

                if !range_aggs.contains_key(name) {
                    println!("    Is new range-level");
                    range_aggs.insert(name.clone(), 1);
                } else {
                    println!("    Has been incremented!");
                    let tally = range_aggs.get_mut(name).unwrap();
                    *tally += 1;
                }
            }
        }
    }

    fn harvest(self) -> <Self as SegmentCollector>::Fruit {
        self.agg
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use tantivy::{
        self,
        query::AllQuery,
        schema::{Document, SchemaBuilder},
        Index,
    };

    const A: &Feature = &Feature::NumIngredients;
    const B: &Feature = &Feature::FatContent;
    const C: &Feature = &Feature::PrepTime;

    #[test]
    fn correct_merges() {
        // Starting with an empty agg {}
        let mut inner: FeatureToRangeCounts = HashMap::with_capacity(NUM_FEATURES);
        let mut original = Aggregations(inner);

        // Merging with one line { A => { existing => 1, solo => 1 } }
        let mut first: FeatureToRangeCounts = HashMap::with_capacity(NUM_FEATURES);
        let mut rs: RangeNameToCount = HashMap::new();
        rs.insert("existing".to_owned(), 1);
        rs.insert("solo".to_owned(), 1);
        first.insert(*A, rs);

        // Should leave us with the same state as first
        original.merge(&Aggregations(first));
        let after_first_merge = original.get(A).unwrap();
        assert_eq!(2, after_first_merge.len());
        assert_eq!(Some(&1), after_first_merge.get("existing"));
        assert_eq!(Some(&1), after_first_merge.get("solo"));

        // Now we'll merge with one like { A => { existing => 3 }, B => { new => 2 } }
        let mut second: FeatureToRangeCounts = HashMap::with_capacity(NUM_FEATURES);
        let mut rsa: RangeNameToCount = HashMap::new();
        let mut rsb: RangeNameToCount = HashMap::new();
        rsa.insert("existing".to_owned(), 3);
        rsb.insert("new".to_owned(), 2);
        second.insert(*A, rsa);
        second.insert(*B, rsb);

        // So we'll want { A => { existing => 4, solo => 1 }, B => { new => 2 } }
        original.merge(&Aggregations(second));
        let after_second_merge_a = original.get(A).unwrap();
        let after_second_merge_b = original.get(B).unwrap();
        assert_eq!(2, after_second_merge_a.len());
        assert_eq!(1, after_second_merge_b.len());
        assert_eq!(Some(&4), after_second_merge_a.get("existing"));
        assert_eq!(Some(&1), after_second_merge_a.get("solo"));
        assert_eq!(Some(&2), after_second_merge_b.get("new"));
    }

    #[test]
    fn usage() -> Result<()> {
        let mut sb = SchemaBuilder::new();
        let field = sb.add_bytes_field("bytes");
        let schema = sb.build();

        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_with_num_threads(1, 10_000_000)?;

        let mut add_doc = |fv: FeatureVector<&mut [u8]>| -> Result<()> {
            let mut doc = Document::default();
            doc.add_bytes(field, fv.as_bytes().to_owned());
            writer.add_document(doc);
            writer.commit()?;
            Ok(())
        };

        let mut doc_feature_a_buf = BytesVector::new_buf();
        let (mut doc_a, _) = BytesVector::parse(&mut doc_feature_a_buf).unwrap();
        let mut doc_feature_b_buf = BytesVector::new_buf();
        let (mut doc_b, _) = BytesVector::parse(&mut doc_feature_b_buf).unwrap();

        // Doc{ A: 5, B: 10, C: nil}
        doc_a.set(A, 5);
        doc_a.set(B, 10);

        // Doc{ A: 7, B: nil, C: 2}
        doc_b.set(A, 7);

        add_doc(doc_a);
        add_doc(doc_b);

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let wanted: WantedAggregations = vec![
            // { A => { "2-10": 2, "0-4": 1 } }
            (
                *A,
                vec![
                    Range("2-10".to_owned(), 2, 10),
                    Range("0-4".to_owned(), 0, 4),
                ],
            ),
            // { B => { "9-100": 1, "420-710": 0 } }
            (
                *B,
                vec![
                    Range("9-100".to_owned(), 9, 100),
                    Range("420-710".to_owned(), 420, 710),
                ],
            ),
            // {}
            (*C, vec![]),
        ];

        let result = searcher.search(&AllQuery, &FeatureAggreagator::for_field(field, wanted))?;

        dbg!(result);

        Ok(())
    }
}
