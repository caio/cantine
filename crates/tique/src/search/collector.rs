use std::ops::AddAssign;

use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::BytesFastFieldReader,
    schema::Field,
    Result, SegmentReader,
};

use crate::search::{AggregationRequest, FeatureValue, FeatureVector};

pub type FeatureRanges<T> = Vec<Option<Vec<T>>>;

trait Zero {
    fn zero() -> Self;
}

impl Zero for FeatureValue {
    fn zero() -> FeatureValue {
        0
    }
}

fn raw_merge_feature_ranges<'a, T>(dest: &'a mut FeatureRanges<T>, src: &'a FeatureRanges<T>)
where
    T: AddAssign<&'a T> + Zero + Clone,
{
    debug_assert_eq!(dest.len(), src.len());
    // All I'm doing here is summing a sparse x dense matrix. Rice?
    for (i, mine) in dest.iter_mut().enumerate() {
        if let Some(ranges) = &src[i] {
            let dest_ranges = mine.get_or_insert_with(|| vec![T::zero(); ranges.len()]);
            raw_merge_ranges(dest_ranges, &ranges);
        }
    }
}

fn raw_merge_ranges<'a, T>(dest: &'a mut [T], src: &'a [T])
where
    T: AddAssign<&'a T>,
{
    debug_assert_eq!(dest.len(), src.len());
    for (i, src_item) in src.iter().enumerate() {
        dest[i] += src_item;
    }
}

pub struct FeatureCollector<T> {
    field: Field,
    agg: FeatureRanges<T>,
    wanted: AggregationRequest,
}

pub struct FeatureSegmentCollector<T> {
    // do I need agg here?
    agg: FeatureRanges<T>,
    reader: BytesFastFieldReader,
    wanted: AggregationRequest,
}

// XXX I can't seem to be able to make <FeatureValue> parametric :(

impl FeatureCollector<FeatureValue> {
    pub fn for_field(
        field: Field,
        num_features: usize,
        wanted: &AggregationRequest,
    ) -> FeatureCollector<FeatureValue> {
        FeatureCollector {
            field,
            wanted: wanted.clone(),
            agg: vec![None; num_features],
        }
    }
}

impl Collector for FeatureCollector<FeatureValue> {
    type Fruit = FeatureRanges<FeatureValue>;
    type Child = FeatureSegmentCollector<FeatureValue>;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(FeatureSegmentCollector {
            agg: vec![None; self.agg.len()],
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

    fn merge_fruits(&self, children: Vec<FeatureRanges<FeatureValue>>) -> Result<Self::Fruit> {
        let mut merged = FeatureRanges::with_capacity(self.agg.len());
        merged.resize(self.agg.len(), None);

        raw_merge_feature_ranges(&mut merged, &self.agg);

        for child in children {
            raw_merge_feature_ranges(&mut merged, &child)
        }

        Ok(merged)
    }
}

impl SegmentCollector for FeatureSegmentCollector<FeatureValue> {
    type Fruit = FeatureRanges<FeatureValue>;

    fn collect(&mut self, doc: u32, _score: f32) {
        let data = self.reader.get_bytes(doc);
        let doc_features = FeatureVector::parse(self.agg.len(), data).unwrap();

        for (feat, ranges) in &self.wanted {
            // Wanted contains a feature that goes beyond num_features
            if *feat > self.agg.len() {
                // XXX Add visibility to when this happens
                continue;
            }

            let opt = doc_features.get(*feat);

            // Document doesn't have this feature: Nothing to do
            if opt.is_none() {
                continue;
            }

            let value = opt.unwrap();

            // Index/Count ranges in the order they were requested
            for (idx, range) in ranges.iter().enumerate() {
                if range.contains(&value) {
                    self.agg
                        .get_mut(*feat)
                        .expect("agg should have been initialized by now")
                        .get_or_insert_with(|| vec![0; ranges.len()])[idx] += 1;
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

    const A: usize = 0;
    const B: usize = 1;
    const C: usize = 2;
    const D: usize = 3;

    #[test]
    fn range_vec_merge() {
        let mut ra = vec![0u16, 0];
        // Merging with a fresh one shouldn't change counts
        raw_merge_ranges(&mut ra, &vec![0, 0]);
        assert_eq!(0, ra[0]);
        assert_eq!(0, ra[1]);

        // Zeroed ra: count should update to be the same as its src
        raw_merge_ranges(&mut ra, &vec![3, 0]);
        assert_eq!(3, ra[0]);
        assert_eq!(0, ra[1]);

        // And everything should increase properly
        raw_merge_ranges(&mut ra, &vec![417, 710]);
        assert_eq!(420, ra[0]);
        assert_eq!(710, ra[1]);
    }

    #[test]
    fn feature_ranges_merge() {
        let mut a: FeatureRanges<u16> = vec![None, None];

        raw_merge_feature_ranges(&mut a, &vec![None, None]);
        assert_eq!(None, a[0]);
        assert_eq!(None, a[1]);

        // Empty merged with filled: copy
        {
            let src = vec![Some(vec![1]), Some(vec![2, 3])];
            raw_merge_feature_ranges(&mut a, &src);

            assert_eq!(Some(vec![1]), a[0]);
            assert_eq!(Some(vec![2, 3]), a[1]);
        }

        // Non empty: just update ranges
        {
            let src = vec![Some(vec![41]), Some(vec![0, 4])];
            raw_merge_feature_ranges(&mut a, &src);

            assert_eq!(Some(vec![42]), a[0]);
            assert_eq!(Some(vec![2, 7]), a[1]);
        }
    }

    #[test]
    fn usage() -> Result<()> {
        // First we create a basic index where there schema is just a bytes field
        let mut sb = SchemaBuilder::new();
        let field = sb.add_bytes_field("bytes");
        let schema = sb.build();

        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_with_num_threads(1, 40_000_000)?;

        let add_doc = |fv: FeatureVector<&mut [u8], usize>| -> Result<()> {
            let mut doc = Document::default();
            doc.add_bytes(field, fv.as_bytes().to_owned());
            writer.add_document(doc);
            Ok(())
        };

        // And we populate it with a couple of docs where
        // the bytes field is a features::FeatureVector
        let num_features = 4;
        let empty_buffer = vec![std::u8::MAX; 4 * 2];

        {
            // Doc{ A: 5, B: 10}
            let mut buf = empty_buffer.clone();
            let mut fv = FeatureVector::parse(num_features, buf.as_mut_slice()).unwrap();
            fv.set(A, 5).unwrap();
            fv.set(B, 10).unwrap();
            add_doc(fv)?;
        }

        {
            // Doc{ A: 7, C: 2}
            let mut buf = empty_buffer.clone();
            let mut fv = FeatureVector::parse(num_features, buf.as_mut_slice()).unwrap();
            fv.set(A, 7).unwrap();
            fv.set(C, 2).unwrap();
            add_doc(fv)?;
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let wanted: AggregationRequest = vec![
            // feature A between ranges 2-10 and 0-5
            (A, vec![2..=10, 0..=5]),
            // and so on...
            (B, vec![9..=100, 420..=710]),
            (C, vec![2..=2]),
            (D, vec![]),
        ];

        let feature_ranges = searcher.search(
            &AllQuery,
            &FeatureCollector::for_field(field, num_features, &wanted),
        )?;

        // { A => { "2-10": 2, "0-5": 1 } }
        assert_eq!(Some(vec![2u16, 1]), feature_ranges[A]);
        // { B => { "9-100": 1, "420-710": 0 } }
        assert_eq!(Some(vec![1, 0]), feature_ranges[B]);
        // { C => { "2" => 1 } }
        assert_eq!(Some(vec![1]), feature_ranges[C]);
        // Asking to count a feature but providing no ranges should no-op
        assert_eq!(None, feature_ranges[D]);

        Ok(())
    }
}
