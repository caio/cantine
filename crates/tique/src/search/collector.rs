use std::collections::HashMap;

use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::BytesFastFieldReader,
    schema::Field,
    Result, SegmentReader,
};

use crate::search::{AggregationRequest, Feature, FeatureVector};

#[derive(Debug)]
pub struct FeatureRanges(Vec<Option<RangeVec>>);

impl FeatureRanges {
    fn merge(&mut self, other: &FeatureRanges) -> Result<()> {
        let FeatureRanges(inner) = self;

        if inner.len() != other.len() {
            return Err(tantivy::Error::SystemError(
                "Cannot merge FeatureRanges of different sizes".to_owned(),
            ));
        }

        for i in 0..inner.len() {
            // For every Some() RangeVec in the other
            if let Some(other_rv) = &other.get(i) {
                inner
                    .get_mut(i)
                    .expect("Bound by self.len(), should never happen")
                    .get_or_insert_with(|| RangeVec::new(other_rv.len()))
                    .merge(other_rv)?;
            }
        }
        Ok(())
    }

    fn len(&self) -> usize {
        let FeatureRanges(inner) = self;
        inner.len()
    }

    fn new(size: usize) -> Self {
        assert!(size != 0);
        FeatureRanges(vec![None; size])
    }

    fn get(&self, idx: usize) -> &Option<RangeVec> {
        assert!(idx < self.len());
        let FeatureRanges(inner) = self;
        &inner[idx]
    }

    fn get_mut(&mut self, idx: usize) -> &mut Option<RangeVec> {
        assert!(idx < self.len());
        let FeatureRanges(inner) = self;
        inner
            .get_mut(idx)
            .expect("Invariant: get_mut should always work")
    }
}

impl Into<HashMap<Feature, Vec<u16>>> for FeatureRanges {
    fn into(self) -> HashMap<Feature, Vec<u16>> {
        let mut res = HashMap::new();
        for feat in Feature::VALUES.iter() {
            if let Some(counts) = self.get(*feat as usize) {
                let RangeVec(inner) = counts;
                res.insert(*feat, inner.clone());
            }
        }

        res
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct RangeVec(Vec<u16>);

impl RangeVec {
    fn new(size: usize) -> Self {
        assert!(size != 0);
        RangeVec(vec![0; size])
    }

    fn merge(&mut self, other: &RangeVec) -> Result<()> {
        let RangeVec(storage) = self;
        if storage.len() == other.len() {
            for i in 0..storage.len() {
                storage[i] += other.get(i);
            }

            Ok(())
        } else {
            Err(tantivy::TantivyError::SystemError(
                "Tried to merge RangeVec of different sizes".to_owned(),
            ))
        }
    }

    fn len(&self) -> usize {
        let RangeVec(storage) = self;
        storage.len()
    }

    fn get(&self, idx: usize) -> u16 {
        assert!(idx < self.len());
        let RangeVec(storage) = self;
        storage[idx]
    }

    fn inc(&mut self, idx: usize) {
        assert!(idx < self.len());
        let RangeVec(storage) = self;
        storage[idx] += 1;
    }
}

pub struct FeatureCollector {
    field: Field,
    agg: FeatureRanges,
    wanted: AggregationRequest,
}

pub struct FeatureSegmentCollector {
    agg: FeatureRanges,
    reader: BytesFastFieldReader,
    wanted: AggregationRequest,
}

impl FeatureCollector {
    pub fn for_field(field: Field, wanted: AggregationRequest) -> FeatureCollector {
        FeatureCollector {
            field,
            agg: FeatureRanges::new(Feature::LENGTH),
            wanted: wanted,
        }
    }
}

impl Collector for FeatureCollector {
    type Fruit = FeatureRanges;
    type Child = FeatureSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: u32,
        segment_reader: &SegmentReader,
    ) -> Result<FeatureSegmentCollector> {
        Ok(FeatureSegmentCollector {
            agg: FeatureRanges::new(Feature::LENGTH),
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

    fn merge_fruits(&self, children: Vec<FeatureRanges>) -> Result<FeatureRanges> {
        let mut merged = FeatureRanges::new(Feature::LENGTH);

        merged.merge(&self.agg)?;

        for child in children {
            merged.merge(&child)?;
        }

        Ok(merged)
    }
}

impl SegmentCollector for FeatureSegmentCollector {
    type Fruit = FeatureRanges;

    fn collect(&mut self, doc: u32, _score: f32) {
        let data = self.reader.get_bytes(doc);
        let doc_features = FeatureVector::parse(data).unwrap();

        for (feat, ranges) in &self.wanted {
            let opt = doc_features.get(&feat);

            // Document doesn't have this feature: Nothing to do
            if opt.is_none() {
                continue;
            }

            let val = opt.unwrap();

            // Index/Count ranges in the order they were requested
            for (idx, range) in ranges.iter().enumerate() {
                let value = val.get();
                if value >= range[0] && value <= range[1] {
                    self.agg
                        .get_mut(*feat as usize)
                        .get_or_insert_with(|| RangeVec::new(ranges.len()))
                        .inc(idx);
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
    const D: &Feature = &Feature::TotalTime;

    #[test]
    fn cannot_merge_different_sized_range_vecs() {
        let mut ra = RangeVec::new(1);
        assert!(ra.merge(&RangeVec::new(2)).is_err());
    }

    #[test]
    fn range_vec_basic_usage() {
        let mut ra = RangeVec::new(1);
        assert_eq!(1, ra.len());

        assert_eq!(0, ra.get(0));
        ra.inc(0);
        assert_eq!(1, ra.get(0));
        ra.inc(0);
        assert_eq!(2, ra.get(0));
    }

    #[test]
    fn can_wrap_existing_vec() {
        let ra = RangeVec(vec![1, 0, 3]);
        assert_eq!(1, ra.get(0));
        assert_eq!(0, ra.get(1));
        assert_eq!(3, ra.get(2));
    }

    #[test]
    fn range_vec_merge() -> Result<()> {
        let mut ra = RangeVec::new(2);

        // Merging with a fresh one shouldn't change counts
        ra.merge(&RangeVec::new(2))?;
        assert_eq!(0, ra.get(0));
        assert_eq!(0, ra.get(1));

        // Zeroed ra: count should update to be the same as its src
        ra.merge(&RangeVec(vec![3, 0]))?;
        assert_eq!(3, ra.get(0));
        assert_eq!(0, ra.get(1));

        // And everything should increase properly
        ra.merge(&RangeVec(vec![417, 710]))?;
        assert_eq!(420, ra.get(0));
        assert_eq!(710, ra.get(1));

        Ok(())
    }

    #[test]
    fn feature_ranges_init() {
        let frs = FeatureRanges::new(2);
        assert_eq!(2, frs.len());

        assert_eq!(&None, frs.get(0));
        assert_eq!(&None, frs.get(1));
    }

    #[test]
    fn cannot_merge_different_sized_feature_ranges() {
        let mut a = FeatureRanges::new(1);
        assert!(a.merge(&FeatureRanges::new(2)).is_err());
    }

    #[test]
    fn cannot_merge_feature_ranges_with_uneven_ranges() {
        let mut a = FeatureRanges(vec![Some(RangeVec(vec![1]))]);
        let b = FeatureRanges(vec![Some(RangeVec(vec![1, 2]))]);
        assert_eq!(a.len(), b.len());
        // a.len() == b.len(), but the inner ranges aren't even
        assert!(a.merge(&b).is_err());
    }

    #[test]
    fn feature_ranges_merge() -> Result<()> {
        let mut a = FeatureRanges::new(2);

        // Merge with empty: nothing changes
        a.merge(&FeatureRanges::new(2))?;
        assert_eq!(&None, a.get(0));
        assert_eq!(&None, a.get(1));

        // Empty merged with filled: copy
        {
            let src = FeatureRanges(vec![Some(RangeVec(vec![1])), Some(RangeVec(vec![2, 3]))]);
            a.merge(&src)?;

            assert_eq!(&Some(RangeVec(vec![1])), a.get(0));
            assert_eq!(&Some(RangeVec(vec![2, 3])), a.get(1));
        }

        // Non empty: just update ranges
        {
            let src = FeatureRanges(vec![Some(RangeVec(vec![41])), Some(RangeVec(vec![0, 4]))]);
            a.merge(&src)?;

            assert_eq!(&Some(RangeVec(vec![42])), a.get(0));
            assert_eq!(&Some(RangeVec(vec![2, 7])), a.get(1));
        }

        Ok(())
    }

    #[test]
    fn usage() -> Result<()> {
        // First we create a basic index where there schema is just a bytes field
        let mut sb = SchemaBuilder::new();
        let field = sb.add_bytes_field("bytes");
        let schema = sb.build();

        let index = Index::create_in_ram(schema);
        let mut writer = index.writer_with_num_threads(1, 10_000_000)?;

        let add_doc = |fv: FeatureVector<&mut [u8]>| -> Result<()> {
            let mut doc = Document::default();
            doc.add_bytes(field, fv.as_bytes().to_owned());
            writer.add_document(doc);
            Ok(())
        };

        // And we populate it with a couple of docs where
        // the bytes field is a features::FeatureVector

        {
            // Doc{ A: 5, B: 10}
            let mut buf = Feature::EMPTY_BUFFER.to_vec();
            let mut fv = FeatureVector::parse(buf.as_mut_slice()).unwrap();
            fv.set(A, 5);
            fv.set(B, 10);
            add_doc(fv)?;
        }

        {
            // Doc{ A: 7, C: 2}
            let mut buf = Feature::EMPTY_BUFFER.to_vec();
            let mut fv = FeatureVector::parse(buf.as_mut_slice()).unwrap();
            fv.set(A, 7);
            fv.set(C, 2);
            add_doc(fv)?;
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let wanted: AggregationRequest = vec![
            // feature A between ranges 2-10 and 0-5
            (*A, vec![[2, 10], [0, 5]]),
            // and so on...
            (*B, vec![[9, 100], [420, 710]]),
            (*C, vec![[2, 2]]),
            (*D, vec![]),
        ];

        let feature_ranges =
            searcher.search(&AllQuery, &FeatureCollector::for_field(field, wanted))?;

        // { A => { "2-10": 2, "0-5": 1 } }
        assert_eq!(&Some(RangeVec(vec![2, 1])), feature_ranges.get(*A as usize));
        // { B => { "9-100": 1, "420-710": 0 } }
        assert_eq!(&Some(RangeVec(vec![1, 0])), feature_ranges.get(*B as usize));
        // { C => { "2" => 1 } }
        assert_eq!(&Some(RangeVec(vec![1])), feature_ranges.get(*C as usize));
        // Asking to count a feature but providing no ranges should no-op
        assert_eq!(&None, feature_ranges.get(*D as usize));

        Ok(())
    }

    #[test]
    fn empty_feature_ranges_becomes_empty_map() {
        assert_eq!(HashMap::new(), FeatureRanges::new(Feature::LENGTH).into());
    }

    #[test]
    fn feature_ranges_into_hashmap() {
        let mut fr = FeatureRanges::new(Feature::LENGTH);

        fr.get_mut(*A as usize).replace(RangeVec(vec![1, 2]));
        fr.get_mut(*B as usize).replace(RangeVec(vec![3]));
        fr.get_mut(*C as usize).replace(RangeVec(vec![4, 5, 6]));

        let as_map: HashMap<Feature, Vec<u16>> = fr.into();

        for feat in Feature::VALUES.iter() {
            match feat {
                A => assert_eq!(&vec![1, 2], as_map.get(A).unwrap()),
                B => assert_eq!(&vec![3], as_map.get(B).unwrap()),
                C => assert_eq!(&vec![4, 5, 6], as_map.get(C).unwrap()),
                _ => assert_eq!(false, as_map.contains_key(feat)),
            }
        }
    }
}
