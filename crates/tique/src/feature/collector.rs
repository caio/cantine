use std::{
    mem::size_of,
    ops::{AddAssign, RangeInclusive},
};

use tantivy::{
    collector::{Collector, SegmentCollector},
    fastfield::BytesFastFieldReader,
    schema::Field,
    Result, SegmentReader,
};

use byteorder::{ByteOrder, NativeEndian};

pub type AggregationRequest<T> = Vec<(usize, Vec<RangeInclusive<T>>)>;
pub type FeatureRanges<T> = Vec<Option<Vec<T>>>;

fn merge_feature_ranges<'a, T>(
    dest: &'a mut FeatureRanges<T>,
    src: &'a [Option<Vec<T>>],
) -> Result<()>
where
    T: AddAssign<&'a T> + Clone,
{
    if dest.len() == src.len() {
        // All I'm doing here is summing a sparse x dense matrix. Rice?
        for (i, mine) in dest.iter_mut().enumerate() {
            if let Some(ranges) = &src[i] {
                if let Some(current) = mine {
                    merge_ranges(current, &ranges)?;
                } else {
                    mine.replace(ranges.clone());
                }
            }
        }
        Ok(())
    } else {
        Err(tantivy::TantivyError::SystemError(
            "Tried to merge uneven feature ranges".to_owned(),
        ))
    }
}

fn merge_ranges<'a, T>(dest: &'a mut [T], src: &'a [T]) -> Result<()>
where
    T: AddAssign<&'a T>,
{
    if dest.len() == src.len() {
        for (i, src_item) in src.iter().enumerate() {
            dest[i] += src_item;
        }
        Ok(())
    } else {
        Err(tantivy::TantivyError::SystemError(
            "Tried to merge uneven range vecs".to_owned(),
        ))
    }
}

pub struct FeatureCollector<T> {
    field: Field,
    num_features: usize,
    wanted: AggregationRequest<T>,
    unset_value: Option<T>,
}

pub struct FeatureSegmentCollector<T> {
    agg: FeatureRanges<T>,
    reader: BytesFastFieldReader,
    wanted: AggregationRequest<T>,
    unset_value: Option<T>,
}

impl<T> FeatureCollector<T>
where
    for<'a> T: Copy + AddAssign<&'a T>,
{
    pub fn for_field(
        field: Field,
        num_features: usize,
        unset_value: Option<T>,
        wanted: &[(usize, Vec<RangeInclusive<T>>)],
    ) -> FeatureCollector<T> {
        FeatureCollector {
            field,
            num_features,
            unset_value,
            wanted: wanted.to_vec(),
        }
    }
}

macro_rules! collector_impl {
    ($t: ty, $reader: expr) => {
        impl Collector for FeatureCollector<$t> {
            type Fruit = FeatureRanges<$t>;
            type Child = FeatureSegmentCollector<$t>;

            fn for_segment(
                &self,
                _segment_local_id: u32,
                segment_reader: &SegmentReader,
            ) -> Result<Self::Child> {
                Ok(FeatureSegmentCollector {
                    agg: vec![None; self.num_features],
                    wanted: self.wanted.clone(),
                    reader: segment_reader
                        .fast_fields()
                        .bytes(self.field)
                        .expect("Field is not a bytes fast field."),
                    unset_value: self.unset_value,
                })
            }

            fn requires_scoring(&self) -> bool {
                false
            }

            fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
                // TODO check if is it worth it to short-circuit the very common
                //      case where children.len() == 1
                let mut merged = vec![None; self.num_features];

                for child in children {
                    merge_feature_ranges(&mut merged, &child)?;
                }

                Ok(merged)
            }
        }

        impl SegmentCollector for FeatureSegmentCollector<$t> {
            type Fruit = FeatureRanges<$t>;

            fn collect(&mut self, doc: u32, _score: f32) {
                let data = self.reader.get_bytes(doc);

                for (feat, ranges) in &self.wanted {
                    let start_offset = *feat * size_of::<$t>();
                    let end_offset = start_offset + size_of::<$t>();

                    if data.len() < end_offset {
                        // XXX Add visibility to when this happens
                        continue;
                    }

                    let value = $reader(&data[start_offset..end_offset]);

                    if let Some(unset_value) = self.unset_value {
                        if value == unset_value {
                            continue;
                        }
                    }

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
    };
}

collector_impl!(u16, NativeEndian::read_u16);
collector_impl!(u32, NativeEndian::read_u32);
collector_impl!(u64, NativeEndian::read_u64);
collector_impl!(i16, NativeEndian::read_i16);
collector_impl!(i32, NativeEndian::read_i32);
collector_impl!(i64, NativeEndian::read_i64);

#[cfg(test)]
mod tests {

    use super::*;
    use zerocopy::AsBytes;

    use tantivy::{
        self,
        query::AllQuery,
        schema::{Document, SchemaBuilder},
        Index,
    };

    #[test]
    fn cannot_merge_uneven_rangevec() {
        assert!(merge_ranges(&mut [0u16], &[1, 2]).is_err());
    }

    #[test]
    fn cannot_merge_uneven_feature_ranges() {
        assert!(merge_feature_ranges::<u16>(&mut vec![None], &[None, None]).is_err());
    }

    #[test]
    fn range_vec_merge() -> Result<()> {
        let mut ra = vec![0u16, 0];
        // Merging with a fresh one shouldn't change counts
        merge_ranges(&mut ra, &[0, 0])?;
        assert_eq!(0, ra[0]);
        assert_eq!(0, ra[1]);

        // Zeroed ra: count should update to be the same as its src
        merge_ranges(&mut ra, &[3, 0])?;
        assert_eq!(3, ra[0]);
        assert_eq!(0, ra[1]);

        // And everything should increase properly
        merge_ranges(&mut ra, &[417, 710])?;
        assert_eq!(420, ra[0]);
        assert_eq!(710, ra[1]);

        Ok(())
    }

    #[test]
    fn feature_ranges_merge() -> Result<()> {
        let mut a: FeatureRanges<u16> = vec![None, None];

        merge_feature_ranges(&mut a, &[None, None])?;
        assert_eq!(None, a[0]);
        assert_eq!(None, a[1]);

        // Empty merged with filled: copy
        {
            let src = vec![Some(vec![1]), Some(vec![2, 3])];
            merge_feature_ranges(&mut a, &src)?;

            assert_eq!(Some(vec![1]), a[0]);
            assert_eq!(Some(vec![2, 3]), a[1]);
        }

        // Non empty: just update ranges
        {
            let src = vec![Some(vec![41]), Some(vec![0, 4])];
            merge_feature_ranges(&mut a, &src)?;

            assert_eq!(Some(vec![42]), a[0]);
            assert_eq!(Some(vec![2, 7]), a[1]);
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
        let mut writer = index.writer_with_num_threads(1, 40_000_000)?;

        const A: usize = 0;
        const B: usize = 1;
        const C: usize = 2;
        const D: usize = 3;

        const NUM_FEATURES: usize = 4;
        const UNSET: u16 = std::u16::MAX;

        let add_doc = |fv: &[u16; NUM_FEATURES]| -> Result<()> {
            let mut doc = Document::default();
            doc.add_bytes(field, fv.as_bytes().to_owned());
            writer.add_document(doc);
            Ok(())
        };

        // And we populate it with a couple of docs where
        // the bytes field is a features::FeatureVector
        add_doc(&[5, 10, UNSET, UNSET])?;
        add_doc(&[7, UNSET, 2, UNSET])?;

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let wanted: AggregationRequest<u16> = vec![
            // feature A between ranges 2-10 and 0-5, etc
            (A, vec![2..=10, 0..=5]),
            (B, vec![9..=100, 420..=710]),
            (C, vec![2..=2]),
            (D, vec![]),
        ];

        let feature_ranges = searcher.search(
            &AllQuery,
            &FeatureCollector::for_field(field, NUM_FEATURES, Some(UNSET), &wanted),
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
