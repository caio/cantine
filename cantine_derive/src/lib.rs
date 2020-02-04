use std::{marker::PhantomData, ops::Range};

use serde::Serialize;
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Result, Score, SegmentLocalId, SegmentReader,
};

pub use cantine_derive_internal::FilterAndAggregation;

#[derive(Serialize, Debug, Clone)]
pub struct RangeStats<T> {
    pub min: T,
    pub max: T,
    pub count: u64,
}

impl<T> RangeStats<T>
where
    T: PartialOrd + Copy,
{
    pub fn collect(&mut self, value: T) {
        if self.min > value {
            self.min = value;
        }

        if self.max < value {
            self.max = value;
        }

        self.count += 1;
    }

    pub fn merge(&mut self, other: &Self) {
        if self.min > other.min {
            self.min = other.min;
        }

        if self.max < other.max {
            self.max = other.max;
        }

        self.count += other.count;
    }
}

impl<T> From<&Range<T>> for RangeStats<T>
where
    T: PartialOrd + Copy,
{
    fn from(src: &Range<T>) -> Self {
        Self {
            min: src.end,
            max: src.start,
            count: 0,
        }
    }
}

pub trait Mergeable: Send + Sync {
    fn merge_same_size(&mut self, other: &Self);
}

pub trait Feature<TQuery>: Sync {
    type Agg: Mergeable + for<'a> From<&'a TQuery>;

    fn collect_into(&self, query: &TQuery, agg: &mut Self::Agg);
}

pub trait FeatureForSegment<T>: Sync {
    type Output: FeatureForDoc<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Output;
}

impl<T, F, O> FeatureForSegment<T> for F
where
    F: Sync + Fn(&SegmentReader) -> O,
    O: FeatureForDoc<T>,
{
    type Output = O;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Output {
        (self)(reader)
    }
}

struct FeatureCollector<T, Q, F> {
    query: Q,
    reader_factory: F,
    _marker: PhantomData<T>,
}

impl<T, A, Q, F, O> FeatureCollector<T, Q, F>
where
    T: 'static + Feature<Q, Agg = A>,
    Q: 'static + Clone + Sync,
    A: 'static + Mergeable + for<'a> From<&'a Q>,
    F: FeatureForSegment<T, Output = O>,
    O: 'static + FeatureForDoc<T>,
{
    pub fn new(query: Q, reader_factory: F) -> Self {
        Self {
            query,
            reader_factory,
            _marker: PhantomData,
        }
    }
}

impl<T, A, Q, F, O> Collector for FeatureCollector<T, Q, F>
where
    T: 'static + Feature<Q, Agg = A>,
    Q: 'static + Clone + Sync,
    A: 'static + Mergeable + for<'a> From<&'a Q>,
    F: FeatureForSegment<T, Output = O>,
    O: 'static + FeatureForDoc<T>,
{
    type Fruit = A;
    type Child = FeatureSegmentCollector<T, A, Q, O>;

    fn for_segment(
        &self,
        _segment_id: SegmentLocalId,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(FeatureSegmentCollector {
            agg: A::from(&self.query),
            query: self.query.clone(),
            reader: self.reader_factory.for_segment(segment_reader),
            _marker: PhantomData,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut iter = fruits.into_iter();

        let mut first = iter.next().unwrap_or_else(|| A::from(&self.query));

        for fruit in iter {
            first.merge_same_size(&fruit);
        }

        Ok(first)
    }
}

pub trait FeatureForDoc<T> {
    fn for_doc(&self, doc: DocId) -> Option<T>;
}

impl<T, F> FeatureForDoc<T> for F
where
    F: Fn(DocId) -> Option<T>,
{
    fn for_doc(&self, doc: DocId) -> Option<T> {
        (self)(doc)
    }
}

struct FeatureSegmentCollector<T, A, Q, F> {
    agg: A,
    query: Q,
    reader: F,
    _marker: PhantomData<T>,
}

impl<T, A, Q, F> SegmentCollector for FeatureSegmentCollector<T, A, Q, F>
where
    T: 'static + Feature<Q, Agg = A>,
    Q: 'static,
    A: 'static + Mergeable + for<'a> From<&'a Q>,
    F: 'static + FeatureForDoc<T>,
{
    type Fruit = A;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if let Some(item) = self.reader.for_doc(doc) {
            item.collect_into(&self.query, &mut self.agg);
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.agg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{convert::TryInto, ops::Range};

    use tantivy::{query::AllQuery, schema::SchemaBuilder, Document, Index};

    struct Metadata {
        a: i16,
        b: u16,
    }

    // XXX Who will test the tests?
    impl Metadata {
        pub fn as_bytes(&self) -> [u8; 4] {
            let mut out = [0u8; 4];
            out[0..2].copy_from_slice(&self.a.to_le_bytes());
            out[2..].copy_from_slice(&self.b.to_le_bytes());
            out
        }

        pub fn from_bytes(src: [u8; 4]) -> Self {
            let a = i16::from_le_bytes(src[0..2].try_into().unwrap());
            let b = u16::from_le_bytes(src[2..].try_into().unwrap());
            Self { a, b }
        }
    }

    #[derive(Debug, Default)]
    struct MetaAgg {
        a: usize,
        b: usize,
    }

    impl Mergeable for MetaAgg {
        fn merge_same_size(&mut self, other: &Self) {
            self.a += other.a;
            self.b += other.b;
        }
    }

    #[derive(Clone)]
    struct LessThanMetaQuery {
        a: i16,
        b: u16,
    }

    impl From<&LessThanMetaQuery> for MetaAgg {
        fn from(_src: &LessThanMetaQuery) -> Self {
            Self::default()
        }
    }

    impl Feature<LessThanMetaQuery> for Metadata {
        type Agg = MetaAgg;

        fn collect_into(&self, query: &LessThanMetaQuery, agg: &mut Self::Agg) {
            if self.a < query.a {
                agg.a += 1;
            }
            if self.b < query.b {
                agg.b += 1;
            }
        }
    }

    #[derive(Clone)]
    struct CountARangesQuery(Vec<Range<i16>>);

    impl From<&CountARangesQuery> for Vec<i16> {
        fn from(src: &CountARangesQuery) -> Self {
            vec![0; src.0.len()]
        }
    }

    impl Mergeable for Vec<i16> {
        fn merge_same_size(&mut self, other: &Self) {
            for (idx, tally) in other.iter().enumerate() {
                self[idx] += tally;
            }
        }
    }

    impl Feature<CountARangesQuery> for Metadata {
        type Agg = Vec<i16>;

        fn collect_into(&self, query: &CountARangesQuery, agg: &mut Self::Agg) {
            for (idx, range) in query.0.iter().enumerate() {
                if range.contains(&self.a) {
                    agg[idx] += 1;
                }
            }
        }
    }

    #[test]
    fn pass() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        let bytes_field = builder.add_bytes_field("metadata_as_bytes");

        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        let add_doc = |meta: Metadata| {
            let mut doc = Document::new();
            doc.add_bytes(bytes_field, meta.as_bytes().to_vec());
            writer.add_document(doc);
        };

        add_doc(Metadata { a: -1, b: 1 });
        add_doc(Metadata { a: -2, b: 2 });
        add_doc(Metadata { a: -3, b: 3 });
        add_doc(Metadata { a: -4, b: 4 });

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let less_than_collector = FeatureCollector::<Metadata, _, _>::new(
            // So we want count:
            //  * Every document that has "a" < -1
            //  * Every document that has "b" < 2
            LessThanMetaQuery { a: -1, b: 2 },
            move |reader: &SegmentReader| {
                let bytes_reader = reader.fast_fields().bytes(bytes_field).unwrap();

                move |doc_id| {
                    let metadata_bytes = bytes_reader.get_bytes(doc_id);
                    metadata_bytes.try_into().ok().map(Metadata::from_bytes)
                }
            },
        );

        let a_ranges_collector = FeatureCollector::<Metadata, _, _>::new(
            // And here we'll get a count for:
            //  * Every doc that a is within -10..0 (4)
            //  * Every doc that a is within 0..10 (0)
            //  * Every doc that a is within -2..4 (2)
            CountARangesQuery(vec![-10..0, 0..10, -2..4]),
            move |reader: &SegmentReader| {
                let bytes_reader = reader.fast_fields().bytes(bytes_field).unwrap();

                move |doc_id| {
                    let metadata_bytes = bytes_reader.get_bytes(doc_id);
                    metadata_bytes.try_into().ok().map(Metadata::from_bytes)
                }
            },
        );

        let (agg, a_range_counts) =
            searcher.search(&AllQuery, &(less_than_collector, a_ranges_collector))?;

        assert_eq!(3, agg.a);
        assert_eq!(1, agg.b);
        assert_eq!(vec![4, 0, 2], a_range_counts);

        Ok(())
    }
}
