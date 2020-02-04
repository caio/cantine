use std::ops::Range;

use serde::Serialize;
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Result, Score, SegmentLocalId, SegmentReader,
};

pub use cantine_derive_internal::{AggregationQuery, FilterQuery};

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

pub trait Aggregator<Q, F>: Send + Sync {
    fn merge_same_size(&mut self, other: &Self);
    fn collect(&mut self, query: &Q, feature: &F);
    fn from_query(query: &Q) -> Self;
}

pub trait Feature: Sized + Sync {
    type Query: Sync + Clone;
    type Agg: Aggregator<Self::Query, Self>;
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

pub struct FeatureCollector<T: Feature, F> {
    query: T::Query,
    reader_factory: F,
}

impl<T, F, O> FeatureCollector<T, F>
where
    T: 'static + Feature,
    F: FeatureForSegment<T, Output = O>,
    O: 'static + FeatureForDoc<T>,
{
    pub fn new(query: T::Query, reader_factory: F) -> Self {
        Self {
            query,
            reader_factory,
        }
    }
}

impl<T, F, O> Collector for FeatureCollector<T, F>
where
    T: 'static + Feature,
    F: FeatureForSegment<T, Output = O>,
    O: 'static + FeatureForDoc<T>,
{
    type Fruit = T::Agg;
    type Child = FeatureSegmentCollector<T, O>;

    fn for_segment(
        &self,
        _segment_id: SegmentLocalId,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(FeatureSegmentCollector {
            agg: T::Agg::from_query(&self.query),
            query: self.query.clone(),
            reader: self.reader_factory.for_segment(segment_reader),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut iter = fruits.into_iter();

        let mut first = iter
            .next()
            .unwrap_or_else(|| T::Agg::from_query(&self.query));

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

pub struct FeatureSegmentCollector<T: Feature, F> {
    agg: T::Agg,
    query: T::Query,
    reader: F,
}

impl<T, F> SegmentCollector for FeatureSegmentCollector<T, F>
where
    T: 'static + Feature,
    F: 'static + FeatureForDoc<T>,
{
    type Fruit = T::Agg;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if let Some(item) = self.reader.for_doc(doc) {
            self.agg.collect(&self.query, &item);
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

    // XXX Who will test the tests?
    impl Aggregator<Vec<Range<i16>>, i16> for Vec<i16> {
        fn merge_same_size(&mut self, other: &Self) {
            for (idx, tally) in other.iter().enumerate() {
                self[idx] += tally;
            }
        }
        fn collect(&mut self, query: &Vec<Range<i16>>, feature: &i16) {
            for (idx, range) in query.iter().enumerate() {
                if range.contains(&feature) {
                    self[idx] += 1;
                }
            }
        }

        fn from_query(query: &Vec<Range<i16>>) -> Self {
            vec![0; query.len()]
        }
    }

    impl Feature for i16 {
        type Query = Vec<Range<i16>>;
        type Agg = Vec<i16>;
    }

    #[test]
    fn pass() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        let bytes_field = builder.add_bytes_field("metadata_as_bytes");

        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        for i in -4i16..0 {
            let mut doc = Document::new();
            doc.add_bytes(bytes_field, i.to_le_bytes().to_vec());
            writer.add_document(doc);
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let ranges_collector = FeatureCollector::<i16, _>::new(
            vec![-10..0, 0..10, -2..4],
            move |reader: &SegmentReader| {
                let bytes_reader = reader.fast_fields().bytes(bytes_field).unwrap();

                move |doc_id| {
                    bytes_reader
                        .get_bytes(doc_id)
                        .try_into()
                        .ok()
                        .map(i16::from_le_bytes)
                }
            },
        );

        let range_counts = searcher.search(&AllQuery, &ranges_collector)?;

        assert_eq!(vec![4, 0, 2], range_counts);

        Ok(())
    }
}
