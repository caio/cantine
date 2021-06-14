use std::ops::Range;

use serde::Serialize;
use tantivy::{
    collector::{Collector, SegmentCollector},
    query::Query,
    schema::{IntOptions, Schema, SchemaBuilder},
    DocId, Document, Result, Score, SegmentOrdinal, SegmentReader,
};

pub use cantine_derive_internal::{Aggregable, Filterable};

pub trait Filterable: Sized {
    type Query;
    type Schema: FilterableSchema<Self, Self::Query>;

    fn create_schema<O: Into<IntOptions>>(builder: &mut SchemaBuilder, options: O) -> Self::Schema;
    fn load_schema(schema: &Schema) -> Result<Self::Schema>;
}

pub trait FilterableSchema<T, Q>: Sized {
    fn add_to_doc(&self, doc: &mut Document, item: &T);
    fn interpret(&self, query: &Q) -> Vec<Box<dyn Query>>;
}

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

pub trait Aggregable: Sized + Send + Sync {
    type Query: Send + Sync + Clone;
    type Agg: Aggregator<Self::Query, Self>;
}

pub trait AggregableForSegment<T>: Send + Sync {
    type Output: AggregableForDoc<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Output;
}

impl<T, F, O> AggregableForSegment<T> for F
where
    F: Send + Sync + Fn(&SegmentReader) -> O,
    O: AggregableForDoc<T>,
{
    type Output = O;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Output {
        (self)(reader)
    }
}

pub struct AggregableCollector<T: Aggregable, F> {
    query: T::Query,
    reader_factory: F,
}

impl<T, F, O> AggregableCollector<T, F>
where
    T: 'static + Aggregable,
    F: AggregableForSegment<T, Output = O>,
    O: 'static + AggregableForDoc<T>,
{
    pub fn new(query: T::Query, reader_factory: F) -> Self {
        Self {
            query,
            reader_factory,
        }
    }
}

impl<T, F, O> Collector for AggregableCollector<T, F>
where
    T: 'static + Aggregable,
    F: Send + AggregableForSegment<T, Output = O>,
    O: 'static + AggregableForDoc<T>,
{
    type Fruit = T::Agg;
    type Child = AggregableSegmentCollector<T, O>;

    fn for_segment(
        &self,
        _segment_ord: SegmentOrdinal,
        segment_reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(AggregableSegmentCollector {
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

pub trait AggregableForDoc<T> {
    fn for_doc(&self, doc: DocId) -> Option<T>;
}

impl<T, F> AggregableForDoc<T> for F
where
    F: Fn(DocId) -> Option<T>,
{
    fn for_doc(&self, doc: DocId) -> Option<T> {
        (self)(doc)
    }
}

pub struct AggregableSegmentCollector<T: Aggregable, F> {
    agg: T::Agg,
    query: T::Query,
    reader: F,
}

impl<T, F> SegmentCollector for AggregableSegmentCollector<T, F>
where
    T: 'static + Aggregable,
    F: 'static + AggregableForDoc<T>,
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

    use tantivy::{
        query::AllQuery,
        schema::{self, SchemaBuilder},
        Document, Index,
    };

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

    impl Aggregable for i16 {
        type Query = Vec<Range<i16>>;
        type Agg = Vec<i16>;
    }

    #[test]
    fn pass() -> Result<()> {
        let mut builder = SchemaBuilder::new();
        let bytes_field = builder.add_bytes_field("metadata_as_bytes", schema::FAST);

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

        let ranges_collector = AggregableCollector::<i16, _>::new(
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
