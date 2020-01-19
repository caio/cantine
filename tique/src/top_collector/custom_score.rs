use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{
    CheckCondition, CollectionResult, ConditionForSegment, ConditionalTopCollector,
    ConditionalTopSegmentCollector,
};

pub struct CustomScoreTopCollector<T, C, F>
where
    C: ConditionForSegment<T>,
{
    scorer_factory: F,
    condition_factory: C,
    collector: ConditionalTopCollector<T, C>,
}

impl<T, C, F> CustomScoreTopCollector<T, C, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    C: ConditionForSegment<T>,
    F: 'static + Sync + ScorerForSegment<T>,
{
    pub fn new(limit: usize, condition_factory: C, scorer_factory: F) -> Self {
        Self {
            collector: ConditionalTopCollector::with_limit(limit, condition_factory.clone()),
            scorer_factory,
            condition_factory,
        }
    }
}

pub trait ScorerForSegment<T>: Sync {
    type Type: DocScorer<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Type;
}

impl<T, C, F> ScorerForSegment<T> for F
where
    F: 'static + Sync + Send + Fn(&SegmentReader) -> C,
    C: DocScorer<T>,
{
    type Type = C;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
        (self)(reader)
    }
}

impl<T, C, F> Collector for CustomScoreTopCollector<T, C, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    C: ConditionForSegment<T> + Sync,
    F: 'static + ScorerForSegment<T>,
{
    type Fruit = CollectionResult<T>;
    type Child = CustomScoreTopSegmentCollector<T, C::Type, F::Type>;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(self.collector.merge_many(children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let scorer = self.scorer_factory.for_segment(reader);
        Ok(CustomScoreTopSegmentCollector::new(
            segment_id,
            self.collector.limit,
            self.condition_factory.for_segment(reader),
            scorer,
        ))
    }
}

pub struct CustomScoreTopSegmentCollector<T, C, F>
where
    C: CheckCondition<T>,
{
    scorer: F,
    collector: ConditionalTopSegmentCollector<T, C>,
}

impl<T, C, F> CustomScoreTopSegmentCollector<T, C, F>
where
    T: PartialOrd + Copy,
    C: CheckCondition<T>,
    F: DocScorer<T>,
{
    fn new(segment_id: SegmentLocalId, limit: usize, condition: C, scorer: F) -> Self {
        Self {
            scorer,
            collector: ConditionalTopSegmentCollector::new(segment_id, limit, condition),
        }
    }
}

impl<T, C, F> SegmentCollector for CustomScoreTopSegmentCollector<T, C, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    C: CheckCondition<T>,
    F: DocScorer<T>,
{
    type Fruit = CollectionResult<T>;

    fn collect(&mut self, doc: DocId, _: Score) {
        let score = self.scorer.score(doc);
        self.collector.visit(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.collector.into_collection_result()
    }
}

pub trait DocScorer<T>: 'static {
    fn score(&self, doc_id: DocId) -> T;
}

impl<F, T> DocScorer<T> for F
where
    F: 'static + Sync + Send + Fn(DocId) -> T,
{
    fn score(&self, doc_id: DocId) -> T {
        (self)(doc_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::{query::AllQuery, schema::SchemaBuilder, Document, Index};

    #[test]
    fn custom_segment_scorer_gets_called() {
        // Use the doc_id as the score
        let mut collector = CustomScoreTopSegmentCollector::new(0, 1, true, |doc_id| doc_id);

        // So that whatever we provide as a score
        collector.collect(1, 42.0);
        let res = collector.harvest();
        assert_eq!(1, res.total);

        let got = &res.items[0];
        // Is disregarded and doc_id is used instead
        assert_eq!((got.1).1, got.0)
    }

    #[test]
    fn custom_top_scorer_integration() -> Result<()> {
        let builder = SchemaBuilder::new();
        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;

        // We add 100 documents to our index
        for _ in 0..100 {
            writer.add_document(Document::new());
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let colletor = CustomScoreTopCollector::new(2, true, |_: &SegmentReader| {
            // Score is doc_id * 10
            |doc_id: DocId| doc_id * 10
        });

        let result = searcher.search(&AllQuery, &colletor)?;

        assert_eq!(100, result.total);
        assert_eq!(2, result.items.len());

        // So we expect that the highest score is 990
        assert_eq!(result.items[0].0, 990);
        assert_eq!(result.items[1].0, 980);

        Ok(())
    }
}
