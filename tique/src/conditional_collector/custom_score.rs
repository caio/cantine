use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, CustomScorer, CustomSegmentScorer, SegmentCollector},
    DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{
    top_collector::TopSegmentCollector,
    topk::{TopK, TopKProvider},
    traits::{CheckCondition, ConditionForSegment},
    CollectionResult,
};

/// A TopCollector that allows you to provide the score
///
/// # Example
///
///
/// ```rust
/// # use tique::conditional_collector::{CustomScoreTopCollector, Descending};
/// # use tantivy::{SegmentReader, DocId};
/// # let limit = 10;
/// # let condition = true;
///
/// // Any tantivy::collector::CustomScorer is valid
/// let scorer = |reader: &SegmentReader| {
///     |doc_id: DocId| -720
/// };
///
/// let custom_collector =
///     CustomScoreTopCollector::<i64, Descending, _, _>::new(limit, condition, scorer);
/// ```
pub struct CustomScoreTopCollector<T, P, C, S>
where
    T: PartialOrd,
    P: TopKProvider<T, DocId>,
    C: ConditionForSegment<T>,
{
    limit: usize,
    scorer_for_segment: S,
    condition_for_segment: C,
    _score: PhantomData<T>,
    _provider: PhantomData<P>,
}

impl<T, P, C, S> CustomScoreTopCollector<T, P, C, S>
where
    T: PartialOrd,
    P: TopKProvider<T, DocId>,
    C: ConditionForSegment<T>,
{
    pub fn new(limit: usize, condition_for_segment: C, scorer_for_segment: S) -> Self {
        Self {
            limit,
            scorer_for_segment,
            condition_for_segment,
            _score: PhantomData,
            _provider: PhantomData,
        }
    }
}

impl<T, P, C, S> Collector for CustomScoreTopCollector<T, P, C, S>
where
    T: 'static + PartialOrd + Copy + Send + Sync,
    P: 'static + Send + Sync + TopKProvider<T, DocId>,
    C: Sync + ConditionForSegment<T>,
    S: CustomScorer<T>,
{
    type Fruit = CollectionResult<T>;
    type Child = CustomScoreTopSegmentCollector<T, C::Type, S::Child, P::Child>;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(P::merge_many(self.limit, children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let scorer = self.scorer_for_segment.segment_scorer(reader)?;
        Ok(CustomScoreTopSegmentCollector::new(
            segment_id,
            P::new_topk(self.limit),
            scorer,
            self.condition_for_segment.for_segment(reader),
        ))
    }
}

pub struct CustomScoreTopSegmentCollector<T, C, S, K>
where
    C: CheckCondition<T>,
    K: TopK<T, DocId>,
{
    scorer: S,
    collector: TopSegmentCollector<T, K, C>,
}

impl<T, C, S, K> CustomScoreTopSegmentCollector<T, C, S, K>
where
    T: Copy,
    C: CheckCondition<T>,
    K: TopK<T, DocId>,
{
    pub fn new(segment_id: SegmentLocalId, topk: K, scorer: S, condition: C) -> Self {
        Self {
            scorer,
            collector: TopSegmentCollector::new(segment_id, topk, condition),
        }
    }
}

impl<T, C, S, K> SegmentCollector for CustomScoreTopSegmentCollector<T, C, S, K>
where
    T: 'static + PartialOrd + Copy + Send + Sync,
    K: 'static + TopK<T, DocId>,
    C: CheckCondition<T>,
    S: CustomSegmentScorer<T>,
{
    type Fruit = CollectionResult<T>;

    fn collect(&mut self, doc: DocId, _: Score) {
        let score = self.scorer.score(doc);
        self.collector.collect(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.collector.into_unsorted_collection_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conditional_collector::{topk::AscendingTopK, Descending};

    use tantivy::{query::AllQuery, schema::SchemaBuilder, Document, Index};

    #[test]
    fn custom_segment_scorer_gets_called() {
        let mut collector = CustomScoreTopSegmentCollector::new(
            0,
            AscendingTopK::new(1),
            // Use the doc_id as the score
            |doc_id| doc_id,
            true,
        );

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

        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        // We add 100 documents to our index
        for _ in 0..100 {
            writer.add_document(Document::new());
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let colletor =
            CustomScoreTopCollector::<_, Descending, _, _>::new(2, true, |_: &SegmentReader| {
                |doc_id: DocId| u64::from(doc_id * 10)
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
