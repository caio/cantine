use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{
    CheckCondition, CollectionResult, ConditionForSegment, ConditionalTopCollector,
    ConditionalTopSegmentCollector,
};

pub trait ScoreModifier<T>: 'static {
    fn modify(&self, doc_id: DocId, score: Score) -> T;
}

impl<F, T> ScoreModifier<T> for F
where
    F: 'static + Fn(DocId, Score) -> T,
{
    fn modify(&self, doc_id: DocId, score: Score) -> T {
        (self)(doc_id, score)
    }
}

pub trait ModifierForSegment<T>: Sync {
    type Type: ScoreModifier<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Type;
}

impl<T, C, F> ModifierForSegment<T> for F
where
    F: 'static + Sync + Send + Fn(&SegmentReader) -> C,
    C: ScoreModifier<T>,
{
    type Type = C;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
        (self)(reader)
    }
}

pub struct TweakedScoreTopSegmentCollector<T, C, F>
where
    C: CheckCondition<T>,
{
    modifier: F,
    collector: ConditionalTopSegmentCollector<T, C>,
}

impl<T, C, F> TweakedScoreTopSegmentCollector<T, C, F>
where
    T: PartialOrd + Copy,
    C: CheckCondition<T>,
    F: ScoreModifier<T>,
{
    fn new(segment_id: SegmentLocalId, limit: usize, condition: C, modifier: F) -> Self {
        Self {
            modifier,
            collector: ConditionalTopSegmentCollector::new(segment_id, limit, condition),
        }
    }
}

impl<T, C, F> SegmentCollector for TweakedScoreTopSegmentCollector<T, C, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    C: CheckCondition<T>,
    F: ScoreModifier<T>,
{
    type Fruit = CollectionResult<T>;

    fn collect(&mut self, doc: DocId, score: Score) {
        let score = self.modifier.modify(doc, score);
        self.collector.visit(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.collector.into_collection_result()
    }
}

pub struct TweakedScoreTopCollector<T, C, F>
where
    C: ConditionForSegment<T>,
{
    modifier_factory: F,
    condition_factory: C,
    collector: ConditionalTopCollector<T, C>,
}

impl<T, C, F> TweakedScoreTopCollector<T, C, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    C: ConditionForSegment<T>,
    F: 'static + Sync + ModifierForSegment<T>,
{
    pub fn new(limit: usize, condition_factory: C, modifier_factory: F) -> Self {
        Self {
            collector: ConditionalTopCollector::with_limit(limit, condition_factory.clone()),
            modifier_factory,
            condition_factory,
        }
    }
}

impl<T, C, F> Collector for TweakedScoreTopCollector<T, C, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    C: ConditionForSegment<T> + Sync,
    F: 'static + ModifierForSegment<T>,
{
    type Fruit = CollectionResult<T>;
    type Child = TweakedScoreTopSegmentCollector<T, C::Type, F::Type>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(self.collector.merge_many(children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let modifier = self.modifier_factory.for_segment(reader);
        Ok(TweakedScoreTopSegmentCollector::new(
            segment_id,
            self.collector.limit,
            self.condition_factory.for_segment(reader),
            modifier,
        ))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use tantivy::{query::AllQuery, schema::SchemaBuilder, Document, Index};

    #[test]
    fn integration() -> Result<()> {
        let builder = SchemaBuilder::new();
        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        for _ in 0..100 {
            writer.add_document(Document::new());
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let colletor = TweakedScoreTopCollector::new(100, true, |_: &SegmentReader| {
            |doc_id: DocId, score: Score| f64::from(score) * f64::from(doc_id)
        });

        let result = searcher.search(&AllQuery, &colletor)?;

        assert_eq!(100, result.items.len());
        let mut item_iter = result.items.into_iter();
        let mut last_score = item_iter.next().unwrap().0;

        // An AllQuery ends up with every doc scoring the same, so
        // this means highest ids will come first
        for item in item_iter {
            assert!(last_score > item.0);
            last_score = item.0;
        }

        Ok(())
    }
}
