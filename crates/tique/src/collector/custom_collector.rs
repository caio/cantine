use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{SearchMarker, TopCollector, TopSegmentCollector};

pub struct CustomScoreTopCollector<T, F> {
    scorer_factory: F,
    collector: TopCollector<T>,
}

impl<T, F> CustomScoreTopCollector<T, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    F: 'static + Sync + DocScorerFactory<T>,
{
    pub fn new(limit: usize, after: Option<SearchMarker<T>>, scorer_factory: F) -> Self {
        Self {
            scorer_factory,
            collector: TopCollector::with_limit(limit, after),
        }
    }

    fn limit(&self) -> usize {
        self.collector.limit
    }

    fn after(&self) -> Option<SearchMarker<T>> {
        self.collector.after.clone()
    }
}

pub trait DocScorerFactory<T>: Sync {
    type Type: DocScorer<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Type;
}

impl<T, C, F> DocScorerFactory<T> for F
where
    F: 'static + Sync + Send + Fn(&SegmentReader) -> C,
    C: DocScorer<T>,
{
    type Type = C;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
        (self)(reader)
    }
}

impl<T, F> Collector for CustomScoreTopCollector<T, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    F: 'static + DocScorerFactory<T>,
{
    type Fruit = Vec<SearchMarker<T>>;
    type Child = CustomScoreTopSegmentCollector<T, F::Type>;

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
            self.limit(),
            self.after(),
            scorer,
        ))
    }
}

pub struct CustomScoreTopSegmentCollector<T, F> {
    scorer: F,
    collector: TopSegmentCollector<T>,
}

impl<T, F> CustomScoreTopSegmentCollector<T, F>
where
    T: PartialOrd + Copy,
    F: DocScorer<T>,
{
    fn new(
        segment_id: SegmentLocalId,
        limit: usize,
        after: Option<SearchMarker<T>>,
        scorer: F,
    ) -> Self {
        Self {
            scorer,
            collector: TopSegmentCollector::new(segment_id, limit, after),
        }
    }
}

impl<T, F> SegmentCollector for CustomScoreTopSegmentCollector<T, F>
where
    T: 'static + PartialOrd + Copy + Sync + Send,
    F: 'static + DocScorer<T>,
{
    type Fruit = Vec<SearchMarker<T>>;

    fn collect(&mut self, doc: DocId, _: Score) {
        let score = self.scorer.score(doc);
        self.collector.visit(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.collector.into_vec()
    }
}

pub trait DocScorer<T> {
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
        let mut collector = CustomScoreTopSegmentCollector::new(0, 1, None, |doc_id| doc_id);

        // So that whatever we provide as a score
        collector.collect(1, 42.0);
        let res = collector.harvest();
        assert_eq!(1, res.len());

        let got = &res[0];
        // Is disregarded and doc_id is used instead
        assert_eq!(got.doc.1, got.score)
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

        let colletor = CustomScoreTopCollector::new(2, None, |_: &SegmentReader| {
            // Score is doc_id * 10
            |doc_id: DocId| doc_id * 10
        });

        let topdocs = searcher.search(&AllQuery, &colletor)?;

        assert_eq!(2, topdocs.len());

        // So we expect that the highest score is 990
        assert_eq!(topdocs[0].score, 990);
        assert_eq!(topdocs[1].score, 980);

        Ok(())
    }
}
