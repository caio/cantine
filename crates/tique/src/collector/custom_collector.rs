use tantivy::{collector::SegmentCollector, DocId, Score, SegmentLocalId};

use super::{SearchMarker, TopSegmentCollector};

struct CustomScoreTopSegmentCollector<T, F> {
    scorer: F,
    collector: TopSegmentCollector<T>,
}

impl<T, F> CustomScoreTopSegmentCollector<T, F>
where
    T: PartialOrd + Copy,
    F: Fn(DocId) -> T,
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
    F: 'static + Fn(DocId) -> T,
{
    type Fruit = Vec<SearchMarker<T>>;

    fn collect(&mut self, doc: DocId, _: Score) {
        let score = (self.scorer)(doc);
        self.collector.visit(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.collector.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_scorer_gets_called() {
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
}
