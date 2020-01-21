use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{
    topk::{TopK, TopKProvider},
    traits::{CheckCondition, ConditionForSegment},
};

pub struct TopCollector<T, P, CF> {
    limit: usize,
    condition_for_segment: CF,
    _score: PhantomData<T>,
    _provider: PhantomData<P>,
}

impl<T, P, CF> TopCollector<T, P, CF>
where
    T: PartialOrd,
    P: TopKProvider<T>,
    CF: ConditionForSegment<T>,
{
    pub fn new(limit: usize, condition_for_segment: CF) -> Self {
        if limit < 1 {
            panic!("Limit must be greater than 0");
        }
        TopCollector {
            limit,
            condition_for_segment,
            _score: PhantomData,
            _provider: PhantomData,
        }
    }
}

impl<P, CF> Collector for TopCollector<Score, P, CF>
where
    P: 'static + Send + Sync + TopKProvider<Score>,
    CF: Sync + ConditionForSegment<Score>,
{
    type Fruit = CollectionResult<Score>;
    type Child = TopSegmentCollector<Score, P::Child, CF::Type>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(P::merge_many(self.limit, children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(TopSegmentCollector::new(
            segment_id,
            P::new_topk(self.limit),
            self.condition_for_segment.for_segment(reader),
        ))
    }
}

pub struct TopSegmentCollector<T, K, C> {
    total: usize,
    visited: usize,
    segment_id: SegmentLocalId,
    topk: K,
    condition: C,
    _marker: PhantomData<T>,
}

impl<T, K, C> TopSegmentCollector<T, K, C>
where
    T: Copy,
    K: TopK<T, DocId>,
    C: CheckCondition<T>,
{
    pub fn new(segment_id: SegmentLocalId, topk: K, condition: C) -> Self {
        Self {
            total: 0,
            visited: 0,
            segment_id,
            topk,
            condition,
            _marker: PhantomData,
        }
    }

    #[cfg(test)]
    fn into_topk(self) -> K {
        self.topk
    }

    pub fn collect(&mut self, doc: DocId, score: T) {
        self.total += 1;
        if self
            .condition
            .check(self.segment_id, doc, score, K::ASCENDING)
        {
            self.visited += 1;
            self.topk.visit(score, doc);
        }
    }

    pub fn into_collection_result(self) -> CollectionResult<T> {
        let segment_id = self.segment_id;
        let items = self
            .topk
            .into_vec()
            .into_iter()
            .map(|(score, doc)| (score, DocAddress(segment_id, doc)))
            .collect();

        // XXX This is unsorted. It's ok because we sort during
        // merge, but using the same time to mean two things is
        // rather confusing
        CollectionResult {
            total: self.total,
            visited: self.visited,
            items,
        }
    }
}

impl<K, C> SegmentCollector for TopSegmentCollector<Score, K, C>
where
    K: 'static + TopK<Score, DocId>,
    C: CheckCondition<Score>,
{
    type Fruit = CollectionResult<Score>;

    fn collect(&mut self, doc: DocId, score: Score) {
        TopSegmentCollector::collect(self, doc, score)
    }

    fn harvest(self) -> Self::Fruit {
        TopSegmentCollector::into_collection_result(self)
    }
}

#[derive(Debug)]
pub struct CollectionResult<T> {
    pub total: usize,
    pub visited: usize,
    pub items: Vec<(T, DocAddress)>,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::conditional_collector::{
        topk::{AscendingTopK, DescendingTopK},
        Ascending, Descending,
    };

    use tantivy::{query::TermQuery, schema, Document, Index, Result, Term};

    #[test]
    fn condition_is_checked() {
        const LIMIT: usize = 4;

        let mut nil_collector = TopSegmentCollector::new(0, AscendingTopK::new(LIMIT), false);

        let mut top_collector = TopSegmentCollector::new(0, AscendingTopK::new(LIMIT), true);

        let condition = |_sid, doc, _score, _asc| doc % 2 == 1;

        let mut just_odds = TopSegmentCollector::new(0, AscendingTopK::new(LIMIT), condition);

        for i in 0..4 {
            nil_collector.collect(i, 420.0);
            top_collector.collect(i, 420.0);
            just_odds.collect(i, 420.0);
        }

        assert_eq!(0, nil_collector.harvest().items.len());
        assert_eq!(4, top_collector.harvest().items.len());

        // Verify that the collected items respect the condition
        let result = just_odds.harvest();
        assert_eq!(4, result.total);
        assert_eq!(2, result.items.len());
        for (score, doc) in result.items {
            let DocAddress(seg_id, doc_id) = doc;
            assert!(condition(seg_id, doc_id, score, true))
        }
    }

    fn check_segment_collector<K, C>(
        topk: K,
        condition: C,
        input: Vec<(Score, DocId)>,
        wanted: Vec<(Score, DocId)>,
    ) where
        K: TopK<Score, DocId> + 'static,
        C: CheckCondition<Score>,
    {
        let mut collector = TopSegmentCollector::new(0, topk, condition);

        for (score, id) in input {
            collector.collect(id, score);
        }

        assert_eq!(wanted, collector.into_topk().into_sorted_vec());
    }

    #[test]
    fn collection_with_a_marker_smoke() {
        // XXX property test maybe? Essentially we are creating
        // a Vec<(Score, DocId)> sorted as `Scored` would,
        // then we pick an arbitrary position to pivot and
        // expect the DescendingTopK to pick everything below
        // and the AscendingTopK to pick everything above
        let marker = (0.5, DocAddress(0, 4));

        check_segment_collector(
            DescendingTopK::new(10),
            marker,
            vec![
                // Every doc with a higher score has appeared already
                (0.6, 7),
                (0.7, 5),
                // Docs with the same score, but lower id too
                (0.5, 3),
                (0.5, 2),
                // [pivot] And, of course, the same doc should not be collected
                (0.5, 4),
                // Lower scores are in
                (0.0, 1),
                // Same score but higher doc, too
                (0.5, 6),
            ],
            vec![(0.5, 6), (0.0, 1)],
        );

        check_segment_collector(
            AscendingTopK::new(10),
            marker,
            vec![
                // Every doc with a higher score should be picked
                (0.6, 7),
                (0.7, 5),
                // Same score but lower id as well
                (0.5, 3),
                (0.5, 2),
                // [pivot] The same doc should not be collected
                (0.5, 4),
                // Docs with lower scores are discarded
                (0.0, 1),
                // Same score but higher doc is discaraded too
                (0.5, 6),
            ],
            vec![(0.5, 2), (0.5, 3), (0.6, 7), (0.7, 5)],
        );
    }

    #[test]
    fn collection_ordering_integration() -> Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let text_field = builder.add_text_field("text", schema::TEXT);

        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        let add_doc = |text: &str| {
            let mut doc = Document::new();
            doc.add_text(text_field, text);
            writer.add_document(doc);
        };

        const NUM_DOCS: usize = 3;
        add_doc("the first doc is simple");
        add_doc("the second doc is a bit larger");
        add_doc("and the third document is rubbish");

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let collector_asc = TopCollector::<_, Ascending, _>::new(NUM_DOCS, true);
        let collector_desc = TopCollector::<_, Descending, _>::new(NUM_DOCS, true);

        // Query for "the", which matches all docs and yields
        // a distinct score for each
        let query = TermQuery::new(
            Term::from_field_text(text_field, "the"),
            schema::IndexRecordOption::WithFreqsAndPositions,
        );
        let (asc, desc) = searcher.search(&query, &(collector_asc, collector_desc))?;

        assert_eq!(NUM_DOCS, asc.items.len());
        assert_eq!(NUM_DOCS, desc.items.len());

        let asc_scores = asc
            .items
            .iter()
            .map(|(score, _doc)| score)
            .collect::<Vec<_>>();

        let mut prev = None;
        for score in &asc_scores {
            if let Some(previous) = prev {
                assert!(previous < score, "The scores should be ascending");
            }
            prev = Some(score)
        }

        let mut desc_scores = desc
            .items
            .iter()
            .map(|(score, _doc)| score)
            .collect::<Vec<_>>();

        desc_scores.reverse();
        assert_eq!(asc_scores, desc_scores);

        Ok(())
    }
}
