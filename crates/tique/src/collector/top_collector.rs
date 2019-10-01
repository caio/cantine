use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{Scored, TopK};

pub trait CollectCondition<T>: 'static + Clone {
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T) -> bool;
}

impl<T> CollectCondition<T> for bool {
    fn check(&self, _: SegmentLocalId, _: DocId, _: T) -> bool {
        *self
    }
}

impl<F, T> CollectCondition<T> for F
where
    F: 'static + Clone + Fn(SegmentLocalId, DocId, T) -> bool,
{
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T) -> bool {
        (self)(segment_id, doc_id, score)
    }
}

pub type SearchMarker<T> = Scored<T, DocAddress>;

impl<T> CollectCondition<T> for SearchMarker<T>
where
    T: 'static + PartialOrd + Clone,
{
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T) -> bool {
        // So: only collect items that would come _after_ this marker
        *self > Scored::new(score, DocAddress(segment_id, doc_id))
    }
}

pub struct ConditionalTopCollector<T, F>
where
    F: CollectCondition<T>,
{
    pub limit: usize,
    condition: F,
    _marker: PhantomData<T>,
}

impl<T, F> ConditionalTopCollector<T, F>
where
    T: PartialOrd,
    F: CollectCondition<T>,
{
    pub fn with_limit(limit: usize, condition: F) -> Self {
        if limit < 1 {
            panic!("Limit must be greater than 0");
        }
        ConditionalTopCollector {
            limit,
            condition,
            _marker: PhantomData,
        }
    }

    pub fn merge_many(&self, children: Vec<Vec<SearchMarker<T>>>) -> Vec<SearchMarker<T>> {
        let mut topk = TopK::new(self.limit);

        for child_fruit in children {
            for Scored { score, doc } in child_fruit {
                topk.visit(score, doc);
            }
        }

        topk.into_sorted_vec()
    }
}

impl<F> Collector for ConditionalTopCollector<Score, F>
where
    F: CollectCondition<Score> + Sync,
{
    type Fruit = Vec<SearchMarker<Score>>;
    type Child = ConditionalTopSegmentCollector<Score, F>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(self.merge_many(children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        _reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(ConditionalTopSegmentCollector::new(
            segment_id,
            self.limit,
            self.condition.clone(),
        ))
    }
}

pub struct ConditionalTopSegmentCollector<T, F>
where
    F: CollectCondition<T>,
{
    segment_id: SegmentLocalId,
    collected: TopK<T, DocId>,
    condition: F,
}

impl<T, F> ConditionalTopSegmentCollector<T, F>
where
    T: PartialOrd + Copy,
    F: CollectCondition<T>,
{
    pub fn new(segment_id: SegmentLocalId, limit: usize, condition: F) -> Self {
        ConditionalTopSegmentCollector {
            collected: TopK::new(limit),
            segment_id,
            condition,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.collected.len()
    }

    #[inline(always)]
    pub fn visit(&mut self, doc: DocId, score: T) {
        if self.condition.check(self.segment_id, doc, score) {
            self.collected.visit(score, doc);
        }
    }

    pub fn into_vec(self) -> Vec<SearchMarker<T>> {
        let segment_id = self.segment_id;
        self.collected
            .into_vec()
            .into_iter()
            .map(|Scored { score, doc }| Scored {
                score,
                doc: DocAddress(segment_id, doc),
            })
            .collect()
    }
}

impl<F> SegmentCollector for ConditionalTopSegmentCollector<Score, F>
where
    F: CollectCondition<Score>,
{
    type Fruit = Vec<SearchMarker<Score>>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.visit(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.into_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn condition_is_checked() {
        const LIMIT: usize = 4;

        let mut nil_collector = ConditionalTopSegmentCollector::new(0, LIMIT, false);

        let mut top_collector = ConditionalTopSegmentCollector::new(0, LIMIT, true);

        let condition = |_sid, doc, _score| doc % 2 == 1;

        let mut just_odds = ConditionalTopSegmentCollector::new(0, LIMIT, condition);

        for i in 0..4 {
            nil_collector.collect(i, 420.0);
            top_collector.collect(i, 420.0);
            just_odds.collect(i, 420.0);
        }

        assert_eq!(0, nil_collector.len());
        assert_eq!(4, top_collector.len());
        assert_eq!(2, just_odds.len());

        // Verify that the collected items respect the condition
        for scored in just_odds.harvest() {
            let DocAddress(seg_id, doc_id) = scored.doc;
            assert!(condition(seg_id, doc_id, scored.score))
        }
    }

    #[test]
    fn collection_with_a_marker_smoke() {
        // Doc id=4 on segment=0 had score=0.5
        let marker = Scored::new(0.5, DocAddress(0, 4));
        let mut collector = ConditionalTopSegmentCollector::new(0, 3, marker);

        // Every doc with a higher score has appeared already
        collector.collect(7, 0.6);
        collector.collect(5, 0.7);
        assert_eq!(0, collector.len());

        // Docs with the same score, but lower id too
        collector.collect(3, 0.5);
        collector.collect(2, 0.5);
        assert_eq!(0, collector.len());

        // And, of course, the same doc should not be collected
        collector.collect(4, 0.5);
        assert_eq!(0, collector.len());

        // Lower scores are in
        collector.collect(1, 0.0);
        // Same score but higher doc, too
        collector.collect(6, 0.5);

        assert_eq!(2, collector.len());
    }

    #[test]
    fn fruits_are_merged_correctly() {
        let collector = ConditionalTopCollector::with_limit(5, true);

        let merged = collector
            .merge_fruits(vec![
                // S0
                vec![Scored::new(0.5, DocAddress(0, 1))],
                // S1 has a doc that scored the same as S0, so
                // it should only appear *after* the one in S0
                vec![
                    Scored::new(0.5, DocAddress(1, 1)),
                    Scored::new(0.6, DocAddress(1, 2)),
                ],
                // S2 has two evenly scored docs, the one with
                // the lowest internal id should appear first
                vec![
                    Scored::new(0.2, DocAddress(2, 2)),
                    Scored::new(0.2, DocAddress(2, 1)),
                ],
            ])
            .unwrap();

        assert_eq!(
            vec![
                Scored::new(0.6, DocAddress(1, 2)),
                Scored::new(0.5, DocAddress(0, 1)),
                Scored::new(0.5, DocAddress(1, 1)),
                Scored::new(0.2, DocAddress(2, 1)),
                Scored::new(0.2, DocAddress(2, 2))
            ],
            merged
        );
    }
}
