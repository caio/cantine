use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{Scored, TopK};

pub trait ConditionForSegment<T>: Clone {
    type Type: CheckCondition<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Type;
}

impl<T, C, F> ConditionForSegment<T> for F
where
    F: Clone + Fn(&SegmentReader) -> C,
    C: CheckCondition<T>,
{
    type Type = C;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
        (self)(reader)
    }
}

impl<T> ConditionForSegment<T> for bool {
    type Type = bool;
    fn for_segment(&self, _reader: &SegmentReader) -> Self::Type {
        *self
    }
}

pub trait CheckCondition<T>: 'static + Clone {
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T) -> bool;
}

impl<T> CheckCondition<T> for bool {
    fn check(&self, _: SegmentLocalId, _: DocId, _: T) -> bool {
        *self
    }
}

impl<F, T> CheckCondition<T> for F
where
    F: 'static + Clone + Fn(SegmentLocalId, DocId, T) -> bool,
{
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T) -> bool {
        (self)(segment_id, doc_id, score)
    }
}

pub type SearchMarker<T> = Scored<T, DocAddress>;

impl<T> CheckCondition<T> for SearchMarker<T>
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
    F: ConditionForSegment<T>,
{
    pub limit: usize,
    condition_factory: F,
    _marker: PhantomData<T>,
}

impl<T, F> ConditionalTopCollector<T, F>
where
    T: PartialOrd,
    F: ConditionForSegment<T>,
{
    pub fn with_limit(limit: usize, condition_factory: F) -> Self {
        if limit < 1 {
            panic!("Limit must be greater than 0");
        }
        ConditionalTopCollector {
            limit,
            condition_factory,
            _marker: PhantomData,
        }
    }

    pub fn merge_many(&self, children: Vec<CollectionResult<T>>) -> CollectionResult<T> {
        CollectionResult::merge_many(self.limit, children)
    }
}

impl<F> Collector for ConditionalTopCollector<Score, F>
where
    F: ConditionForSegment<Score> + Sync,
{
    type Fruit = CollectionResult<Score>;
    type Child = ConditionalTopSegmentCollector<Score, F::Type>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(self.merge_many(children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(ConditionalTopSegmentCollector::new(
            segment_id,
            self.limit,
            self.condition_factory.for_segment(reader),
        ))
    }
}

pub struct ConditionalTopSegmentCollector<T, F>
where
    F: CheckCondition<T>,
{
    segment_id: SegmentLocalId,
    collected: TopK<T, DocId>,
    visited: usize,
    total: usize,
    condition: F,
}

impl<T, F> ConditionalTopSegmentCollector<T, F>
where
    T: PartialOrd + Copy,
    F: CheckCondition<T>,
{
    pub fn new(segment_id: SegmentLocalId, limit: usize, condition: F) -> Self {
        ConditionalTopSegmentCollector {
            collected: TopK::new(limit),
            segment_id,
            condition,
            visited: 0,
            total: 0,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.collected.len()
    }

    #[inline(always)]
    pub fn visit(&mut self, doc: DocId, score: T) {
        self.total += 1;
        if self.condition.check(self.segment_id, doc, score) {
            self.visited += 1;
            self.collected.visit(score, doc);
        }
    }

    pub fn into_collection_result(self) -> CollectionResult<T> {
        let segment_id = self.segment_id;
        let items = self
            .collected
            .into_vec()
            .into_iter()
            .map(|Scored { score, doc }| Scored {
                score,
                doc: DocAddress(segment_id, doc),
            })
            .collect();

        CollectionResult {
            total: self.total,
            visited: self.visited,
            items,
        }
    }
}

impl<F> SegmentCollector for ConditionalTopSegmentCollector<Score, F>
where
    F: CheckCondition<Score>,
{
    type Fruit = CollectionResult<Score>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.visit(doc, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.into_collection_result()
    }
}

#[derive(Debug)]
pub struct CollectionResult<T> {
    pub total: usize,
    pub visited: usize,
    pub items: Vec<SearchMarker<T>>,
}

impl<T: PartialOrd> CollectionResult<T> {
    pub fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T> {
        let mut topk = TopK::new(limit);
        let mut total = 0;
        let mut visited = 0;

        for item in items {
            total += item.total;
            visited += item.visited;

            for Scored { score, doc } in item.items {
                topk.visit(score, doc);
            }
        }

        CollectionResult {
            total,
            visited,
            items: topk.into_sorted_vec(),
        }
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
        let result = just_odds.harvest();
        assert_eq!(4, result.total);
        assert_eq!(2, result.visited);
        for scored in result.items {
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
                CollectionResult {
                    total: 1,
                    visited: 1,
                    items: vec![Scored::new(0.5, DocAddress(0, 1))],
                },
                // S1 has a doc that scored the same as S0, so
                // it should only appear *after* the one in S0
                CollectionResult {
                    total: 1,
                    visited: 1,
                    items: vec![
                        Scored::new(0.5, DocAddress(1, 1)),
                        Scored::new(0.6, DocAddress(1, 2)),
                    ],
                },
                // S2 has two evenly scored docs, the one with
                // the lowest internal id should appear first
                CollectionResult {
                    total: 1,
                    visited: 1,
                    items: vec![
                        Scored::new(0.2, DocAddress(2, 2)),
                        Scored::new(0.2, DocAddress(2, 1)),
                    ],
                },
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
            merged.items
        );
    }

    use tantivy::{query::AllQuery, schema, Document, Index, Result};

    #[test]
    fn only_collect_even_public_ids() -> Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let id_field = builder.add_u64_field("public_id", schema::FAST);

        let index = Index::create_in_ram(builder.build());

        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;

        const NUM_DOCS: u64 = 10;
        for public_id in 0..NUM_DOCS {
            let mut doc = Document::new();
            doc.add_u64(id_field, public_id);
            writer.add_document(doc);
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let condition_factory = |reader: &SegmentReader| {
            let id_reader = reader.fast_fields().u64(id_field).unwrap();

            move |_segment_id, doc_id, _score| {
                let stored_id = id_reader.get(doc_id);
                stored_id % 2 == 0
            }
        };
        let results = searcher.search(
            &AllQuery,
            &ConditionalTopCollector::with_limit(NUM_DOCS as usize, condition_factory),
        )?;

        assert_eq!(5, results.items.len());

        Ok(())
    }
}
