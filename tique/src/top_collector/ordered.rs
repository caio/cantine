use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{CheckCondition, CollectionResult, ConditionForSegment, Scored};

pub trait TopK<T, D> {
    fn visit(&mut self, score: T, doc: D);
    fn into_sorted_vec(self) -> Vec<(T, D)>;
    fn into_vec(self) -> Vec<(T, D)>;
}

pub trait TopKProvider<T: PartialOrd> {
    type Child: TopK<T, DocId>;
    fn new_topk(limit: usize) -> Self::Child;
    fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T>;
}

pub struct OrderedCollector<T, P, CF> {
    limit: usize,
    condition_factory: CF,
    _score: PhantomData<T>,
    _provider: PhantomData<P>,
}

impl<T, P, CF> OrderedCollector<T, P, CF>
where
    T: PartialOrd,
    P: 'static + Send + Sync + TopKProvider<Score>,
    CF: ConditionForSegment<T> + Sync,
{
    pub fn new(limit: usize, condition_factory: CF) -> Self {
        if limit < 1 {
            panic!("Limit must be greater than 0");
        }
        OrderedCollector {
            limit,
            condition_factory,
            _score: PhantomData,
            _provider: PhantomData,
        }
    }
}

impl<P, CF> Collector for OrderedCollector<Score, P, CF>
where
    P: 'static + Send + Sync + TopKProvider<Score>,
    CF: ConditionForSegment<Score> + Sync,
{
    type Fruit = CollectionResult<Score>;
    type Child = OrderedSegmentCollector<Score, P::Child, CF::Type>;

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
        Ok(OrderedSegmentCollector {
            total: 0,
            visited: 0,
            segment_id,
            topk: P::new_topk(self.limit),
            condition: self.condition_factory.for_segment(reader),
            _marker: PhantomData,
        })
    }
}

pub struct OrderedSegmentCollector<T, K, C> {
    total: usize,
    visited: usize,
    segment_id: SegmentLocalId,
    topk: K,
    condition: C,
    _marker: PhantomData<T>,
}

impl<K, C> SegmentCollector for OrderedSegmentCollector<Score, K, C>
where
    K: TopK<Score, DocId> + 'static,
    C: CheckCondition<Score>,
{
    type Fruit = CollectionResult<Score>;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.total += 1;
        if self.condition.check(self.segment_id, doc, score) {
            self.visited += 1;
            self.topk.visit(score, doc);
        }
    }

    fn harvest(self) -> Self::Fruit {
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

mod topk {
    use std::{
        cmp::{Ordering, Reverse},
        collections::BinaryHeap,
    };

    use super::{CollectionResult, DocId, Scored, TopK, TopKProvider};

    pub struct Ascending;

    impl<T: PartialOrd> TopKProvider<T> for Ascending {
        type Child = AscendingTopK<T, DocId>;

        fn new_topk(limit: usize) -> Self::Child {
            AscendingTopK::new(limit)
        }

        fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T> {
            let mut topk = AscendingTopK::new(limit);

            let mut total = 0;
            let mut visited = 0;

            for item in items {
                total += item.total;
                visited += item.visited;

                for (score, doc) in item.items {
                    topk.visit(score, doc);
                }
            }

            CollectionResult {
                total,
                visited,
                items: topk.into_sorted_vec().into_iter().collect(),
            }
        }
    }

    pub struct Descending;

    impl<T: PartialOrd> TopKProvider<T> for Descending {
        type Child = DescendingTopK<T, DocId>;

        fn new_topk(limit: usize) -> Self::Child {
            DescendingTopK {
                limit,
                heap: BinaryHeap::with_capacity(limit),
            }
        }

        fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T> {
            let mut topk = DescendingTopK::new(limit);

            let mut total = 0;
            let mut visited = 0;

            for item in items {
                total += item.total;
                visited += item.visited;

                for (score, doc) in item.items {
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

    pub struct AscendingTopK<S, D> {
        limit: usize,
        heap: BinaryHeap<Scored<S, Reverse<D>>>,
    }

    pub struct DescendingTopK<S, D> {
        limit: usize,
        heap: BinaryHeap<Reverse<Scored<S, D>>>,
    }

    impl<T: PartialOrd, D: PartialOrd> AscendingTopK<T, D> {
        fn new(limit: usize) -> Self {
            Self {
                limit,
                heap: BinaryHeap::with_capacity(limit),
            }
        }

        fn visit(&mut self, score: T, doc: D) {
            if self.heap.len() < self.limit {
                self.heap.push(Scored {
                    score,
                    doc: Reverse(doc),
                });
            } else if let Some(mut head) = self.heap.peek_mut() {
                if match head.score.partial_cmp(&score) {
                    Some(Ordering::Equal) => doc < head.doc.0,
                    Some(Ordering::Greater) => true,
                    _ => false,
                } {
                    head.score = score;
                    head.doc.0 = doc;
                }
            }
        }

        fn into_sorted_vec(self) -> Vec<(T, D)> {
            self.heap
                .into_sorted_vec()
                .into_iter()
                .map(|s| (s.score, s.doc.0))
                .collect()
        }

        fn into_vec(self) -> Vec<(T, D)> {
            self.heap
                .into_vec()
                .into_iter()
                .map(|s| (s.score, s.doc.0))
                .collect()
        }
    }

    impl<T: PartialOrd, D: PartialOrd> DescendingTopK<T, D> {
        fn new(limit: usize) -> Self {
            Self {
                limit,
                heap: BinaryHeap::with_capacity(limit),
            }
        }

        fn visit(&mut self, score: T, doc: D) {
            if self.heap.len() < self.limit {
                self.heap.push(Reverse(Scored { score, doc }));
            } else if let Some(mut head) = self.heap.peek_mut() {
                if match head.0.score.partial_cmp(&score) {
                    Some(Ordering::Equal) => doc < head.0.doc,
                    Some(Ordering::Less) => true,
                    _ => false,
                } {
                    head.0.score = score;
                    head.0.doc = doc;
                }
            }
        }

        fn into_sorted_vec(self) -> Vec<(T, D)> {
            self.heap
                .into_sorted_vec()
                .into_iter()
                .map(|s| (s.0.score, s.0.doc))
                .collect()
        }

        fn into_vec(self) -> Vec<(T, D)> {
            self.heap
                .into_vec()
                .into_iter()
                .map(|s| (s.0.score, s.0.doc))
                .collect()
        }
    }

    impl<T: PartialOrd> TopK<T, DocId> for AscendingTopK<T, DocId> {
        fn visit(&mut self, score: T, doc: DocId) {
            AscendingTopK::visit(self, score, doc);
        }

        fn into_sorted_vec(self) -> Vec<(T, DocId)> {
            AscendingTopK::into_sorted_vec(self)
        }

        fn into_vec(self) -> Vec<(T, DocId)> {
            AscendingTopK::into_vec(self)
        }
    }

    impl<T: PartialOrd> TopK<T, DocId> for DescendingTopK<T, DocId> {
        fn visit(&mut self, score: T, doc: DocId) {
            DescendingTopK::visit(self, score, doc);
        }

        fn into_sorted_vec(self) -> Vec<(T, DocId)> {
            DescendingTopK::into_sorted_vec(self)
        }

        fn into_vec(self) -> Vec<(T, DocId)> {
            DescendingTopK::into_vec(self)
        }
    }

    #[cfg(test)]
    mod tests {

        use super::*;

        fn check_topk<S, D, K: TopK<S, D>>(mut topk: K, input: Vec<(S, D)>, wanted: Vec<(S, D)>)
        where
            S: PartialOrd + std::fmt::Debug,
            D: PartialOrd + std::fmt::Debug,
        {
            for (score, id) in input {
                topk.visit(score, id);
            }

            assert_eq!(wanted, topk.into_sorted_vec());
        }

        #[test]
        fn not_at_capacity() {
            let input = vec![(0.8, 1), (0.2, 3), (0.5, 4), (0.3, 5)];
            let mut wanted = vec![(0.2, 3), (0.3, 5), (0.5, 4), (0.8, 1)];

            check_topk(AscendingTopK::new(4), input.clone(), wanted.clone());

            wanted.reverse();
            check_topk(DescendingTopK::new(4), input, wanted);
        }

        #[test]
        fn at_capacity() {
            let input = vec![(0.8, 1), (0.2, 3), (0.3, 5), (0.9, 7), (-0.2, 9)];

            check_topk(
                AscendingTopK::new(3),
                input.clone(),
                vec![(-0.2, 9), (0.2, 3), (0.3, 5)],
            );

            check_topk(
                DescendingTopK::new(3),
                input,
                vec![(0.9, 7), (0.8, 1), (0.3, 5)],
            );
        }

        #[test]
        fn break_even_scores_by_lowest_doc() {
            let input = vec![(0.1, 3), (0.1, 1), (0.1, 6), (0.5, 5), (0.5, 4), (0.1, 2)];

            check_topk(
                AscendingTopK::new(5),
                input.clone(),
                vec![(0.1, 1), (0.1, 2), (0.1, 3), (0.1, 6), (0.5, 4)],
            );

            check_topk(
                DescendingTopK::new(5),
                input,
                vec![(0.5, 4), (0.5, 5), (0.1, 1), (0.1, 2), (0.1, 3)],
            );
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use tantivy::{query::TermQuery, schema, Document, Index, Result, Term};

    #[test]
    fn collect_even_public_ids_ascendingly() -> Result<()> {
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

        let collector_asc = OrderedCollector::<_, topk::Ascending, _>::new(NUM_DOCS, true);
        let collector_desc = OrderedCollector::<_, topk::Descending, _>::new(NUM_DOCS, true);

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
