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
    type TK: TopK<T, DocId>;
    fn new_topk(limit: usize) -> Self::TK;
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
    pub fn with_limit(limit: usize, condition_factory: CF) -> Self {
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
    type Child = OrderedSegmentCollector<Score, P::TK, CF::Type>;

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
        self.topk.visit(score, doc);
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
            .map(|(score, doc)| Scored {
                score,
                doc: DocAddress(segment_id, doc),
            })
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
    // use crate::top_collector::Scored;

    pub struct Ascending;

    impl<T: PartialOrd> TopKProvider<T> for Ascending {
        type TK = AscendingTopK<T, DocId>;

        fn new_topk(limit: usize) -> Self::TK {
            AscendingTopK {
                limit,
                heap: BinaryHeap::with_capacity(limit),
            }
        }

        fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T> {
            let mut topk = AscendingTopK {
                limit,
                heap: BinaryHeap::with_capacity(limit),
            };

            let mut total = 0;
            let mut visited = 0;

            for item in items {
                total += item.total;
                visited += item.visited;

                for collected in item.items {
                    topk.visit(collected.score, collected.doc);
                }
            }

            CollectionResult {
                total,
                visited,
                items: topk
                    .into_sorted_vec()
                    .into_iter()
                    .map(|(score, doc)| Scored { score, doc })
                    .collect(),
            }
        }
    }

    pub struct AscendingTopK<S, D> {
        limit: usize,
        heap: BinaryHeap<Scored<S, D>>,
    }

    //     pub struct DescendingTopK<S, D> {
    //         limit: usize,
    //         heap: BinaryHeap<Scored<S, D>>,
    //     }

    impl<T: PartialOrd, D: PartialOrd> AscendingTopK<T, D> {
        fn visit(&mut self, score: T, doc: D) {
            if self.heap.len() < self.limit {
                self.heap.push(Scored { score, doc });
            } else if let Some(mut head) = self.heap.peek_mut() {
                if match head.score.partial_cmp(&score) {
                    Some(Ordering::Equal) => doc < head.doc,
                    Some(Ordering::Less) => true,
                    _ => false,
                } {
                    head.score = score;
                    head.doc = doc;
                }
            }
        }

        fn into_sorted_vec(self) -> Vec<(T, D)> {
            self.heap
                .into_sorted_vec()
                .into_iter()
                .map(|Scored { score, doc }| (score, doc))
                .collect()
        }

        fn into_vec(self) -> Vec<(T, D)> {
            self.heap
                .into_vec()
                .into_iter()
                .map(|Scored { score, doc }| (score, doc))
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
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn pass() {
        let _collector = OrderedCollector::<Score, topk::Ascending, _>::with_limit(10, true);
    }
}
