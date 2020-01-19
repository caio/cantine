use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use tantivy::{DocAddress, DocId};

use super::CollectionResult;

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
    pub(crate) fn new(limit: usize) -> Self {
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
    pub(crate) fn new(limit: usize) -> Self {
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

pub(crate) struct Scored<S, D> {
    pub score: S,
    pub doc: D,
}

impl<S: PartialOrd, D: PartialOrd> Scored<S, D> {
    pub(crate) fn new(score: S, doc: D) -> Self {
        Self { score, doc }
    }
}

impl<S: PartialOrd, D: PartialOrd> PartialOrd for Scored<S, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PartialOrd, D: PartialOrd> Ord for Scored<S, D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Highest score first
        match self.score.partial_cmp(&other.score) {
            Some(Ordering::Equal) | None => {
                // Break even by lowest id
                other.doc.partial_cmp(&self.doc).unwrap_or(Ordering::Equal)
            }
            Some(rest) => rest,
        }
    }
}

impl<S: PartialOrd, D: PartialOrd> PartialEq for Scored<S, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<S: PartialOrd, D: PartialOrd> Eq for Scored<S, D> {}

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
