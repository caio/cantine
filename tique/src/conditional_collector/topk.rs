use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

use super::CollectionResult;

pub trait TopK<T, D> {
    const ASCENDING: bool;
    fn visit(&mut self, doc: D, score: T);
    fn into_sorted_vec(self) -> Vec<(D, T)>;
    fn into_vec(self) -> Vec<(D, T)>;
}

pub trait TopKProvider<T: PartialOrd, D: Ord> {
    type Child: TopK<T, D>;

    fn new_topk(limit: usize) -> Self::Child;
    fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T>;
}

/// Marker to create a TopCollector in *ascending* order
pub struct Ascending;

impl<T: PartialOrd, D: Ord> TopKProvider<T, D> for Ascending {
    type Child = AscendingTopK<T, D>;

    fn new_topk(limit: usize) -> Self::Child {
        AscendingTopK::new(limit)
    }

    fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T> {
        CollectionResult::merge_many(AscendingTopK::new(limit), items)
    }
}

/// Marker to create a TopCollector in *descending* order
pub struct Descending;

impl<T: PartialOrd, D: Ord> TopKProvider<T, D> for Descending {
    type Child = DescendingTopK<T, D>;

    fn new_topk(limit: usize) -> Self::Child {
        DescendingTopK {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    fn merge_many(limit: usize, items: Vec<CollectionResult<T>>) -> CollectionResult<T> {
        CollectionResult::merge_many(DescendingTopK::new(limit), items)
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

impl<T: PartialOrd, D: Ord> AscendingTopK<T, D> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    fn visit(&mut self, doc: D, score: T) {
        let scored = Scored {
            score,
            doc: Reverse(doc),
        };
        if self.heap.len() < self.limit {
            self.heap.push(scored);
        } else if let Some(mut head) = self.heap.peek_mut() {
            if head.cmp(&scored) == Ordering::Greater {
                head.score = scored.score;
                head.doc = scored.doc;
            }
        }
    }

    fn into_sorted_vec(self) -> Vec<(D, T)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|s| (s.doc.0, s.score))
            .collect()
    }

    fn into_vec(self) -> Vec<(D, T)> {
        self.heap
            .into_vec()
            .into_iter()
            .map(|s| (s.doc.0, s.score))
            .collect()
    }
}

impl<T: PartialOrd, D: Ord> DescendingTopK<T, D> {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    fn visit(&mut self, doc: D, score: T) {
        let scored = Reverse(Scored { score, doc });
        if self.heap.len() < self.limit {
            self.heap.push(scored);
        } else if let Some(mut head) = self.heap.peek_mut() {
            if head.cmp(&scored) == Ordering::Greater {
                head.0.score = scored.0.score;
                head.0.doc = scored.0.doc;
            }
        }
    }

    fn into_sorted_vec(self) -> Vec<(D, T)> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|s| (s.0.doc, s.0.score))
            .collect()
    }

    fn into_vec(self) -> Vec<(D, T)> {
        self.heap
            .into_vec()
            .into_iter()
            .map(|s| (s.0.doc, s.0.score))
            .collect()
    }
}

impl<T: PartialOrd, D: Ord> TopK<T, D> for AscendingTopK<T, D> {
    const ASCENDING: bool = true;

    fn visit(&mut self, doc: D, score: T) {
        AscendingTopK::visit(self, doc, score);
    }

    fn into_sorted_vec(self) -> Vec<(D, T)> {
        AscendingTopK::into_sorted_vec(self)
    }

    fn into_vec(self) -> Vec<(D, T)> {
        AscendingTopK::into_vec(self)
    }
}

impl<T: PartialOrd, D: Ord> TopK<T, D> for DescendingTopK<T, D> {
    const ASCENDING: bool = false;

    fn visit(&mut self, doc: D, score: T) {
        DescendingTopK::visit(self, doc, score);
    }

    fn into_sorted_vec(self) -> Vec<(D, T)> {
        DescendingTopK::into_sorted_vec(self)
    }

    fn into_vec(self) -> Vec<(D, T)> {
        DescendingTopK::into_vec(self)
    }
}

pub(crate) struct Scored<S, D> {
    pub score: S,
    pub doc: D,
}

impl<S: PartialOrd, D: Ord> Scored<S, D> {
    pub(crate) fn new(score: S, doc: D) -> Self {
        Self { score, doc }
    }
}

impl<S: PartialOrd, D: Ord> PartialOrd for Scored<S, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<S: PartialOrd, D: Ord> Ord for Scored<S, D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Highest score first
        match self.score.partial_cmp(&other.score) {
            Some(Ordering::Equal) | None => {
                // Break even by lowest id
                other.doc.cmp(&self.doc)
            }
            Some(rest) => rest,
        }
    }
}

impl<S: PartialOrd, D: Ord> PartialEq for Scored<S, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<S: PartialOrd, D: Ord> Eq for Scored<S, D> {}

#[cfg(test)]
mod tests {

    use super::*;

    fn check_topk<S, D, K>(mut topk: K, input: Vec<(S, D)>, wanted: Vec<(S, D)>)
    where
        S: PartialOrd + std::fmt::Debug,
        D: PartialOrd + std::fmt::Debug,
        K: TopK<S, D>,
    {
        for (score, id) in input {
            topk.visit(id, score);
        }

        assert_eq!(
            wanted,
            topk.into_sorted_vec()
                .into_iter()
                .map(|(doc, score)| (score, doc))
                .collect::<Vec<_>>()
        );
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
