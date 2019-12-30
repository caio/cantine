use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};

pub struct TopK<S, D> {
    limit: usize,
    heap: BinaryHeap<Reverse<Scored<S, D>>>,
}

impl<S: PartialOrd, D: PartialOrd> TopK<S, D> {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn visit(&mut self, score: S, doc: D) {
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

    pub fn into_sorted_vec(self) -> Vec<Scored<S, D>> {
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(item)| item)
            .collect()
    }

    pub fn into_vec(self) -> Vec<Scored<S, D>> {
        self.heap
            .into_vec()
            .into_iter()
            .map(|Reverse(item)| item)
            .collect()
    }
}

// TODO warn about exposing docid,segment_id publicly
#[derive(Debug, Clone)]
pub struct Scored<S, D> {
    pub score: S,
    pub doc: D,
}

impl<S: PartialOrd, D: PartialOrd> Scored<S, D> {
    pub fn new(score: S, doc: D) -> Self {
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
    use super::{Scored, TopK};

    #[test]
    fn not_at_capacity() {
        let mut topk = TopK::new(3);

        assert!(topk.is_empty());

        topk.visit(0.8, 1);
        topk.visit(0.2, 3);
        topk.visit(0.3, 5);

        assert_eq!(3, topk.len());

        assert_eq!(
            vec![
                Scored::new(0.8, 1),
                Scored::new(0.3, 5),
                Scored::new(0.2, 3)
            ],
            topk.into_sorted_vec()
        )
    }

    #[test]
    fn at_capacity() {
        let mut topk = TopK::new(4);

        topk.visit(0.8, 1);
        topk.visit(0.2, 3);
        topk.visit(0.3, 5);
        topk.visit(0.9, 7);
        topk.visit(-0.2, 9);

        assert_eq!(4, topk.len());

        assert_eq!(
            vec![
                Scored::new(0.9, 7),
                Scored::new(0.8, 1),
                Scored::new(0.3, 5),
                Scored::new(0.2, 3)
            ],
            topk.into_sorted_vec()
        );
    }

    #[test]
    fn break_even_scores_by_lowest_doc() {
        let mut topk = TopK::new(5);
        topk.visit(0.1, 3);
        topk.visit(0.1, 1);
        topk.visit(0.1, 6);
        topk.visit(0.5, 5);
        topk.visit(0.5, 4);
        topk.visit(0.1, 2);
        assert_eq!(
            vec![
                Scored::new(0.5, 4),
                Scored::new(0.5, 5),
                Scored::new(0.1, 1),
                Scored::new(0.1, 2),
                Scored::new(0.1, 3),
            ],
            topk.into_sorted_vec()
        );
    }
}
