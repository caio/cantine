use std::{
    cmp::{Ordering, Reverse},
    collections::BinaryHeap,
};
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

/// This is pretty much tantivy's TopCollector and friends, tweaked to:
///   * Have a stable ordering, using DocAddress to break even scores
///   * Support pagination via a SearchMarker, without pages or offsets
/// And specialized fixed on score to make things easier for now

pub struct TopCollector<T> {
    limit: usize,
    after: Option<Scored<T, DocAddress>>,
}

pub struct TopSegmentCollector<T> {
    limit: usize,
    segment_id: SegmentLocalId,
    heap: BinaryHeap<Reverse<Scored<T, DocId>>>,
    after: Option<Scored<T, DocId>>,
}

// TODO warn about exposing docid,segment_id publicly
#[derive(Debug, Clone)]
pub struct Scored<T, D> {
    score: T,
    doc: D,
}

impl<T: PartialOrd, D: PartialOrd> Scored<T, D> {
    pub fn new(score: T, doc: D) -> Self {
        Self { score, doc }
    }
}

impl<T: PartialOrd, D: PartialOrd> PartialOrd for Scored<T, D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: PartialOrd, D: PartialOrd> Ord for Scored<T, D> {
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

impl<T: PartialOrd, D: PartialOrd> PartialEq for Scored<T, D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<T: PartialOrd, D: PartialOrd> Eq for Scored<T, D> {}

impl TopCollector<Score> {
    pub fn with_limit(limit: usize, after: Option<Scored<Score, DocAddress>>) -> Self {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0");
        }
        TopCollector { limit, after }
    }
}

impl Collector for TopCollector<Score> {
    type Fruit = Vec<Scored<Score, DocAddress>>;
    type Child = TopSegmentCollector<Score>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut top_collector = BinaryHeap::new();

        for child_fruit in children {
            for Scored { score, doc } in child_fruit {
                if top_collector.len() < self.limit {
                    top_collector.push(Reverse(Scored { score, doc }));
                } else if let Some(mut head) = top_collector.peek_mut() {
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
        }

        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .map(|Reverse(item)| item)
            .collect())
    }

    fn for_segment(&self, segment_id: SegmentLocalId, _: &SegmentReader) -> Result<Self::Child> {
        if let Some(after) = &self.after {
            if segment_id == after.doc.segment_ord() {
                return Ok(TopSegmentCollector::new(
                    segment_id,
                    self.limit,
                    Some(Scored {
                        score: after.score,
                        doc: after.doc.doc(),
                    }),
                ));
            }
        }
        Ok(TopSegmentCollector::new(segment_id, self.limit, None))
    }
}

impl TopSegmentCollector<Score> {
    fn new(segment_id: SegmentLocalId, limit: usize, after: Option<Scored<Score, DocId>>) -> Self {
        TopSegmentCollector {
            limit,
            heap: BinaryHeap::with_capacity(limit),
            segment_id,
            after,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.heap.len()
    }
}

impl SegmentCollector for TopSegmentCollector<Score> {
    type Fruit = Vec<Scored<Score, DocAddress>>;

    #[inline(always)]
    fn collect(&mut self, doc: DocId, score: Score) {
        if let Some(after) = &self.after {
            let scored = Scored { score, doc };
            if *after <= scored {
                return;
            }
        }

        if self.heap.len() >= self.limit {
            if let Some(mut head) = self.heap.peek_mut() {
                if match head.0.score.partial_cmp(&score) {
                    Some(Ordering::Equal) => doc < head.0.doc,
                    Some(Ordering::Less) => true,
                    _ => false,
                } {
                    head.0.score = score;
                    head.0.doc = doc;
                }
            }
        } else {
            self.heap.push(Reverse(Scored { score, doc }));
        }
    }

    fn harvest(self) -> Self::Fruit {
        let segment_id = self.segment_id;
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|scored_doc_id| Scored {
                score: scored_doc_id.0.score,
                doc: DocAddress(segment_id, scored_doc_id.0.doc),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_collector_not_at_capacity() {
        let mut top_collector = TopSegmentCollector::new(0, 3, None);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        assert_eq!(
            top_collector.harvest(),
            vec![
                Scored::new(0.8, DocAddress(0, 1)),
                Scored::new(0.3, DocAddress(0, 5)),
                Scored::new(0.2, DocAddress(0, 3))
            ]
        );
    }

    #[test]
    fn test_top_collector_at_capacity() {
        let mut top_collector = TopSegmentCollector::new(0, 4, None);
        top_collector.collect(1, 0.8);
        top_collector.collect(3, 0.2);
        top_collector.collect(5, 0.3);
        top_collector.collect(7, 0.9);
        top_collector.collect(9, -0.2);
        assert_eq!(
            top_collector.harvest(),
            vec![
                Scored::new(0.9, DocAddress(0, 7)),
                Scored::new(0.8, DocAddress(0, 1)),
                Scored::new(0.3, DocAddress(0, 5)),
                Scored::new(0.2, DocAddress(0, 3))
            ]
        );
    }

    #[test]
    fn test_top_collector_stability() {
        let mut top_collector = TopSegmentCollector::new(0, 5, None);
        top_collector.collect(3, 0.1);
        top_collector.collect(1, 0.1);
        top_collector.collect(6, 0.1);
        top_collector.collect(5, 0.5);
        top_collector.collect(4, 0.5);
        top_collector.collect(2, 0.1);
        assert_eq!(
            top_collector.harvest(),
            vec![
                Scored::new(0.5, DocAddress(0, 4)),
                Scored::new(0.5, DocAddress(0, 5)),
                Scored::new(0.1, DocAddress(0, 1)),
                Scored::new(0.1, DocAddress(0, 2)),
                Scored::new(0.1, DocAddress(0, 3)),
            ]
        );
    }

    #[test]
    fn collection_with_a_marker_smoke() {
        // Doc id=4 on segment=0 had score=0.5
        let marker = Scored { score: 0.5, doc: 4 };
        let mut collector = TopSegmentCollector::new(0, 3, Some(marker));

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

        assert_eq!(
            vec![
                Scored::new(0.5, DocAddress(0, 6)),
                Scored::new(0.0, DocAddress(0, 1))
            ],
            collector.harvest()
        );
    }

    #[test]
    fn fruits_are_merged_correctly() {
        let collector = TopCollector::with_limit(5, None);

        let merged = collector
            .merge_fruits(vec![
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

    use rand::prelude::*;

    #[test]
    fn after_iteration() {
        let mut items: Vec<(DocId, Score)> = Vec::new();

        for i in 0..10 {
            items.push((i, random()));
        }

        let search_after = |n: usize,
                            after: Option<Scored<Score, DocAddress>>|
         -> Vec<Scored<Score, DocAddress>> {
            let mut collector = TopSegmentCollector::new(
                0,
                n,
                after.and_then(|s| Some(Scored::new(s.score, s.doc.1))),
            );
            for (id, score) in &items {
                collector.collect(*id, *score);
            }
            collector.harvest()
        };

        let sorted = search_after(10, None);

        assert_eq!(10, sorted.len());

        for i in 0..9 {
            let next_found = search_after(1, Some(sorted[i].clone()));
            assert_eq!(1, next_found.len());

            assert_eq!(sorted[i + 1], next_found[0]);
        }
    }
}
