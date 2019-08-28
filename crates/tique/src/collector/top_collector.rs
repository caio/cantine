use std::{cmp::Ordering, collections::BinaryHeap};
use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

/// This is pretty much tantivy's TopCollector and friends, tweaked to:
///   * Have a stable ordering, using DocAddress to break even scores
///   * Support pagination via a SearchMarker, without pages or offsets
/// And specialized fixed on score to make things easier for now

struct StableComparableDoc<D> {
    feature: Score,
    doc: D,
}

#[derive(Clone)]
pub struct SearchMarker {
    score: Score,
    addr: DocAddress,
}

// A reference to the last picked result in a search result
impl SearchMarker {
    // Answers wether the previous search(es) would have listed
    // a document with the given attributes.
    pub fn has_seen(&self, feature: Score, segment_id: SegmentLocalId, doc_id: DocId) -> bool {
        // If feature > self.score => yes
        match self.score.partial_cmp(&feature) {
            // Score is the same, so we should only pick the higher addresses
            Some(Ordering::Equal) => DocAddress(segment_id, doc_id) <= self.addr,
            // Given document has a score higher than the marker: it should have
            // definitely appeared in the results
            Some(Ordering::Less) => true,
            // No document with lower score appears before the marker
            _ => false,
        }
    }

    pub fn new(score: Score, segment: SegmentLocalId, doc: DocId) -> Self {
        SearchMarker {
            score,
            addr: DocAddress(segment, doc),
        }
    }
}

impl<D: PartialOrd> PartialOrd for StableComparableDoc<D> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<D: PartialOrd> Ord for StableComparableDoc<D> {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // Highest feature first
        match other.feature.partial_cmp(&self.feature) {
            Some(Ordering::Equal) | None => {
                // Break even by _lowest_ doc
                self.doc.partial_cmp(&other.doc).unwrap_or(Ordering::Equal)
            }
            Some(rest) => rest,
        }
    }
}

impl<D: PartialOrd> PartialEq for StableComparableDoc<D> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<D: PartialOrd> Eq for StableComparableDoc<D> {}

pub struct TopCollector {
    limit: usize,
    after: Option<SearchMarker>,
}

impl TopCollector {
    pub fn with_limit(limit: usize, after: Option<SearchMarker>) -> TopCollector {
        if limit < 1 {
            panic!("Limit must be strictly greater than 0.");
        }
        TopCollector { limit, after }
    }
}

impl Collector for TopCollector {
    type Fruit = Vec<(Score, DocAddress)>;
    type Child = TopSegmentCollector;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        let mut top_collector = BinaryHeap::new();

        for child_fruit in children {
            for (feature, doc) in child_fruit {
                if top_collector.len() < self.limit {
                    top_collector.push(StableComparableDoc { feature, doc });
                } else if let Some(mut head) = top_collector.peek_mut() {
                    if match head.feature.partial_cmp(&feature) {
                        Some(Ordering::Equal) => doc < head.doc,
                        Some(Ordering::Less) => true,
                        _ => false,
                    } {
                        *head = StableComparableDoc { feature, doc };
                    }
                }
            }
        }

        Ok(top_collector
            .into_sorted_vec()
            .into_iter()
            .map(|cdoc| (cdoc.feature, cdoc.doc))
            .collect())
    }

    fn for_segment(&self, segment_id: SegmentLocalId, _: &SegmentReader) -> Result<Self::Child> {
        Ok(TopSegmentCollector::new(
            segment_id,
            self.limit,
            self.after.clone(),
        ))
    }
}

pub struct TopSegmentCollector {
    limit: usize,
    heap: BinaryHeap<StableComparableDoc<DocId>>,
    segment_id: u32,
    after: Option<SearchMarker>,
}

impl TopSegmentCollector {
    fn new(
        segment_id: SegmentLocalId,
        limit: usize,
        after: Option<SearchMarker>,
    ) -> TopSegmentCollector {
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

impl SegmentCollector for TopSegmentCollector {
    type Fruit = Vec<(Score, DocAddress)>;

    #[inline(always)]
    fn collect(&mut self, doc: DocId, feature: Score) {
        if let Some(marker) = &self.after {
            if marker.has_seen(feature, self.segment_id, doc) {
                return;
            }
        }

        if self.heap.len() >= self.limit {
            if let Some(head) = self.heap.peek() {
                if match head.feature.partial_cmp(&feature) {
                    Some(Ordering::Equal) => doc < head.doc,
                    Some(Ordering::Less) => true,
                    _ => false,
                } {
                    if let Some(mut head) = self.heap.peek_mut() {
                        head.feature = feature;
                        head.doc = doc;
                    }
                }
            }
        } else {
            self.heap.push(StableComparableDoc { feature, doc });
        }
    }

    fn harvest(self) -> Self::Fruit {
        let segment_id = self.segment_id;
        self.heap
            .into_sorted_vec()
            .into_iter()
            .map(|comparable_doc| {
                (
                    comparable_doc.feature,
                    DocAddress(segment_id, comparable_doc.doc),
                )
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
                (0.8, DocAddress(0, 1)),
                (0.3, DocAddress(0, 5)),
                (0.2, DocAddress(0, 3))
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
                (0.9, DocAddress(0, 7)),
                (0.8, DocAddress(0, 1)),
                (0.3, DocAddress(0, 5)),
                (0.2, DocAddress(0, 3))
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
                (0.5, DocAddress(0, 4)),
                (0.5, DocAddress(0, 5)),
                (0.1, DocAddress(0, 1)),
                (0.1, DocAddress(0, 2)),
                (0.1, DocAddress(0, 3)),
            ]
        );
    }

    #[test]
    fn search_marker() {
        let same_score = 0.9;
        let same_segment = 2;
        let same_doc = 10;
        let marker = SearchMarker::new(same_score, same_segment, same_doc);

        // Higher score: seen
        assert!(marker.has_seen(1.0, same_segment, same_doc));
        // Lower score: not seen
        assert_eq!(false, marker.has_seen(0.8, same_segment, same_doc));

        // Same score, lower segment: seen
        assert!(marker.has_seen(same_score, 0, same_doc));
        // Same score, higher segment: not seen
        assert_eq!(false, marker.has_seen(same_score, 3, same_doc));

        // Same score and segment, lower doc: seen
        assert!(marker.has_seen(same_score, same_segment, 9));
        // Same score and segment, higher doc: not seen
        assert_eq!(false, marker.has_seen(same_score, same_segment, 11));

        // Exactly the same as After: seen
        assert!(marker.has_seen(same_score, same_segment, same_doc));
    }

    #[test]
    fn collection_with_a_marker_smoke() {
        // Doc id=4 on segment=0 had score=0.5
        let marker = SearchMarker::new(0.5, 0, 4);
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
            vec![(0.5, DocAddress(0, 6)), (0.0, DocAddress(0, 1))],
            collector.harvest()
        );
    }

    #[test]
    fn fruits_are_merged_correctly() {
        let collector = TopCollector::with_limit(5, None);

        let merged = collector
            .merge_fruits(vec![
                vec![(0.5, DocAddress(0, 1))],
                // S1 has a doc that scored the same as S0, so
                // it should only appear *after* the one in S0
                vec![(0.5, DocAddress(1, 1)), (0.6, DocAddress(1, 2))],
                // S2 has two evenly scored docs, the one with
                // the lowest internal id should appear first
                vec![(0.2, DocAddress(2, 2)), (0.2, DocAddress(2, 1))],
            ])
            .unwrap();

        assert_eq!(
            vec![
                (0.6, DocAddress(1, 2)),
                (0.5, DocAddress(0, 1)),
                (0.5, DocAddress(1, 1)),
                (0.2, DocAddress(2, 1)),
                (0.2, DocAddress(2, 2))
            ],
            merged
        )
    }
}
