use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{Scored, TopK};

/// This is pretty much tantivy's TopCollector and friends, tweaked to:
///   * Have a stable ordering, using DocAddress to break even scores
///   * Support pagination via a SearchMarker, without pages or offsets
/// And specialized fixed on score to make things easier for now

pub struct TopCollector<T> {
    pub limit: usize,
    pub after: Option<SearchMarker<T>>,
}

pub type SearchMarker<T> = Scored<T, DocAddress>;

pub struct TopSegmentCollector<T> {
    segment_id: SegmentLocalId,
    collected: TopK<T, DocId>,
    after: Option<SearchMarker<T>>,
}

impl<T> TopCollector<T>
where
    T: PartialOrd,
{
    pub fn with_limit(limit: usize, after: Option<SearchMarker<T>>) -> Self {
        if limit < 1 {
            panic!("Limit must be greater than 0");
        }
        TopCollector { limit, after }
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

impl Collector for TopCollector<Score> {
    type Fruit = Vec<SearchMarker<Score>>;
    type Child = TopSegmentCollector<Score>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(self.merge_many(children))
    }

    fn for_segment(&self, segment_id: SegmentLocalId, _: &SegmentReader) -> Result<Self::Child> {
        Ok(TopSegmentCollector::new(
            segment_id,
            self.limit,
            self.after.clone(),
        ))
    }
}

impl<T> TopSegmentCollector<T>
where
    T: PartialOrd + Copy,
{
    pub fn new(segment_id: SegmentLocalId, limit: usize, after: Option<SearchMarker<T>>) -> Self {
        TopSegmentCollector {
            collected: TopK::new(limit),
            segment_id,
            after,
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.collected.len()
    }

    #[inline(always)]
    pub fn visit(&mut self, doc: DocId, score: T) {
        if let Some(after) = &self.after {
            let scored = Scored {
                score,
                doc: DocAddress(self.segment_id, doc),
            };
            if *after <= scored {
                return;
            }
        }

        self.collected.visit(score, doc);
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

impl SegmentCollector for TopSegmentCollector<Score> {
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
    fn collection_with_a_marker_smoke() {
        // Doc id=4 on segment=0 had score=0.5
        let marker = Scored::new(0.5, DocAddress(0, 4));
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

        assert_eq!(2, collector.len());
    }

    #[test]
    fn fruits_are_merged_correctly() {
        let collector = TopCollector::with_limit(5, None);

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
