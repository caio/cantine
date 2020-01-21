use std::cmp::Ordering;

use tantivy::{DocAddress, DocId, SegmentLocalId, SegmentReader};

use super::topk::Scored;

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
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T, ascending: bool) -> bool;
}

impl<T> CheckCondition<T> for bool {
    fn check(&self, _: SegmentLocalId, _: DocId, _: T, _: bool) -> bool {
        *self
    }
}

impl<F, T> CheckCondition<T> for F
where
    F: 'static + Clone + Fn(SegmentLocalId, DocId, T, bool) -> bool,
{
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T, ascending: bool) -> bool {
        (self)(segment_id, doc_id, score, ascending)
    }
}

impl<T> CheckCondition<T> for (T, DocAddress)
where
    T: 'static + PartialOrd + Clone + Copy,
{
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T, ascending: bool) -> bool {
        let wanted = if ascending {
            Ordering::Less
        } else {
            Ordering::Greater
        };

        Scored::new(self.0, self.1).cmp(&Scored::new(score, DocAddress(segment_id, doc_id)))
            == wanted
    }
}

pub trait ScorerForSegment<T>: Sync {
    type Type: DocScorer<T>;
    fn for_segment(&self, reader: &SegmentReader) -> Self::Type;
}

impl<T, C, F> ScorerForSegment<T> for F
where
    F: 'static + Sync + Send + Fn(&SegmentReader) -> C,
    C: DocScorer<T>,
{
    type Type = C;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
        (self)(reader)
    }
}

pub trait DocScorer<T>: 'static {
    fn score(&self, doc_id: DocId) -> T;
}

impl<F, T> DocScorer<T> for F
where
    F: 'static + Sync + Send + Fn(DocId) -> T,
{
    fn score(&self, doc_id: DocId) -> T {
        (self)(doc_id)
    }
}
