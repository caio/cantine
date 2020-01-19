use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

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

impl<T> CheckCondition<T> for (T, DocAddress)
where
    T: 'static + PartialOrd + Clone + Copy,
{
    fn check(&self, segment_id: SegmentLocalId, doc_id: DocId, score: T) -> bool {
        // FIXME descending?
        Scored::new(self.0, self.1) > Scored::new(score, DocAddress(segment_id, doc_id))
    }
}
