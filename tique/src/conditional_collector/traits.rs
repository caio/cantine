use std::cmp::Ordering;

use tantivy::{DocAddress, DocId, SegmentOrdinal, SegmentReader};

use super::topk::Scored;

/// A trait that allows defining arbitrary conditions to be checked
/// before considering a matching document for inclusion in the
/// top results.
pub trait ConditionForSegment<T>: Clone {
    /// The concrete type of the result from calling `for_segment`
    type Type: CheckCondition<T>;

    /// Creates a `Self::Type` instance responsible for checking if
    /// the matching documents in the given segment reader are to
    /// be considered as collection candidates.
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

impl<T> ConditionForSegment<T> for (T, DocAddress)
where
    T: 'static + PartialOrd + Copy,
{
    type Type = Self;
    fn for_segment(&self, _reader: &SegmentReader) -> Self::Type {
        *self
    }
}

/// The condition that gets checked before collection. In order for
/// a document to appear in the results it must first return true
/// for `check`.
pub trait CheckCondition<T>: 'static + Clone {
    /// Decides wether the given document is a valid candidate or not
    ///
    /// The `ascending` parameter signals the ordering chosen via
    /// `Ascending` or `Descending`
    fn check(&self, segment_ord: SegmentOrdinal, doc_id: DocId, score: T, ascending: bool) -> bool;
}

impl<T> CheckCondition<T> for bool {
    fn check(&self, _: SegmentOrdinal, _: DocId, _: T, _: bool) -> bool {
        *self
    }
}

impl<F, T> CheckCondition<T> for F
where
    F: 'static + Clone + Fn(SegmentOrdinal, DocId, T, bool) -> bool,
{
    fn check(&self, segment_ord: SegmentOrdinal, doc_id: DocId, score: T, ascending: bool) -> bool {
        (self)(segment_ord, doc_id, score, ascending)
    }
}

impl<T> CheckCondition<T> for (T, DocAddress)
where
    T: 'static + PartialOrd + Copy,
{
    fn check(&self, segment_ord: SegmentOrdinal, doc_id: DocId, score: T, ascending: bool) -> bool {
        let wanted = if ascending {
            Ordering::Less
        } else {
            Ordering::Greater
        };

        Scored::new(self.0, self.1).cmp(&Scored::new(
            score,
            DocAddress {
                segment_ord,
                doc_id,
            },
        )) == wanted
    }
}
