use std::{marker::PhantomData, ops::Neg};

use tantivy::{
    fastfield::{FastFieldReader, FastValue},
    schema::Field,
    DocId, SegmentReader,
};

use super::{DocScorer, ScorerForSegment};

pub fn descending<T>(field: Field) -> DescendingFastField<T> {
    DescendingFastField(field, PhantomData)
}

pub fn ascending<T>(field: Field) -> AscendingFastField<T> {
    AscendingFastField(field, PhantomData)
}

pub struct DescendingFastField<T>(Field, PhantomData<T>);

pub struct AscendingFastField<T>(Field, PhantomData<T>);

macro_rules! impl_scorer_for_segment {
    ($type: ident) => {
        impl ScorerForSegment<$type> for DescendingFastField<$type> {
            type Type = DescendingScorer<$type>;

            fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
                let scorer = reader.fast_fields().$type(self.0).expect("Field is FAST");
                DescendingScorer(scorer)
            }
        }

        impl ScorerForSegment<$type> for AscendingFastField<$type> {
            type Type = AscendingScorer<$type>;

            fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
                let scorer = reader.fast_fields().$type(self.0).expect("Field is FAST");
                AscendingScorer(scorer)
            }
        }
    };
}

impl_scorer_for_segment!(f64);
impl_scorer_for_segment!(i64);
impl_scorer_for_segment!(u64);

pub struct DescendingScorer<T: FastValue>(FastFieldReader<T>);

pub struct AscendingScorer<T: FastValue>(FastFieldReader<T>);

impl<T> DocScorer<T> for DescendingScorer<T>
where
    T: FastValue + 'static,
{
    fn score(&self, doc_id: DocId) -> T {
        self.0.get(doc_id)
    }
}

impl DocScorer<u64> for AscendingScorer<u64> {
    fn score(&self, doc_id: DocId) -> u64 {
        std::u64::MAX - self.0.get(doc_id)
    }
}

macro_rules! impl_neg_reversed_scorer {
    ($type: ty) => {
        impl DocScorer<$type> for AscendingScorer<$type> {
            fn score(&self, doc_id: DocId) -> $type {
                self.0.get(doc_id).neg()
            }
        }
    };
}

impl_neg_reversed_scorer!(i64);
impl_neg_reversed_scorer!(f64);

#[cfg(test)]
use super::{CollectionResult, CustomScoreTopCollector};

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::{
        query::AllQuery,
        schema::{SchemaBuilder, FAST},
        DocAddress, Document, Index, Result,
    };

    fn just_the_ids<T: PartialOrd>(res: CollectionResult<T>) -> Vec<DocId> {
        res.items
            .into_iter()
            .map(|item| {
                let DocAddress(_segment, id) = item.doc;
                id
            })
            .collect()
    }

    macro_rules! check_order_from_sorted_values {
        ($name: ident, $field: ident, $add: ident, $type: ty, $values: expr) => {
            #[test]
            fn $name() -> Result<()> {
                let mut sb = SchemaBuilder::new();

                let field = sb.$field("field", FAST);
                let index = Index::create_in_ram(sb.build());
                let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

                for v in $values.iter() {
                    let mut doc = Document::new();
                    doc.$add(field, *v);
                    writer.add_document(doc);
                }

                writer.commit()?;

                let reader = index.reader()?;
                let searcher = reader.searcher();
                let size = $values.len();
                let condition = true;

                let collector =
                    CustomScoreTopCollector::new(size, condition, descending::<$type>(field));

                let reversed_collector =
                    CustomScoreTopCollector::new(size, condition, ascending::<$type>(field));

                let (top, reversed_top) =
                    searcher.search(&AllQuery, &(collector, reversed_collector))?;

                let sorted_scores: Vec<$type> = top.items.iter().map(|r| r.score).collect();
                assert_eq!(
                    $values,
                    sorted_scores.as_slice(),
                    "found scores don't match input"
                );

                let ids = just_the_ids(top);
                let mut reversed_ids = just_the_ids(reversed_top);

                reversed_ids.reverse();
                assert_eq!(
                    ids,
                    reversed_ids.as_slice(),
                    "should have found the same ids, in reversed order"
                );

                Ok(())
            }
        };
    }

    check_order_from_sorted_values!(
        u64_field_sort_functionality,
        add_u64_field,
        add_u64,
        u64,
        [3, 2, 1, 0]
    );

    check_order_from_sorted_values!(
        i64_field_sort_functionality,
        add_i64_field,
        add_i64,
        i64,
        [3, 2, 1, 0, -1, -2, -3]
    );

    check_order_from_sorted_values!(
        f64_field_sort_functionality,
        add_f64_field,
        add_f64,
        f64,
        [100.0, 42.0, 0.71, 0.42]
    );
}
