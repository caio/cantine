use std::marker::PhantomData;

use tantivy::{
    collector::{Collector, CustomScorer, SegmentCollector},
    DocAddress, DocId, Result, Score, SegmentLocalId, SegmentReader,
};

use super::{
    custom_score::CustomScoreTopCollector,
    topk::{TopK, TopKProvider},
    traits::{CheckCondition, ConditionForSegment},
};

/// A TopCollector like tantivy's, with added support for ordering
/// and conditions.
///
/// # Examples
///
/// ## A top-collector that behaves like `tantivy`s
///
/// The first `Score` type is usually inferred but we need to be
/// explicit for this example.
///
/// ```rust
/// # use tique::conditional_collector::{TopCollector,Descending};
/// let collector =
///     TopCollector::<tantivy::Score, Descending, _>::new(10, true);
/// ```
///
/// ## Custom condition from a closure.
///
/// ```rust
/// # use tantivy::{Score,SegmentReader};
/// # use tique::conditional_collector::{TopCollector,Ascending};
/// let condition_for_segment = |reader: &SegmentReader| {
///     // Fetch useful stuff from the `reader`, then:
///     |segment_id, doc_id, score, is_ascending| {
///         // Express whatever logic you want
///         true
///     }
/// };
///
/// let collector =
///     TopCollector::<Score, Ascending, _>::new(20, condition_for_segment);
/// ```
///
/// ## Customizing the Score
///
/// ```rust
/// # use tique::conditional_collector::{TopCollector, Ascending};
/// # use tantivy::{SegmentReader, DocId};
/// # let limit = 10;
/// # let condition = true;
/// // Any `tantivy::collector::CustomScorer` is valid
/// let scorer = |reader: &SegmentReader| {
///     |doc_id: DocId| -720
/// };
///
/// let custom_collector =
///     TopCollector::<i64, Ascending, _>::new(limit, condition)
///         .with_custom_scorer(scorer);
/// ```
///
/// ## Using a fast field as the score
///
/// One typical use-case for customizing scores is sorting by a
/// fast field, so we provide a helper for that.
///
/// *CAUTION*: Using a field that is not `FAST` or is of a different
/// type than the one you specify will lead to a panic at runtime.
///
/// ```rust
/// # use tique::conditional_collector::{Ascending, Descending, TopCollector};
/// # let rank_field = tantivy::schema::Field::from_field_id(0);
/// # let id_field = tantivy::schema::Field::from_field_id(1);
/// # let limit = 10;
/// # let condition = true;
/// let min_rank_collector =
///     TopCollector::<f64, Ascending, _>::new(limit, condition)
///         .top_fast_field(rank_field);
///
/// let top_ids_collector =
///     TopCollector::<u64, Descending, _>::new(limit, condition)
///         .top_fast_field(id_field);
/// ```
pub struct TopCollector<T, P, CF> {
    limit: usize,
    condition_for_segment: CF,
    _score: PhantomData<T>,
    _provider: PhantomData<P>,
}

impl<T, P, CF> TopCollector<T, P, CF>
where
    T: PartialOrd,
    P: TopKProvider<T, DocId>,
    CF: ConditionForSegment<T>,
{
    /// Creates a new TopCollector with capacity of `limit`
    /// and respecting the given `ConditionForSegment`
    /// implementation.
    pub fn new(limit: usize, condition_for_segment: CF) -> Self {
        if limit < 1 {
            panic!("Limit must be greater than 0");
        }
        TopCollector {
            limit,
            condition_for_segment,
            _score: PhantomData,
            _provider: PhantomData,
        }
    }
}

impl<T, P, CF> TopCollector<T, P, CF>
where
    T: 'static + Copy + Send + Sync + PartialOrd,
    P: 'static + Send + Sync + TopKProvider<T, DocId>,
    CF: Send + Sync + ConditionForSegment<T>,
{
    /// Transforms this collector into that that uses the given
    /// scorer instead of the default scoring functionality.
    pub fn with_custom_scorer<C: CustomScorer<T>>(
        self,
        custom_scorer: C,
    ) -> impl Collector<Fruit = CollectionResult<T>> {
        CustomScoreTopCollector::<T, P, _, _>::new(
            self.limit,
            self.condition_for_segment,
            custom_scorer,
        )
    }
}

macro_rules! impl_top_fast_field {
    ($type: ident, $err: literal) => {
        impl<P, CF> TopCollector<$type, P, CF>
        where
            P: 'static + Send + Sync + TopKProvider<$type, DocId>,
            CF: Send + Sync + ConditionForSegment<$type>,
        {
            /// Transforms this collector into one that sorts by the given
            /// fast field. Will panic if the field is not FAST or the wrong
            /// type.
            pub fn top_fast_field(
                self,
                field: tantivy::schema::Field,
            ) -> impl Collector<Fruit = CollectionResult<$type>> {
                let scorer_for_segment = move |reader: &SegmentReader| {
                    let ff = reader.fast_fields().$type(field).expect($err);
                    move |doc_id| ff.get(doc_id)
                };
                CustomScoreTopCollector::<$type, P, _, _>::new(
                    self.limit,
                    self.condition_for_segment,
                    scorer_for_segment,
                )
            }
        }
    };
}

impl_top_fast_field!(u64, "Field is not a fast u64 field");
impl_top_fast_field!(i64, "Field is not a fast i64 field");
impl_top_fast_field!(f64, "Field is not a fast f64 field");

impl<P, CF> Collector for TopCollector<Score, P, CF>
where
    P: 'static + Send + Sync + TopKProvider<Score, DocId>,
    CF: Sync + ConditionForSegment<Score>,
{
    type Fruit = CollectionResult<Score>;
    type Child = TopSegmentCollector<Score, P::Child, CF::Type>;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, children: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(P::merge_many(self.limit, children))
    }

    fn for_segment(
        &self,
        segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        Ok(TopSegmentCollector::new(
            segment_id,
            P::new_topk(self.limit),
            self.condition_for_segment.for_segment(reader),
        ))
    }
}

pub struct TopSegmentCollector<T, K, C> {
    total: usize,
    visited: usize,
    segment_id: SegmentLocalId,
    topk: K,
    condition: C,
    _marker: PhantomData<T>,
}

impl<T, K, C> TopSegmentCollector<T, K, C>
where
    T: Copy,
    K: TopK<T, DocId>,
    C: CheckCondition<T>,
{
    pub fn new(segment_id: SegmentLocalId, topk: K, condition: C) -> Self {
        Self {
            total: 0,
            visited: 0,
            segment_id,
            topk,
            condition,
            _marker: PhantomData,
        }
    }

    #[cfg(test)]
    fn into_topk(self) -> K {
        self.topk
    }

    pub fn collect(&mut self, doc: DocId, score: T) {
        self.total += 1;
        if self
            .condition
            .check(self.segment_id, doc, score, K::ASCENDING)
        {
            self.visited += 1;
            self.topk.visit(score, doc);
        }
    }

    pub fn into_unsorted_collection_result(self) -> CollectionResult<T> {
        let segment_id = self.segment_id;
        let items = self
            .topk
            .into_vec()
            .into_iter()
            .map(|(score, doc)| (score, DocAddress(segment_id, doc)))
            .collect();

        CollectionResult {
            total: self.total,
            visited: self.visited,
            items,
        }
    }
}

impl<K, C> SegmentCollector for TopSegmentCollector<Score, K, C>
where
    K: 'static + TopK<Score, DocId>,
    C: CheckCondition<Score>,
{
    type Fruit = CollectionResult<Score>;

    fn collect(&mut self, doc: DocId, score: Score) {
        TopSegmentCollector::collect(self, doc, score)
    }

    fn harvest(self) -> Self::Fruit {
        TopSegmentCollector::into_unsorted_collection_result(self)
    }
}

#[derive(Debug)]
pub struct CollectionResult<T> {
    pub total: usize,
    pub visited: usize,
    pub items: Vec<(T, DocAddress)>,
}

impl<T> CollectionResult<T> {
    pub fn has_next(&self) -> bool {
        self.visited - self.items.len() > 0
    }

    pub(crate) fn merge_many<K: TopK<T, DocAddress>>(mut topk: K, items: Vec<Self>) -> Self {
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::conditional_collector::{
        topk::{AscendingTopK, DescendingTopK},
        Ascending, Descending,
    };

    use tantivy::{
        query::{AllQuery, TermQuery},
        schema, Document, Index, Result, Term,
    };

    #[test]
    fn condition_is_checked() {
        const LIMIT: usize = 4;

        let mut nil_collector = TopSegmentCollector::new(0, AscendingTopK::new(LIMIT), false);

        let mut top_collector = TopSegmentCollector::new(0, AscendingTopK::new(LIMIT), true);

        let condition = |_sid, doc, _score, _asc| doc % 2 == 1;

        let mut just_odds = TopSegmentCollector::new(0, AscendingTopK::new(LIMIT), condition);

        for i in 0..4 {
            nil_collector.collect(i, 420.0);
            top_collector.collect(i, 420.0);
            just_odds.collect(i, 420.0);
        }

        assert_eq!(0, nil_collector.harvest().items.len());
        assert_eq!(4, top_collector.harvest().items.len());

        // Verify that the collected items respect the condition
        let result = just_odds.harvest();
        assert_eq!(4, result.total);
        assert_eq!(2, result.items.len());
        for (score, doc) in result.items {
            let DocAddress(seg_id, doc_id) = doc;
            assert!(condition(seg_id, doc_id, score, true))
        }
    }

    fn check_segment_collector<K, C>(
        topk: K,
        condition: C,
        input: Vec<(Score, DocId)>,
        wanted: Vec<(Score, DocId)>,
    ) where
        K: TopK<Score, DocId> + 'static,
        C: CheckCondition<Score>,
    {
        let mut collector = TopSegmentCollector::new(0, topk, condition);

        for (score, id) in input {
            collector.collect(id, score);
        }

        assert_eq!(wanted, collector.into_topk().into_sorted_vec());
    }

    #[test]
    fn collection_with_a_marker_smoke() {
        // XXX property test maybe? Essentially we are creating
        // a Vec<(Score, DocId)> sorted as `Scored` would,
        // then we pick an arbitrary position to pivot and
        // expect the DescendingTopK to pick everything below
        // and the AscendingTopK to pick everything above
        let marker = (0.5, DocAddress(0, 4));

        check_segment_collector(
            DescendingTopK::new(10),
            marker,
            vec![
                // Every doc with a higher score has appeared already
                (0.6, 7),
                (0.7, 5),
                // Docs with the same score, but lower id too
                (0.5, 3),
                (0.5, 2),
                // [pivot] And, of course, the same doc should not be collected
                (0.5, 4),
                // Lower scores are in
                (0.0, 1),
                // Same score but higher doc, too
                (0.5, 6),
            ],
            vec![(0.5, 6), (0.0, 1)],
        );

        check_segment_collector(
            AscendingTopK::new(10),
            marker,
            vec![
                // Every doc with a higher score should be picked
                (0.6, 7),
                (0.7, 5),
                // Same score but lower id as well
                (0.5, 3),
                (0.5, 2),
                // [pivot] The same doc should not be collected
                (0.5, 4),
                // Docs with lower scores are discarded
                (0.0, 1),
                // Same score but higher doc is discaraded too
                (0.5, 6),
            ],
            vec![(0.5, 2), (0.5, 3), (0.6, 7), (0.7, 5)],
        );
    }

    #[test]
    fn collection_ordering_integration() -> Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let text_field = builder.add_text_field("text", schema::TEXT);

        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        let add_doc = |text: &str| {
            let mut doc = Document::new();
            doc.add_text(text_field, text);
            writer.add_document(doc);
        };

        const NUM_DOCS: usize = 3;
        add_doc("the first doc is simple");
        add_doc("the second doc is a bit larger");
        add_doc("and the third document is rubbish");

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let collector_asc = TopCollector::<_, Ascending, _>::new(NUM_DOCS, true);
        let collector_desc = TopCollector::<_, Descending, _>::new(NUM_DOCS, true);

        // Query for "the", which matches all docs and yields
        // a distinct score for each
        let query = TermQuery::new(
            Term::from_field_text(text_field, "the"),
            schema::IndexRecordOption::WithFreqsAndPositions,
        );
        let (asc, desc) = searcher.search(&query, &(collector_asc, collector_desc))?;

        assert_eq!(NUM_DOCS, asc.items.len());
        assert_eq!(NUM_DOCS, desc.items.len());

        let asc_scores = asc
            .items
            .iter()
            .map(|(score, _doc)| score)
            .collect::<Vec<_>>();

        let mut prev = None;
        for score in &asc_scores {
            if let Some(previous) = prev {
                assert!(previous < score, "The scores should be ascending");
            }
            prev = Some(score)
        }

        let mut desc_scores = desc
            .items
            .iter()
            .map(|(score, _doc)| score)
            .collect::<Vec<_>>();

        desc_scores.reverse();
        assert_eq!(asc_scores, desc_scores);

        Ok(())
    }

    #[test]
    fn fast_field_collection() -> Result<()> {
        let mut builder = schema::SchemaBuilder::new();

        let field = builder.add_f64_field("field", schema::FAST);

        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

        const NUM_DOCS: usize = 100;
        for v in 0..NUM_DOCS {
            let mut doc = Document::new();
            doc.add_f64(field, f64::from(v as u32));
            writer.add_document(doc);
        }

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let collector_asc =
            TopCollector::<f64, Ascending, _>::new(NUM_DOCS, true).top_fast_field(field);
        let collector_desc =
            TopCollector::<f64, Descending, _>::new(NUM_DOCS, true).top_fast_field(field);

        let (top_asc, mut top_desc) =
            searcher.search(&AllQuery, &(collector_asc, collector_desc))?;

        assert_eq!(NUM_DOCS, top_asc.items.len());

        top_desc.items.reverse();
        assert_eq!(top_asc.items, top_desc.items);

        Ok(())
    }
}
