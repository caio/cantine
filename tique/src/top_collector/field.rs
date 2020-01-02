use tantivy::{collector::Collector, schema::Field, SegmentReader};

use super::{CollectionResult, ConditionForSegment, CustomScoreTopCollector};

macro_rules! fast_field_custom_score_collector {
    ($name: ident, $type: ty, $reader: ident) => {
        pub fn $name<C>(
            field: Field,
            limit: usize,
            condition_factory: C,
        ) -> impl Collector<Fruit = CollectionResult<$type>>
        where
            C: ConditionForSegment<$type> + Sync,
        {
            let scorer_for_segment = move |reader: &SegmentReader| {
                let scorer = reader
                    .fast_fields()
                    .$reader(field)
                    .expect("Not a fast field");
                move |doc_id| scorer.get(doc_id)
            };
            CustomScoreTopCollector::new(limit, condition_factory, scorer_for_segment)
        }
    };
}

fast_field_custom_score_collector!(ordered_by_i64_fast_field, i64, i64);
fast_field_custom_score_collector!(ordered_by_u64_fast_field, u64, u64);
fast_field_custom_score_collector!(ordered_by_f64_fast_field, f64, f64);

#[cfg(test)]
mod tests {
    use super::*;

    use tantivy::{
        query::AllQuery,
        schema::{SchemaBuilder, FAST},
        Document, Index, Result,
    };

    #[test]
    fn integration() -> Result<()> {
        let mut sb = SchemaBuilder::new();

        let u64_field = sb.add_u64_field("u64", FAST);
        let i64_field = sb.add_i64_field("i64", FAST);
        let f64_field = sb.add_f64_field("f64", FAST);

        let index = Index::create_in_ram(sb.build());
        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;

        let add_doc = |a: u64, b: i64, c: f64| {
            let mut doc = Document::new();
            doc.add_u64(u64_field, a);
            doc.add_i64(i64_field, b);
            doc.add_f64(f64_field, c);
            writer.add_document(doc);
        };

        add_doc(10, -10, 7.2);
        add_doc(20, -20, 4.2);

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let u64_collector = ordered_by_u64_fast_field(u64_field, 2, true);
        let i64_collector = ordered_by_i64_fast_field(i64_field, 2, true);
        let f64_collector = ordered_by_f64_fast_field(f64_field, 2, true);

        let (top_u64, top_i64, top_f64) =
            searcher.search(&AllQuery, &(u64_collector, i64_collector, f64_collector))?;

        let sorted_u64_scores: Vec<u64> = top_u64.items.into_iter().map(|r| r.score).collect();
        assert_eq!(vec![20, 10], sorted_u64_scores);

        let sorted_i64_scores: Vec<i64> = top_i64.items.into_iter().map(|r| r.score).collect();
        assert_eq!(vec![-10, -20], sorted_i64_scores);

        let sorted_f64_scores: Vec<f64> = top_f64.items.into_iter().map(|r| r.score).collect();
        assert_eq!(vec![7.2, 4.2], sorted_f64_scores);

        Ok(())
    }
}
