use tantivy::{collector::Collector, schema::Field, SegmentReader};

use super::{CollectCondition, CustomScoreTopCollector, SearchMarker};

macro_rules! fast_field_custom_score_collector {
    ($name: ident, $type: ty, $reader: ident) => {
        pub fn $name<C>(
            field: Field,
            limit: usize,
            condition: C,
        ) -> impl Collector<Fruit = Vec<SearchMarker<$type>>>
        where
            C: CollectCondition<$type> + Sync,
        {
            let scorer_for_segment = move |reader: &SegmentReader| {
                let scorer = reader
                    .fast_fields()
                    .$reader(field)
                    .expect("Not a fast field");
                move |doc_id| scorer.get(doc_id)
            };
            CustomScoreTopCollector::new(limit, condition, scorer_for_segment)
        }
    };
}

fast_field_custom_score_collector!(ordered_by_i64_fast_field, i64, i64);
fast_field_custom_score_collector!(ordered_by_u64_fast_field, u64, u64);

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

        let index = Index::create_in_ram(sb.build());
        let mut writer = index.writer_with_num_threads(1, 50_000_000)?;

        let add_doc = |a: u64, b: i64| {
            let mut doc = Document::new();
            doc.add_u64(u64_field, a);
            doc.add_i64(i64_field, b);
            writer.add_document(doc);
        };

        add_doc(10, -10);
        add_doc(20, -20);

        writer.commit()?;

        let reader = index.reader()?;
        let searcher = reader.searcher();

        let top_u64_collector = ordered_by_u64_fast_field(u64_field, 2, true);
        let top_i64_collector = ordered_by_i64_fast_field(i64_field, 2, true);

        let (top_u64, top_i64) =
            searcher.search(&AllQuery, &(top_u64_collector, top_i64_collector))?;

        let sorted_u64_scores: Vec<u64> = top_u64.into_iter().map(|r| r.score).collect();

        assert_eq!(vec![20, 10], sorted_u64_scores);

        let sorted_i64_scores: Vec<i64> = top_i64.into_iter().map(|r| r.score).collect();

        assert_eq!(vec![-10, -20], sorted_i64_scores);

        Ok(())
    }
}
