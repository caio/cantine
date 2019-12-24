use std::{collections::HashMap, convert::TryFrom, ops::Range, sync::Arc};

use tantivy::{
    query::AllQuery,
    schema::{SchemaBuilder, Value},
    Document, Index, SegmentReader,
};

use tique::{FilterAndAggregation, RangeStats};

#[derive(FilterAndAggregation, Default)]
pub struct Feat {
    pub a: u64,
    pub b: Option<i16>,
    pub c: f32,
    pub d: Option<f64>,
}

#[test]
#[allow(unused_variables)]
fn reads_inner_type_of_option() {
    let filter_query = FeatFilterQuery::default();
    let filter_a: Option<Range<u64>> = filter_query.a;
    let filter_b: Option<Range<i16>> = filter_query.b;
    let filter_c: Option<Range<f32>> = filter_query.c;

    let agg_query = FeatAggregationQuery::default();
    let agg_a: Vec<Range<u64>> = agg_query.a;
    let agg_b: Vec<Range<i16>> = agg_query.b;
}

#[test]
fn aggregation_result_from_query() {
    let res = FeatAggregationResult::from(FeatAggregationQuery {
        a: vec![0..10, 5..15],
        b: vec![-10..120],
        ..FeatAggregationQuery::default()
    });

    assert_eq!(2, res.a.len());
    assert_eq!(1, res.b.len());
    assert!(res.c.is_empty());
    assert!(res.d.is_empty());
}

fn agg_counts<T: serde::Serialize>(items: &[RangeStats<T>]) -> Vec<u64> {
    items.iter().map(|s| s.count).collect()
}

#[test]
fn collect_works_as_intended() {
    let query = FeatAggregationQuery {
        a: vec![],
        b: vec![-10..0, 0..10],
        c: vec![42.0..420.0],
        ..FeatAggregationQuery::default()
    };

    let mut agg = FeatAggregationResult::from(&query);

    agg.collect(
        &query,
        &Feat {
            a: 10,
            b: None,
            c: 100.0,
            ..Feat::default()
        },
    );

    assert!(agg.a.is_empty());
    assert_eq!(vec![0, 0], agg_counts(&agg.b));
    assert_eq!(vec![1], agg_counts(&agg.c));

    agg.collect(
        &query,
        &Feat {
            b: Some(300),
            c: 0.0,
            ..Feat::default()
        },
    );

    // No change
    assert!(agg.a.is_empty());
    assert_eq!(vec![0, 0], agg_counts(&agg.b));
    assert_eq!(vec![1], agg_counts(&agg.c));

    agg.collect(
        &query,
        &Feat {
            b: Some(-5),
            c: 0.0,
            ..Feat::default()
        },
    );

    assert_eq!(vec![1, 0], agg_counts(&agg.b));
    assert_eq!(vec![1], agg_counts(&agg.c));

    agg.collect(
        &query,
        &Feat {
            b: Some(7),
            c: 400.0,
            ..Feat::default()
        },
    );

    assert_eq!(vec![1, 1], agg_counts(&agg.b));
    assert_eq!(vec![2], agg_counts(&agg.c));
}

#[test]
fn filter_fields_can_read_and_write_from_schema() {
    let mut builder = SchemaBuilder::new();
    let original = FeatFilterFields::from(&mut builder);
    let loaded = FeatFilterFields::try_from(&builder.build()).unwrap();
    assert_eq!(original, loaded);
}

#[test]
fn filter_query_interpretation() {
    let mut builder = SchemaBuilder::new();
    let fields = FeatFilterFields::from(&mut builder);

    assert_eq!(
        0,
        fields
            .interpret(&FeatFilterQuery {
                ..FeatFilterQuery::default()
            })
            .len()
    );

    assert_eq!(
        1,
        fields
            .interpret(&FeatFilterQuery {
                a: Some(0..10),
                ..FeatFilterQuery::default()
            })
            .len()
    );

    assert_eq!(
        2,
        fields
            .interpret(&FeatFilterQuery {
                a: Some(0..10),
                c: Some(1.1..2.2),
                ..FeatFilterQuery::default()
            })
            .len()
    );
}

#[test]
fn add_to_doc_sets_fields_properly() {
    let mut builder = SchemaBuilder::new();
    let fields = FeatFilterFields::from(&mut builder);

    let mut doc = Document::new();

    fields.add_to_doc(
        &mut doc,
        &Feat {
            a: 10,
            d: Some(0.42),
            ..Feat::default()
        },
    );

    // Set values are filled properly
    assert_eq!(Some(&Value::U64(10)), doc.get_first(fields.a));
    assert_eq!(Some(&Value::F64(0.0)), doc.get_first(fields.c));
    assert_eq!(Some(&Value::F64(0.42)), doc.get_first(fields.d));
    // Unsed optional values aren't added
    assert_eq!(None, doc.get_first(fields.b));
}

#[test]
fn collector_integration() -> tantivy::Result<()> {
    let mut builder = SchemaBuilder::new();

    let id_field = builder.add_u64_field("id", tantivy::schema::FAST);
    let fields = FeatFilterFields::from(&mut builder);

    let index = Index::create_in_ram(builder.build());

    let mut writer = index.writer_with_num_threads(1, 50_000_000)?;
    let mut db = HashMap::new();

    let mut add_feat = |id: u64, feat| {
        let mut doc = Document::new();

        doc.add_u64(id_field, id);
        fields.add_to_doc(&mut doc, &feat);
        writer.add_document(doc);

        db.insert(id, feat);
    };

    add_feat(
        1,
        Feat {
            a: 1,
            c: 1.0,
            ..Feat::default()
        },
    );

    add_feat(
        2,
        Feat {
            a: 2,
            b: Some(2),
            c: 2.0,
            ..Feat::default()
        },
    );

    add_feat(
        3,
        Feat {
            a: 3,
            b: Some(3),
            c: 3.0,
            d: Some(3.0),
        },
    );

    writer.commit()?;

    let query = FeatAggregationQuery {
        a: vec![0..1, 2..4],
        b: vec![0..3],
        c: vec![0.0..0.1, 1.0..3.1],
        d: vec![42.0..100.0],
    };

    let db = Arc::new(db);
    let collector = FeatCollector::new(query, move |seg_reader: &SegmentReader| {
        let id_reader = seg_reader.fast_fields().u64(id_field).unwrap();
        let db = db.clone();
        move |doc, query, agg| {
            let id = id_reader.get(doc);
            agg.collect(query, db.get(&id).unwrap());
        }
    });

    let reader = index.reader()?;
    let searcher = reader.searcher();

    let agg_result = searcher.search(&AllQuery, &collector)?;

    assert_eq!(vec![0, 2], agg_counts(&agg_result.a));
    assert_eq!(vec![1], agg_counts(&agg_result.b));
    assert_eq!(vec![0, 3], agg_counts(&agg_result.c));
    assert_eq!(vec![0], agg_counts(&agg_result.d));

    Ok(())
}
