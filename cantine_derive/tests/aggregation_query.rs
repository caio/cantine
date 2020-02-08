use bincode;

use serde::{Deserialize, Serialize};
use tantivy::{query::AllQuery, schema::SchemaBuilder, Document, Index, SegmentReader};

use cantine_derive::{Aggregable, AggregableCollector, RangeStats};

#[derive(Aggregable, Serialize, Deserialize, Default)]
pub struct Feat {
    pub a: u64,
    pub b: Option<i16>,
    pub c: f32,
    pub d: Option<f64>,
}

type FeatAggregationResult = <Feat as Aggregable>::Agg;
type FeatAggregationQuery = <Feat as Aggregable>::Query;

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
fn agg_query_full_range_generation() {
    assert_eq!(
        FeatAggregationQuery {
            a: vec![std::u64::MIN..std::u64::MAX],
            b: vec![std::i16::MIN..std::i16::MAX],
            c: vec![std::f32::MIN..std::f32::MAX],
            d: vec![std::f64::MIN..std::f64::MAX],
        },
        FeatAggregationQuery::full_range(),
    );
}

#[test]
fn collector_integration() -> tantivy::Result<()> {
    let mut builder = SchemaBuilder::new();

    let bytes_field = builder.add_bytes_field("bincode_feat");

    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

    let add_feat = |feat| {
        let mut doc = Document::new();
        doc.add_bytes(
            bytes_field,
            bincode::serialize(&feat).expect("serialize ok"),
        );

        writer.add_document(doc);
    };

    add_feat(Feat {
        a: 1,
        c: 1.0,
        ..Feat::default()
    });

    add_feat(Feat {
        a: 2,
        b: Some(2),
        c: 2.0,
        ..Feat::default()
    });

    add_feat(Feat {
        a: 3,
        b: Some(3),
        c: 3.0,
        d: Some(3.0),
    });

    writer.commit()?;

    let query = FeatAggregationQuery {
        a: vec![0..1, 2..4],
        b: vec![0..3],
        c: vec![0.0..0.1, 1.0..3.1],
        d: vec![42.0..100.0],
    };

    let collector =
        AggregableCollector::<Feat, _>::new(query, move |seg_reader: &SegmentReader| {
            let reader = seg_reader.fast_fields().bytes(bytes_field).unwrap();
            move |doc| bincode::deserialize(reader.get_bytes(doc)).ok()
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
