use std::{convert::TryFrom, ops::Range};
use tantivy::schema::SchemaBuilder;

use cantine_derive::FilterAndAggregation;

#[derive(FilterAndAggregation, Default)]
pub struct Feat {
    pub a: u64,
    pub b: Option<i16>,
    pub c: usize,
}

#[test]
#[allow(unused_variables)]
fn reads_inner_type_of_option() {
    let filter_query = FeatFilterQuery::default();
    let filter_a: Option<Range<u64>> = filter_query.a;
    let filter_b: Option<Range<i16>> = filter_query.b;

    let agg_query = FeatAggregationQuery::default();
    let agg_a: Vec<Range<u64>> = agg_query.a;
    let agg_b: Vec<Range<i16>> = agg_query.b;
}

#[test]
fn aggregation_result_from_query() {
    let mut res: FeatAggregationResult = FeatAggregationQuery::default().into();
    assert!(res.a.is_empty());
    assert!(res.b.is_empty());

    res = FeatAggregationQuery {
        a: vec![0..10],
        ..FeatAggregationQuery::default()
    }
    .into();

    assert_eq!(vec![0], res.a);
    assert!(res.b.is_empty());

    res = FeatAggregationQuery {
        a: vec![0..10, 5..15],
        b: vec![-10..120],
        ..FeatAggregationQuery::default()
    }
    .into();

    assert_eq!(vec![0, 0], res.a);
    assert_eq!(vec![0], res.b);
}

#[test]
fn can_merge_agg_result() {
    let mut res = FeatAggregationResult {
        a: vec![0, 12, 100],
        b: vec![37],
        ..FeatAggregationResult::default()
    };

    res.merge_same_size(&FeatAggregationResult {
        a: vec![10, 3, 900],
        b: vec![5],
        ..FeatAggregationResult::default()
    });

    assert_eq!(vec![10, 15, 1000], res.a);
    assert_eq!(vec![42], res.b);
    assert!(res.c.is_empty());
}

#[test]
fn collect_works_as_intended() {
    let query = FeatAggregationQuery {
        a: vec![],
        b: vec![-10..0, 0..10],
        c: vec![42..420],
    };

    let mut agg = FeatAggregationResult::from(&query);

    agg.collect(
        &query,
        &Feat {
            a: 10,
            b: None,
            c: 100,
        },
    );

    assert!(agg.a.is_empty());
    assert_eq!(vec![0, 0], agg.b);
    assert_eq!(vec![1], agg.c);

    agg.collect(
        &query,
        &Feat {
            b: Some(300),
            c: 0,
            ..Feat::default()
        },
    );

    // No change
    assert_eq!(vec![0, 0], agg.b);
    assert_eq!(vec![1], agg.c);

    agg.collect(
        &query,
        &Feat {
            b: Some(-5),
            c: 0,
            ..Feat::default()
        },
    );

    assert_eq!(vec![1, 0], agg.b);
    assert_eq!(vec![1], agg.c);

    agg.collect(
        &query,
        &Feat {
            b: Some(7),
            c: 400,
            ..Feat::default()
        },
    );

    assert_eq!(vec![1, 1], agg.b);
    assert_eq!(vec![2], agg.c);
}

#[test]
fn filter_fields_can_read_and_write_from_schema() {
    let mut builder = SchemaBuilder::new();
    let original = FeatFilterFields::from(&mut builder);
    let loaded = FeatFilterFields::try_from(&builder.build()).unwrap();
    assert_eq!(original, loaded);
}
