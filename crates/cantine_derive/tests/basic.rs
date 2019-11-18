use std::ops::Range;

use cantine_derive::FilterAndAggregation;

#[allow(dead_code)]
#[derive(FilterAndAggregation)]
struct Features {
    pub a: u64,
    pub b: Option<i16>,
}

#[test]
fn compiles_ok() {
    FeaturesFilterQuery::default();
    FeaturesAggregationQuery::default();
    FeaturesAggregationResult::default();
}

#[test]
#[allow(unused_variables)]
fn reads_inner_type_of_option() {
    let filter_query = FeaturesFilterQuery::default();
    let filter_a: Option<Range<u64>> = filter_query.a;
    let filter_b: Option<Range<i16>> = filter_query.b;

    let agg_query = FeaturesAggregationQuery::default();
    let agg_a: Vec<Range<u64>> = agg_query.a;
    let agg_b: Vec<Range<i16>> = agg_query.b;
}

#[test]
fn aggregation_result_from_query() {
    let mut res: FeaturesAggregationResult = FeaturesAggregationQuery::default().into();
    assert!(res.a.is_empty());
    assert!(res.b.is_empty());

    res = FeaturesAggregationQuery {
        a: vec![0..10],
        ..FeaturesAggregationQuery::default()
    }
    .into();

    assert_eq!(vec![0], res.a);
    assert!(res.b.is_empty());

    res = FeaturesAggregationQuery {
        a: vec![0..10, 5..15],
        b: vec![-10..120],
    }
    .into();

    assert_eq!(vec![0, 0], res.a);
    assert_eq!(vec![0], res.b);
}
