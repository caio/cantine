use std::convert::TryFrom;

use tantivy::{
    schema::{SchemaBuilder, Value},
    Document,
};

use cantine_derive::FilterQuery;

#[derive(FilterQuery, Default)]
pub struct Feat {
    pub a: u64,
    pub b: Option<i16>,
    pub c: f32,
    pub d: Option<f64>,
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
