use std::convert::TryFrom;

use tantivy::schema::{Field, Schema, SchemaBuilder, FAST, STORED, TEXT};

use crate::model::FeaturesFilterFields;

#[derive(Clone)]
pub struct IndexFields {
    pub id: Field,
    pub fulltext: Field,
    pub features: FeaturesFilterFields,
}

const FIELD_ID: &str = "id";
const FIELD_FULLTEXT: &str = "fulltext";

impl From<&mut SchemaBuilder> for IndexFields {
    fn from(builder: &mut SchemaBuilder) -> Self {
        IndexFields {
            id: builder.add_u64_field(FIELD_ID, STORED | FAST),
            fulltext: builder.add_text_field(FIELD_FULLTEXT, TEXT),
            features: FeaturesFilterFields::from(builder),
        }
    }
}

impl TryFrom<&Schema> for IndexFields {
    // TODO better error
    type Error = &'static str;

    fn try_from(schema: &Schema) -> Result<Self, Self::Error> {
        let id = schema.get_field(FIELD_ID).ok_or("missing id field")?;
        let fulltext = schema
            .get_field(FIELD_FULLTEXT)
            .ok_or("missing fulltext field")?;
        Ok(IndexFields {
            id,
            fulltext,
            features: FeaturesFilterFields::try_from(schema)?,
        })
    }
}
