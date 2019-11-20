use std::convert::TryFrom;

use tantivy::schema::{Field, Schema, SchemaBuilder, FAST, STORED, TEXT};

#[derive(Clone)]
pub struct IndexFields {
    pub id: Field,
    pub fulltext: Field,
    pub features: Field,
}

const FIELD_ID: &str = "id";
const FIELD_FULLTEXT: &str = "fulltext";
const FIELD_FEATURES: &str = "features_bincode";

impl From<&mut SchemaBuilder> for IndexFields {
    fn from(builder: &mut SchemaBuilder) -> Self {
        IndexFields {
            id: builder.add_u64_field(FIELD_ID, STORED | FAST),
            fulltext: builder.add_text_field(FIELD_FULLTEXT, TEXT),
            features: builder.add_bytes_field(FIELD_FEATURES),
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
        let features = schema
            .get_field(FIELD_FEATURES)
            .ok_or("missing features field")?;
        Ok(IndexFields {
            id,
            fulltext,
            features,
        })
    }
}
