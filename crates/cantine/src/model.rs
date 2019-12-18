use std::{convert::TryInto, mem::size_of};

use base64::{self, URL_SAFE_NO_PAD};
use serde::{
    de::{Deserializer, Error, Visitor},
    Deserialize, Serialize, Serializer,
};
use uuid::{self, Uuid};

use crate::database::DatabaseRecord;
use cantine_derive::FilterAndAggregation;

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub struct Recipe {
    pub uuid: Uuid,

    pub recipe_id: RecipeId,
    pub name: String,
    pub crawl_url: String,

    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub images: Vec<String>,

    pub similar_recipe_ids: Vec<u64>,

    pub features: Features,
}

pub type RecipeId = u64;

impl DatabaseRecord for Recipe {
    fn get_id(&self) -> u64 {
        self.recipe_id
    }
    fn get_uuid(&self) -> uuid::Bytes {
        *self.uuid.as_bytes()
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RecipeCard {
    pub name: String,
    pub uuid: Uuid,
    pub crawl_url: String,
    pub num_ingredients: u8,
    pub instructions_length: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calories: Option<u32>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RecipeInfo {
    pub uuid: Uuid,

    pub name: String,
    pub crawl_url: String,

    pub ingredients: Vec<String>,
    pub images: Vec<String>,

    pub num_ingredients: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calories: Option<u32>,
}

impl From<Recipe> for RecipeInfo {
    fn from(src: Recipe) -> Self {
        Self {
            uuid: src.uuid,
            name: src.name,
            crawl_url: src.crawl_url,

            ingredients: src.ingredients,
            images: src.images,

            num_ingredients: src.features.num_ingredients,
            total_time: src.features.total_time,
            calories: src.features.calories,
        }
    }
}

impl From<Recipe> for RecipeCard {
    fn from(src: Recipe) -> Self {
        Self {
            name: src.name,
            uuid: src.uuid,
            crawl_url: src.crawl_url,
            num_ingredients: src.features.num_ingredients,
            instructions_length: src.features.instructions_length,
            total_time: src.features.total_time,
            calories: src.features.calories,
        }
    }
}

#[derive(FilterAndAggregation, Serialize, Deserialize, Debug, Default, PartialEq, Clone)]
pub struct Features {
    pub num_ingredients: u8,
    pub instructions_length: u32,

    pub prep_time: Option<u32>,
    pub total_time: Option<u32>,
    pub cook_time: Option<u32>,

    pub calories: Option<u32>,
    pub fat_content: Option<f32>,
    pub carbohydrate_content: Option<f32>,
    pub protein_content: Option<f32>,

    pub diet_lowcarb: Option<f32>,
    pub diet_vegetarian: Option<f32>,
    pub diet_vegan: Option<f32>,
    pub diet_keto: Option<f32>,
    pub diet_paleo: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Sort {
    Relevance,
    NumIngredients,
    InstructionsLength,
    TotalTime,
    CookTime,
    PrepTime,
    Calories,
    FatContent,
    CarbContent,
    ProteinContent,
}

#[derive(Serialize, Deserialize, Debug, Default)]
#[serde(deny_unknown_fields)]
pub struct SearchQuery {
    pub fulltext: Option<String>,
    pub sort: Option<Sort>,
    pub num_items: Option<u8>,
    pub filter: Option<FeaturesFilterQuery>,
    pub agg: Option<FeaturesAggregationQuery>,
    pub after: Option<SearchCursor>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchResult {
    pub items: Vec<RecipeCard>,
    pub total_found: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agg: Option<FeaturesAggregationResult>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<SearchCursor>,
}

#[derive(Debug, Default, PartialEq)]
pub struct SearchCursor(pub u64, pub uuid::Bytes);

impl SearchCursor {
    pub const SIZE: usize = size_of::<SearchCursor>();

    pub fn new(score_bits: u64, uuid: &Uuid) -> Self {
        Self(score_bits, *uuid.as_bytes())
    }

    pub fn from_bytes(src: &[u8; Self::SIZE]) -> Self {
        let score_bits =
            u64::from_be_bytes(src[0..8].try_into().expect("Slice has correct length"));
        Self(
            score_bits,
            src[8..].try_into().expect("Slice has correct length"),
        )
    }

    pub fn write_bytes(&self, buf: &mut [u8; Self::SIZE]) {
        buf[0..8].copy_from_slice(&self.0.to_be_bytes());
        buf[8..].copy_from_slice(&self.1[..]);
    }
}

// XXX Only valid because I know the result is multiple of 4
const ENCODED_SEARCH_CURSOR_LEN: usize = SearchCursor::SIZE * 8 / 6;

impl Serialize for SearchCursor {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut buf = [0u8; SearchCursor::SIZE];

        self.write_bytes(&mut buf);

        let mut encode_buf = [0u8; ENCODED_SEARCH_CURSOR_LEN];
        base64::encode_config_slice(&buf, URL_SAFE_NO_PAD, &mut encode_buf[..]);

        let encoded = std::str::from_utf8(&encode_buf[..]).unwrap();
        serializer.serialize_str(encoded)
    }
}

struct SearchCursorVisitor;

impl<'de> Visitor<'de> for SearchCursorVisitor {
    type Value = SearchCursor;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Base64-encoded SearchCursor")
    }

    fn visit_bytes<E: Error>(self, input: &[u8]) -> Result<Self::Value, E> {
        if input.len() != ENCODED_SEARCH_CURSOR_LEN {
            return Err(Error::invalid_length(ENCODED_SEARCH_CURSOR_LEN, &self));
        }

        let mut decode_buf = [0u8; SearchCursor::SIZE];
        base64::decode_config_slice(input, URL_SAFE_NO_PAD, &mut decode_buf[..])
            .map_err(|_| Error::custom("base64_decode failed"))?;

        Ok(SearchCursor::from_bytes(
            &decode_buf.try_into().expect("Slice has correct length"),
        ))
    }
}

impl<'de> Deserialize<'de> for SearchCursor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(SearchCursorVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json;

    #[test]
    fn search_cursor_json_round_trip() {
        for i in 0..100 {
            let cursor = SearchCursor::new(i, &Uuid::new_v4());

            let serialized = serde_json::to_string(&cursor).unwrap();
            let deserialized = serde_json::from_str(&serialized).unwrap();

            assert_eq!(cursor, deserialized);
        }
    }
}
