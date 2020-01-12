use std::convert::TryInto;

use base64::{self, URL_SAFE_NO_PAD};
use serde::{
    de::{Deserializer, Error, Visitor},
    Deserialize, Serialize, Serializer,
};
use tantivy::Score;
use uuid::{self, Uuid};

use crate::database::DatabaseRecord;
use tique::FilterAndAggregation;

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
    pub image: Option<String>,
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

    pub num_ingredients: u8,
    pub ingredients: Vec<String>,

    pub instructions: Vec<String>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub images: Vec<String>,
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
            images: src.images,

            ingredients: src.ingredients,
            instructions: src.instructions,

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
            image: src.images.into_iter().next(),
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
    pub carb_content: Option<f32>,
    pub protein_content: Option<f32>,

    pub diet_lowcarb: Option<f32>,
    pub diet_vegetarian: Option<f32>,
    pub diet_vegan: Option<f32>,
    pub diet_keto: Option<f32>,
    pub diet_paleo: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
// XXX This can be derived from Features too
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
    pub num_items: Option<u8>,
    pub filter: Option<FeaturesFilterQuery>,
    pub agg: Option<FeaturesAggregationQuery>,
    pub after: Option<SearchCursor>,

    pub sort: Option<Sort>,
    #[serde(default)]
    pub ascending: bool,
}

#[derive(Serialize, Debug, Default)]
pub struct SearchResult {
    pub items: Vec<RecipeCard>,
    pub total_found: usize,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub agg: Option<FeaturesAggregationResult>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<SearchCursor>,
}

#[derive(Debug, PartialEq)]
pub enum SearchCursor {
    F64Field(f64, uuid::Bytes),
    U64Field(u64, uuid::Bytes),
    Relevance(Score, uuid::Bytes),
}

impl SearchCursor {
    /// tag + score_as_bits + uuid
    pub const SIZE: usize = 1 + 8 + 16;

    pub fn uuid(&self) -> &uuid::Bytes {
        match self {
            Self::Relevance(_, uuid) => uuid,
            Self::U64Field(_, uuid) => uuid,
            Self::F64Field(_, uuid) => uuid,
        }
    }

    pub fn from_bytes(src: &[u8; Self::SIZE]) -> Result<Self, &str> {
        // tag 0 + 0-padding for f32
        if src[0..5] == [0, 0, 0, 0, 0] {
            let score = f32::from_be_bytes(src[5..9].try_into().unwrap());
            Ok(Self::Relevance(score, src[9..].try_into().unwrap()))
        } else if src[0] == 1 {
            let score = u64::from_be_bytes(src[1..9].try_into().unwrap());
            Ok(Self::U64Field(score, src[9..].try_into().unwrap()))
        } else if src[0] == 2 {
            let score = f64::from_be_bytes(src[1..9].try_into().unwrap());
            Ok(Self::F64Field(score, src[9..].try_into().unwrap()))
        } else {
            Err("Invalid payload")
        }
    }

    pub fn write_bytes(&self, buf: &mut [u8; Self::SIZE]) {
        match self {
            Self::Relevance(score, uuid) => {
                // tag 0 + 0-padding
                buf[0..5].copy_from_slice(&[0, 0, 0, 0, 0]);
                buf[5..9].copy_from_slice(&score.to_be_bytes());
                buf[9..].copy_from_slice(&uuid[..]);
            }
            Self::U64Field(score, uuid) => {
                buf[0] = 1;
                buf[1..9].copy_from_slice(&score.to_be_bytes());
                buf[9..].copy_from_slice(&uuid[..]);
            }
            Self::F64Field(score, uuid) => {
                buf[0] = 2;
                buf[1..9].copy_from_slice(&score.to_be_bytes());
                buf[9..].copy_from_slice(&uuid[..]);
            }
        }
    }
}

const ENCODED_SEARCH_CURSOR_LEN: usize = 34;

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

        SearchCursor::from_bytes(&decode_buf.try_into().expect("Slice has correct length"))
            .map_err(|_| Error::custom("invalid payload"))
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

    use quickcheck::{quickcheck, TestResult};
    use serde_json;

    #[test]
    fn search_cursor_json_round_trip() {
        let roundtrip = |cursor| {
            let serialized = serde_json::to_string(&cursor).unwrap();
            let deserialized = serde_json::from_str(&serialized).unwrap();

            assert_eq!(cursor, deserialized);
        };

        for i in 0..100 {
            roundtrip(SearchCursor::Relevance(
                i as f32 * 1.0f32,
                *Uuid::new_v4().as_bytes(),
            ));
            roundtrip(SearchCursor::U64Field(i, *Uuid::new_v4().as_bytes()));
            roundtrip(SearchCursor::F64Field(
                i as f64 * 1.0f64,
                *Uuid::new_v4().as_bytes(),
            ));
        }
    }

    fn search_cursor_from_bytes(mut input: Vec<u8>) -> TestResult {
        if input.len() != SearchCursor::SIZE {
            TestResult::discard()
        } else {
            // Must not crash ever
            let _result = SearchCursor::from_bytes(input.as_slice().try_into().unwrap());

            // Tag=1 uses the whole payload
            input[0] = 1;
            SearchCursor::from_bytes(input.as_slice().try_into().unwrap())
                .expect("SearchCursor::U64Field");

            // Tag=2 uses the whole payload
            input[0] = 2;
            SearchCursor::from_bytes(input.as_slice().try_into().unwrap())
                .expect("SearchCursor::F64Field");

            // Tag=0 requires padding
            input[0..5].copy_from_slice(&[0, 0, 0, 0, 0]);
            SearchCursor::from_bytes(input.as_slice().try_into().unwrap())
                .expect("SearchCursor::Relevance");

            TestResult::passed()
        }
    }

    #[allow(unused_must_use)]
    fn search_cursor_from_base64(input: Vec<u8>) -> TestResult {
        if input.len() != ENCODED_SEARCH_CURSOR_LEN {
            TestResult::discard()
        } else {
            let visitor = SearchCursorVisitor;
            visitor.visit_bytes::<serde_json::Error>(input.as_slice().try_into().unwrap());
            TestResult::passed()
        }
    }

    #[test]
    fn search_cursor_deserialization_does_not_crash() {
        quickcheck(search_cursor_from_bytes as fn(Vec<u8>) -> TestResult);
        quickcheck(search_cursor_from_base64 as fn(Vec<u8>) -> TestResult);
    }
}
