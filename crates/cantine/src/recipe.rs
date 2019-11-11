use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cantine_derive::FilterAndAggregation;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct Recipe {
    pub uuid: Uuid,

    pub recipe_id: u64,
    pub name: String,
    pub crawl_url: String,

    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub images: Vec<String>,

    pub similar_recipe_ids: Vec<u64>,

    pub features: Features,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct RecipeCard {
    pub uuid: Uuid,
    pub crawl_url: String,
    pub num_ingredients: u8,
    pub instructions_length: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calories: Option<f32>,
}

impl From<Recipe> for RecipeCard {
    fn from(src: Recipe) -> Self {
        Self {
            uuid: src.uuid,
            crawl_url: src.crawl_url,
            num_ingredients: src.features.num_ingredients,
            instructions_length: src.features.instructions_length,
            total_time: src.features.total_time,
            calories: src.features.calories,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, PartialEq, FilterAndAggregation)]
pub struct Features {
    pub num_ingredients: u8,
    pub instructions_length: u32,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prep_time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_time: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cook_time: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub calories: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fat_content: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub carbohydrate_content: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protein_content: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub diet_lowcarb: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diet_vegetarian: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diet_vegan: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diet_keto: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diet_paleo: Option<f32>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Sort {
    Relevance,
    NumIngredients,
    TotalTime,
    CookTime,
    PrepTime,
    Calories,
    FatContent,
    CarbContent,
    ProteinContent,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchQuery {
    fulltext: Option<String>,
    sort: Option<Sort>,
    num_items: Option<u8>,
    filters: Option<FeaturesFilterQuery>,
    agg: Option<FeaturesAggregationQuery>,
    // TODO decide how to expose After<score,@id>
    after: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchResult {
    items: Vec<RecipeCard>,

    #[serde(skip_serializing_if = "Option::is_none")]
    agg: Option<FeaturesAggregationResult>,

    // TODO Ref=SearchQuery.after
    #[serde(skip_serializing_if = "Option::is_none")]
    after: Option<String>,
}
