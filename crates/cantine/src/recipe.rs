use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use cantine_derive::FilterAndAggregation;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Recipe {
    pub recipe_id: u64,
    pub name: String,
    pub slug: String,
    pub site_name: String,
    pub crawl_url: String,
    pub ingredients: Vec<String>,
    pub instructions: Vec<String>,
    pub diets: HashMap<String, f64>,

    pub prep_time: Option<u32>,
    pub total_time: Option<u32>,
    pub cook_time: Option<u32>,

    pub calories: Option<f64>,
    pub fat_content: Option<f64>,
    pub carbohydrate_content: Option<f64>,
    pub protein_content: Option<f64>,
    pub similar_recipe_ids: Vec<u64>,
}

#[derive(Serialize, Deserialize, Debug, Default, FilterAndAggregation)]
pub struct Feature {
    pub num_ingredients: u64,

    pub prep_time: Option<u64>,
    pub total_time: Option<u64>,
    pub cook_time: Option<u64>,

    pub calories: Option<f64>,
    pub fat_content: Option<f64>,
    pub carbohydrate_content: Option<f64>,
    pub protein_content: Option<f64>,

    pub diet_lowcarb: Option<f64>,
    pub diet_vegetarian: Option<f64>,
    pub diet_vegan: Option<f64>,
    pub diet_keto: Option<f64>,
    pub diet_paleo: Option<f64>,
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
    filters: Option<FeatureFilterQuery>,
    agg: Option<FeatureAggregationQuery>,
    // TODO decide how to expose After<score,@id>
    after: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchResult {
    // TODO RecipeCard
    items: Vec<u64>,
    agg: Option<FeatureAggregationResult>,
    // TODO Ref=SearchQuery.after
    after: Option<String>,
}
