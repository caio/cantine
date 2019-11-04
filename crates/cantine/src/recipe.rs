use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
