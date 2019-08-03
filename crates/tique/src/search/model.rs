use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum Diet {
    LowCarb,
    Vegetarian,
    Vegan,
    Keto,
    Paleo,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DietChoice(Diet, f32);

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

#[derive(Serialize, Deserialize, Debug)]
pub struct IntRange(u64, u64);

#[derive(Serialize, Deserialize, Debug)]
pub struct FloatRange(f64, f64);

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchQuery {
    // Shirley, there's a better way!
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fulltext: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Sort>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub diet: Option<DietChoice>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_ingredients: Option<IntRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_time: Option<IntRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cook_time: Option<IntRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub prep_time: Option<IntRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub kcal_content: Option<IntRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fat_content: Option<FloatRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub carb_content: Option<FloatRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub protein_content: Option<FloatRange>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_number: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json;

    #[test]
    fn can_deserialize_empty_obj() {
        assert!(serde_json::from_str::<SearchQuery>("{}").is_ok());
    }

    #[test]
    fn empty_serializes_to_empty() {
        assert_eq!(
            "{}",
            serde_json::to_string_pretty(&SearchQuery::default()).unwrap()
        );
    }
}
