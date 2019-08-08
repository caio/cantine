use serde::{Deserialize, Serialize};

pub use super::features::Feature;

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
pub struct IntRange(u16, u16);

impl From<std::ops::Range<u16>> for IntRange {
    fn from(src: std::ops::Range<u16>) -> Self {
        IntRange(src.start, src.end)
    }
}

impl Into<std::ops::Range<u64>> for &IntRange {
    fn into(self) -> std::ops::Range<u64> {
        let IntRange(start, end) = self;
        // NOTE (start, end+1) because std::ops::Range is half-open
        (*start as u64)..(*end as u64 + 1)
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fulltext: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Sort>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<(Feature, IntRange)>>,

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
