use serde::{Deserialize, Serialize};

use crate::search::Feature;

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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Range(u16, u16);

impl Range {
    pub fn contains(&self, item: u16) -> bool {
        let Range(start, end) = self;
        item >= *start && item <= *end
    }
}

impl From<(u16, u16)> for Range {
    fn from(src: (u16, u16)) -> Self {
        Range(src.0, src.1)
    }
}

impl From<std::ops::Range<u16>> for Range {
    fn from(src: std::ops::Range<u16>) -> Self {
        Range(src.start, src.end)
    }
}

impl Into<std::ops::Range<u64>> for &Range {
    fn into(self) -> std::ops::Range<u64> {
        let Range(start, end) = self;
        // NOTE (start, end+1) because std::ops::Range is half-open
        (*start as u64)..(*end as u64 + 1)
    }
}

pub type AggregationSpec = Vec<(Feature, Vec<Range>)>;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchQuery {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fulltext: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Sort>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Vec<(Feature, Range)>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_number: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub page_size: Option<u8>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub aggregations: Option<AggregationSpec>,
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
