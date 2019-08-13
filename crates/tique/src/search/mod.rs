use tantivy;

mod collector;
mod features;
mod parser;
mod query_parser;
mod search;

type Result<T> = tantivy::Result<T>;

pub use collector::FeatureCollector;
pub use features::{Feature, FeatureVector};
pub use query_parser::QueryParser;
pub use search::{AggregationRequest, FeatureIndexFields, RecipeIndex, SearchRequest};
