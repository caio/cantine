use tantivy;

mod collector;
mod features;
mod parser;
mod query_parser;
mod search;

type Result<T> = tantivy::Result<T>;

pub use collector::{FeatureCollector, FeatureRanges};
pub use features::{FeatureValue, FeatureVector, IsUnset};
pub use query_parser::QueryParser;
pub use search::{AggregationRequest, FeatureIndexFields, SearchRequest};
