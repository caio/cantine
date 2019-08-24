use tantivy;

mod collector;
mod featureindex;
mod featurevector;
mod parser;
mod query_parser;

type Result<T> = tantivy::Result<T>;

pub use collector::{FeatureCollector, FeatureRanges};
pub use featureindex::{AggregationRequest, FeatureIndexFields, SearchRequest};
pub use featurevector::{FeatureValue, FeatureVector, IsUnset};
pub use query_parser::QueryParser;
