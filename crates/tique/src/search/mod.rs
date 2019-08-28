use tantivy;

mod collector;
mod featureindex;
mod featurevector;
mod parser;
mod query_parser;
mod top_collector;

type Result<T> = tantivy::Result<T>;

pub use collector::{AggregationRequest, FeatureCollector, FeatureRanges};
pub use featureindex::FeatureIndexFields;
pub use featurevector::{FeatureValue, FeatureVector};
pub use query_parser::QueryParser;
