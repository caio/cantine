use tantivy;

mod collector;
mod features;
mod model;
mod parser;
mod query_parser;
mod search;

type Result<T> = tantivy::Result<T>;

pub use features::{Feature, FeatureVector};
pub use model::SearchQuery;
pub use search::{FeatureIndexFields, RecipeIndex};
