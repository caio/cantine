use tantivy;

mod model;
mod parser;
mod query_parser;
mod search;

type Result<T> = tantivy::Result<T>;

pub use search::RecipeIndex;
pub use model::SearchQuery;
