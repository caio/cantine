use tantivy;

mod query_parser;
mod search;

pub type Result<T> = tantivy::Result<T>;

pub use search::Searcher;
