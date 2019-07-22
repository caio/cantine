use tantivy;

mod search;

pub type Result<T> = tantivy::Result<T>;

pub use search::Searcher;
