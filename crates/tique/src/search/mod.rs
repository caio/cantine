use tantivy;

mod parser;
mod query_parser;
mod top_collector;

type Result<T> = tantivy::Result<T>;

pub use query_parser::QueryParser;
