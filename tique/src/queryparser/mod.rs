//! # QueryParser
//!
//! A query parser with a simple grammar geared towards usage by
//! end-users, with no knowledge about IR, your index nor boolean
//! logic.
//!
//! Supports multiple fields, boosts, required (+) and restricted (-)
//! items and can generate queries using `DisMaxQuery` for better
//! results when you have fields with very similar vocabularies.
//!
//! **NOTE**: Requires the `queryparser` compilation feature.
//!
//! ```no_run
//! # use tantivy::{Index, schema::Field};
//! # fn test(index: &Index) -> tantivy::Result<()> {
//! # let name = tantivy::schema::Field::from_field_id(0);
//! # let ingredients = tantivy::schema::Field::from_field_id(1);
//! let parser = tique::QueryParser::new(&index, vec![name, ingredients])?;
//!
//! if let Some(query) = parser.parse(r#"+bacon cheese -ingredients:olive "deep fry""#) {
//!     // Do your thing with the query...
//! }
//!
//! # Ok(())
//! # }
//! ```
mod parser;
mod raw;

pub use parser::QueryParser;
