#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_doc_code_examples)]
//! Utilities to drive a tantivy search index
//!
//! # Overview
//!
//! Here's a brief overview of the functionality we provide. Check the
//! module docs for more details and examples.
//!
//! ## conditional_collector
//!
//! Collectors with built-in support for changing the ordering and
//! cursor-based pagination (or rather: support for conditionally
//! skipping documents that match the query).
//!
//! ```no_run
//! use tique::conditional_collector::{Ascending, TopCollector};
//! # let f64_field = tantivy::schema::Field::from_field_id(0);
//!
//! let min_rank_collector =
//!     TopCollector::<f64, Ascending, _>::new(10, true).top_fast_field(f64_field);
//! ```
//!
//! ## topterms
//!
//! Uses your index to find keywords and similar items to your documents
//! or any arbitrary input.
//!
//!```no_run
//! # use tantivy::{Index, collector::TopDocs, schema::{Field, Schema, TEXT}};
//! # use tique::topterms::TopTerms;
//! # let mut builder = Schema::builder();
//! # let body = builder.add_text_field("body", TEXT);
//! # let title = builder.add_text_field("title", TEXT);
//! # let index = Index::create_in_ram(builder.build());
//! let topterms = TopTerms::new(&index, vec![body, title])?;
//! let keywords = topterms.extract(5, "the quick fox jumps over the lazy dog");
//!
//! let similarity_query = keywords.into_boosted_query(1.0);
//! # Ok::<(), tantivy::TantivyError>(())
//!```
pub mod conditional_collector;
pub mod topterms;

#[cfg(feature = "queryparser")]
mod queryparser;
#[cfg(feature = "queryparser")]
pub use queryparser::QueryParser;

mod dismax;
pub use dismax::DisMaxQuery;
