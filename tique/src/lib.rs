//! Utilities to drive a tantivy search index
//!
//! # Overview
//!
//! ## `conditional_collector`
//!
//! Collectors with built-in support for changing the ordering and
//! cursor-based pagination (or rather: support for conditionally
//! skipping documents that match the query).
//!
//! ```rust
//! use tique::conditional_collector::{Ascending, TopCollector};
//! # let f64_field = tantivy::schema::Field::from_field_id(0);
//!
//! let min_rank_collector =
//!     TopCollector::<f64, Ascending, _>::new(10, true).top_fast_field(f64_field);
//! ```
//!
//! Check the module docs for more details.
//!
//! ## Unstable
//!
//! This crate also contains unpolished functionality that is made availble
//! through the `unstable` feature flag:
//!
//! * `tique::query_parser`: A very simple query parser that only knows
//!   about term and phrase queries (and their negation). Mostly an excuse
//!   to play with `nom`
//!
//! * `tique_derive::FilterAndAggregation`: Takes a struct of (possibly
//!   `Option`) primitives and generates a bunch of code to assist with
//!   indexing, filtering and aggregating.
pub mod conditional_collector;

#[cfg(feature = "unstable")]
pub mod queryparser;
