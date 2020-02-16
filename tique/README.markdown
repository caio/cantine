# tique
[![crates.io](https://img.shields.io/crates/v/tique.svg)](https://crates.io/crates/tique)
[![docs.rs](https://docs.rs/tique/badge.svg)](https://docs.rs/tique)

Utilities to drive a tantivy search index

## Overview

Here's a brief overview of the functionality we provide. Check the
module docs for more details and examples.

### conditional_collector

Collectors with built-in support for changing the ordering and
cursor-based pagination (or rather: support for conditionally
skipping documents that match the query).

```rust
use tique::conditional_collector::{Ascending, TopCollector};

let min_rank_collector =
    TopCollector::<f64, Ascending, _>::new(10, true).top_fast_field(f64_field);
```

### topterms

Uses your index to find keywords and similar items to your documents
or any arbitrary input.

```rust
let topterms = TopTerms::new(&index, vec![body, title])?;
let keywords = topterms.extract(5, "the quick fox jumps over the lazy dog");
```

## Unstable

This crate also contains unpolished functionality that is made availble
through the `unstable` feature flag:

* `query_parser`: A very simple query parser that only knows about term
  and phrase queries (and their negation). Mostly an excuse to play
  with `nom`
