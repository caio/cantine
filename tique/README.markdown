# tique
[![crates.io](https://img.shields.io/crates/v/tique.svg)](https://crates.io/crates/tique)
[![docs.rs](https://docs.rs/tique/badge.svg)](https://docs.rs/tique)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

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

let similarity_query = keywords.into_boosted_query(1.0);
```

### QueryParser

A query parser with a simple grammar geared towards usage by
end-users, with no knowledge about IR, your index nor boolean
logic.

Supports multiple fields, boosts, required (+) and restricted (-)
items and can generate queries using `DisMaxQuery` for better
results when you have fields with very similar vocabularies.

**NOTE**: Requires the `queryparser` compilation feature.

```rust
let parser = tique::QueryParser::new(&index, vec![name, ingredients])?;

if let Some(query) = parser.parse(r#"+bacon cheese -ingredients:olive "deep fry""#) {
    // Do your thing with the query...
}

```

## Dependency Policy

This library's default dependency will always be just `tantivy`, anything
that requires more will be added as optional feature.
