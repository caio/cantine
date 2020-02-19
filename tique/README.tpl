# {{crate}}
[![crates.io](https://img.shields.io/crates/v/{{crate}}.svg)](https://crates.io/crates/tique)
[![docs.rs](https://docs.rs/{{crate}}/badge.svg)](https://docs.rs/tique)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

{{readme}}

## Dependency Policy

This library's default dependency will always be just `tantivy`, anything
that requires more will be added as optional feature.

## Unstable

This crate contains unpolished functionality that is made available
through the `unstable` feature flag:

* `query_parser`: A very simple query parser that only knows about term
  and phrase queries (and their negation). Mostly an excuse to play
  with `nom`
