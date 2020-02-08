# {{crate}}
[![crates.io](https://img.shields.io/crates/v/{{crate}}.svg)](https://crates.io/crates/tique)
[![docs.rs](https://docs.rs/{{crate}}/badge.svg)](https://docs.rs/tique)

{{readme}}

## Unstable

This crate also contains unpolished functionality that is made availble
through the `unstable` feature flag:

* `query_parser`: A very simple query parser that only knows about term
  and phrase queries (and their negation). Mostly an excuse to play
  with `nom`
