# Cantine

This is a cargo workspace containing:

* [tique][] is a [public crate][pub], with [live documentation][doc].
  It features utilities for [tantivy][] so you can build your own
  full text search engine. Refer to the [tique][] subdirectory for
  additional information and docs.

And a couple of crates developed alongside as a public dogfooding
and learning exercise:

* `cantine` is a recipe search API. It wires a memory-mapped file as
  a metadata db (`cantine::database`) with the tantivy search index
  (`cantine::index`) under a `actix-web`-based server.

* `cantine_derive`: Takes a struct of (possibly `Option`) primitives
  and generates a bunch of code to assist with indexing, filtering and
  aggregations. Used by `cantine` to skip writing tedious business
  logic and to aggregate features by decoding a bytes fast field as
  a features struct.


[tique]: tique/
[pub]: https://crates.io/crates/tique
[doc]: https://docs.rs/tique
[tantivy]: https://github.com/tantivy-search/tantivy