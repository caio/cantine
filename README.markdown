# Cantine

A cooking recipe search API.

## Notes

This is mostly an exercise in learning rust, so if you are looking for
well-thought-out things you won't have much luck. The code here is
organized as a cargo workspace where the business logic and server
code are placed inside the `cantine` crate and isolated functionality
such as cursor-based pagination and query/aggregation-related code
generation is implemented in `tique`.

I plan on exploring the whole ecosystem so documentation will come
someday, but for now here's a brief outline of the modules:

* `tique::top_collector`: A `tantivy` top collector that allows
  checking for a condition before collecting a matching document,
  particularly useful in streaming and highly dynamic filtering
  contexts. Used in `cantine::index` as a way to navigate results
  without offsets/pages

* `tique::query_parser`: A simplified query parser that only knows
  about term and phrase queries (and their negation). Mostly an excuse
  to play with `nom`

* `tique_derive`: Takes a struct of (possibly `Option`) primitives and
  generates a bunch of code to assist with indexing, filtering and
  aggregating. Used by `cantine` to skip writing tedious business
  logic and to aggregate features by decoding a bytes fast field as
  a features struct

* `cantine::database`: A memory-mapped file used as database with the
  index stored in a separate log file and payload serialized as
  `bincode`
