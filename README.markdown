# Cantine

A cooking recipe search JSON API.

## Walkthrough

The API is publicly accessible:

```bash
export API=https://caio.co/recipes/api/v0
export CT="Content-Type: application/json"
```

You can query via `POST` on `/search`:

```bash
curl -d'{ "fulltext": "bacon", "num_items": 3 }' -H "${CT}" ${API}/search
```

The output will contain an array under `items` with each item
containing fields like `name`, `crawl_url`, `num_ingredients`,
`image` and more.

If you want more details about a specic recipe, you can `GET`
at `/recipe/{uuid}`

### Pagination

You should have noticed a `next` field in the output of our
previous search. Should look like base64-encoded gibberish.

If you submit the same search, but with an extra `after` key
with the value you got from `next`, you get (surprise!) the
next results:

```bash
curl -d'{ "fulltext": "bacon", "after": "AAAAAABAy6c0cM0Rb7VSU3OJkjB7_hHxeA" }' -H "${CT}" ${API}/search
```

Notice that the result contains a `next` field again? So long
as a result contains a `next` you can keep using it as `after`
to paginate through a result set of any size.


### Querying Features

You can find out about recipe features we know by querying the
`/info` endpoint:

```bash
curl $API/info
```

Here's a commented example of what you would see by looking
at the output under `features.num_ingredients`:

```javascript
{
  // Lowest number of ingredients (at least) one indexed recipe has
  "min": 2,
  // Ditto, but highest
  "max": 93,
  // Number of recipes in the index with the "num_ingredients" feature
  "count": 1183461,
}
```

You can sort by any feature that doesn't start with "diet_" via `sort`
and change the order to ascending via `ascending` (defaults to `false`):

```bash
curl -d'{ "sort": "num_ingredients", "ascending": true }' -H "${CT}" ${API}/search
```

And you can query for any feature and value ranges you want. Recipes
with calories within the `[100,350[` range:

```bash
curl -d'{ "filter": { "calories": [100, 350] } }' -H "${CT}" ${API}/search
```

Maybe you'd like to see a more detailed breakdown of a feature:

```bash
curl -d'{ "fulltext": "cheese bacon", "agg": { "total_time": [ [0, 15], [15, 60], [60, 240] ] } }' -H "${CT}" ${API}/search
```

Of course, you can filter and aggregate as many features/ranges as
you want.

**NOTE**: For performance reasons, the `agg` field is omitted from
the result if too many recipes are found. Adding more filters
and words to your query always help reducing the number of results.


## Notes

This is mostly an exercise in learning rust, so if you are looking for
well-thought-out things you won't have much luck. The code here is
organized as a cargo workspace where the business logic and server
code are placed inside the `cantine` crate and isolated functionality
such as cursor-based pagination and query/aggregation-related code
generation is implemented in `tique`.

I plan on exploring the whole ecosystem so documentation will come
someday, but for now here's a brief outline of the modules:

* `tique::top_collector`: A `tantivy` group of top collectors that
  allows checking for a condition before collecting a matching
  document, particularly useful in streaming and highly dynamic
  filtering contexts. Used in `cantine::index` as a way to navigate
  results without offsets/pages, sort by fields and change ordering.

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

* `cantine::index`: What actually drives the recipe index, doing all
  the custom pagination and sorting logic. It's where most of the
  code from `tique` gets used.

## Instructions

You can use the sample data to run a tiny version of the API:

```bash
cargo run --bin load /tmp/cantine < cantine/tests/sample_recipes.jsonlines
RUST_LOG=debug cargo run /tmp/cantine
```
