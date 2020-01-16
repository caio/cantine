# Cantine

A cooking recipe search JSON API with over a million recipes.

## Walkthrough

The API is publicly accessible at `https://caio.co/recipes/api/v0`.

You can search via `POST` on `/search`:

```bash
curl -H "Content-Type: application/json" -d'{ "fulltext": "bacon" }' https://caio.co/recipes/api/v0/search
```

The output will contain an array under `items` with each item
containing fields like `name`, `crawl_url`, `num_ingredients`,
`image` and more.

If you want more details about a specific recipe, you can `GET`
at `/recipe/{uuid}`.

There's one more useful endpoint you can `GET`: `/info`.  We'll
refer to it in more detail later, but it basically describes
some of the features we support.

Now, to make things easier to read we'll create a simple function
in bash:

```bash
export API=https://caio.co/recipes/api/v0
function search() { curl -XPOST "$API/search" -H "Content-Type: application/json" -d"$1"; echo; }
```

So we can do a useful search for recipes with bacon, the
phrase "deep fry" and *without* eggs:

```bash
search '{ "fulltext": "bacon -egg \"deep fry\"" }'
```

### Pagination

You should have noticed a `next` field in the output of our
previous search. Should look like base64-encoded gibberish.

If you submit the same search, but with an extra `after` key
with the value you got from `next`, you get (surprise!) the
next results:

```bash
search '{ "fulltext": "bacon", "after": "AAAAAABAy6c0cM0Rb7VSU3OJkjB7_hHxeA" }'
```

Notice that the result contains a `next` field again? So long
as a result contains a `next` you can keep using it as `after`
to paginate through a result set of any size.

### Sorting

From the `/info` endpoint you can learn all the valid sort
options.  Currently the default is "relevance", you can sort by
every feature sans diet-related ones and you can change the order
to ascending.

```bash
search '{ "sort": "num_ingredients_asc" }'
```

### Querying Features

From the `/info` endpoint we can also learn about the features we
know about each recipe.

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

#### Filtering

You can query for any feature and value ranges you want. Recipes
with calories within the `[100,350[` range:

```bash
search '{ "fulltext": "picanha", "filter": { "calories": [100, 350] } }'
```

#### Aggregating

You can get a breakdown of any/every feature for arbitrary (half-open)
ranges.

Maybe you'd like to see a more detailed counts of a search by
total time:

```bash
search '{ "fulltext": "cheese bacon", "agg": { "total_time": [ [0, 15], [15, 60], [60, 240] ] } }'
```

The output will contain a new `agg` field, that looks something
like this:

```json
{
  "agg": {
    "total_time": [
      {
        "min": 0,
        "max": 14,
        "count": 3158
      },
      {
        "min": 15,
        "max": 58,
        "count": 8982
      },
      {
        "min": 60,
        "max": 225,
        "count": 1594
      }
    ]
}
```

Which is, in order, the breakdown of each of the ranges we
requested in the search. So if we add a new filter for `[15,60]`
to the search we should expect `8982` matching recipes:

```
search '{ "fulltext": "cheese bacon", "filter": { "total_time": [15, 60] } }'
```

Of course, you can filter and aggregate as many features/ranges as
you want.

**NOTE**: For performance reasons, the `agg` field is omitted from
the result if too many recipes are found (300k currently).

## "Documentation"

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
