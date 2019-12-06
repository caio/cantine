use std::{
    io::{stdin, BufRead, BufReader},
    path::PathBuf,
};

use serde_json;
use structopt::StructOpt;
use tantivy::{Result, Searcher};

use cantine::{
    database::BincodeDatabase,
    index::Cantine,
    model::{Recipe, SearchCursor, SearchQuery, SearchResult, Sort},
};

/// Queries data generated via `load`
#[derive(Debug, StructOpt)]
#[structopt(name = "query")]
pub struct QueryOptions {
    /// Path to the data directory that will be queries
    #[structopt(short, long)]
    base_path: PathBuf,
    /// Only aggregate when found less recipes than given threshold
    #[structopt(short, long)]
    agg_threshold: Option<usize>,
}

fn search(
    database: &BincodeDatabase<Recipe>,
    searcher: &Searcher,
    cantine: &Cantine,
    query: SearchQuery,
    agg_threshold: Option<usize>,
) -> Result<SearchResult> {
    let interpreted_query = cantine.interpret_query(&query)?;
    let limit = query.num_items.unwrap_or(10) as usize;

    let (total_found, recipe_ids, after) = cantine.search(
        &searcher,
        &interpreted_query,
        limit,
        query.sort.unwrap_or(Sort::Relevance),
        query.after.unwrap_or(SearchCursor::START),
    )?;

    let agg = if let Some(agg_query) = query.agg {
        if total_found <= agg_threshold.unwrap_or(std::usize::MAX) {
            Some(cantine.aggregate_features(&searcher, &interpreted_query, agg_query)?)
        } else {
            None
        }
    } else {
        None
    };

    let mut items = Vec::with_capacity(recipe_ids.len());
    for recipe_id in recipe_ids {
        let recipe: Recipe = database
            .get_by_id(recipe_id)
            .expect("db operational")
            .expect("item in the index always present in the db");
        items.push(recipe.into());
    }

    Ok(SearchResult {
        total_found,
        items,
        agg,
        after,
    })
}

pub fn main() -> Result<()> {
    let options = QueryOptions::from_args();

    let (index, cantine) = Cantine::open(options.base_path.join("tantivy"))?;
    let database = BincodeDatabase::open(options.base_path.join("database")).unwrap();

    let stdin = stdin();
    let reader = BufReader::new(stdin.lock());

    let index_reader = index.reader()?;
    let searcher = index_reader.searcher();

    for line in reader.lines() {
        let line = line.unwrap();
        let query = serde_json::from_str(line.as_str()).expect("valid SearchQuery json");

        eprintln!("Executing query {:?}", &query);
        let result = search(&database, &searcher, &cantine, query, options.agg_threshold)?;

        println!("{}", serde_json::to_string(&result).unwrap());
    }

    Ok(())
}
