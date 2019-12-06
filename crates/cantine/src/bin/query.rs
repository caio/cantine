use std::{
    io::{stdin, BufRead, BufReader},
    path::PathBuf,
};

use serde_json;
use structopt::StructOpt;
use tantivy::Result;

use cantine::{
    database::BincodeDatabase,
    index::Cantine,
    model::{Recipe, SearchResult},
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

pub fn main() -> Result<()> {
    let options = QueryOptions::from_args();

    let cantine = Cantine::open(options.base_path.join("tantivy"))?;
    let database = BincodeDatabase::open(options.base_path.join("database")).unwrap();

    let stdin = stdin();
    let reader = BufReader::new(stdin.lock());

    for line in reader.lines() {
        let line = line.unwrap();
        let query = serde_json::from_str(line.as_str()).expect("valid SearchQuery json");

        eprintln!("Executing query {:?}", &query);
        let (total_found, recipe_ids, after, agg) = cantine.search(query, options.agg_threshold)?;

        let mut items = Vec::new();
        for recipe_id in recipe_ids {
            let recipe: Recipe = database
                .get_by_id(recipe_id)
                .expect("db operational")
                .expect("item in the index always present in the db");
            items.push(recipe.into());
        }

        let result = SearchResult {
            total_found,
            items,
            agg,
            after,
        };

        println!("{}", serde_json::to_string(&result).unwrap());
    }

    Ok(())
}
