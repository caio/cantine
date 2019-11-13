use std::{convert::TryFrom, num::NonZeroUsize, path::PathBuf};

use structopt::StructOpt;
use tantivy::{self, schema::Value, tokenizer::TokenizerManager, Index};

use cantine::{
    database::BincodeDatabase,
    index::IndexFields,
    model::{Recipe, RecipeCard},
};
use tique::queryparser::QueryParser;
use tique::top_collector::ConditionalTopCollector;

/// Queries data generated via `load`
#[derive(Debug, StructOpt)]
#[structopt(name = "query")]
pub struct QueryOptions {
    /// Maximum number of recipes to retrieve
    #[structopt(short, long, default_value = "3")]
    num_results: NonZeroUsize,
    /// Path to the data directory that will be queries
    #[structopt(short, long)]
    base_path: PathBuf,

    /// What to search for
    query: String,
}

pub fn main() -> Result<(), String> {
    let options = QueryOptions::from_args();
    println!("Started with {:?}", options);

    let index = Index::open_in_dir(options.base_path.join("tantivy")).unwrap();
    let fields = IndexFields::try_from(&index.schema())?;

    let parser = QueryParser::new(
        fields.fulltext,
        TokenizerManager::default().get("en_stem").unwrap(),
        true,
    );

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let topdocs = searcher
        .search(
            // FIXME errors
            &parser.parse(&options.query.as_str()).unwrap().unwrap(),
            &ConditionalTopCollector::with_limit(options.num_results.get(), true),
        )
        .unwrap();

    let db: BincodeDatabase<Recipe> =
        BincodeDatabase::open(options.base_path.join("database")).unwrap();

    let mut recipes: Vec<RecipeCard> = Vec::new();
    for item in topdocs.iter() {
        let doc = searcher.doc(item.doc).unwrap();
        if let Some(&Value::U64(id)) = doc.get_first(fields.id) {
            recipes.push(db.get_by_id(id).unwrap().unwrap().into());
        } else {
            return Err(format!("Found doc without id: {:?}", doc));
        }
    }

    dbg!(recipes);

    Ok(())
}
