use std::{collections::HashMap, io, path::Path};

use crate::{
    database::BincodeDatabase,
    search::{FeatureIndexFields, QueryParser, SearchRequest},
    CerberusRecipeModel, Feature,
};

use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use serde_json;
use tantivy::{directory::MmapDirectory, tokenizer::TokenizerManager, Index};

#[derive(Serialize, Deserialize)]
struct ResultRecipe {
    id: u64,
    name: String,
    info_url: String,
    features: HashMap<String, u16>,
}

#[derive(Serialize, Deserialize)]
struct SearchResponse {
    hits: Vec<ResultRecipe>,
    num_hits: usize,
    page: usize,
    num_pages: usize,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    agg: HashMap<Feature, Vec<u16>>,
}

pub fn query(matches: &ArgMatches) -> io::Result<()> {
    let base_path = Path::new(matches.value_of("base_dir").unwrap());

    // TODO read lines from stdin instead
    let json_query = matches.value_of("query").unwrap();
    let request: SearchRequest = serde_json::from_str(json_query).unwrap();

    let db_path = base_path.join("database");
    let database = BincodeDatabase::new(&db_path).unwrap();

    let index_path = base_path.join("tantivy");
    let (index, fields) =
        FeatureIndexFields::open_or_create(Feature::LENGTH, Some(index_path), None).unwrap();

    let tokenizer = TokenizerManager::default()
        .get("en_stem")
        .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))
        .unwrap();

    let query_parser = QueryParser::new(fields.fulltext(), tokenizer);
    let reader = index.reader_builder().try_into().unwrap();
    let searcher = reader.searcher();

    // TODO change the search request to u16-only too
    let mut wanted = Vec::new();
    if let Some(agg) = &request.agg {
        for (feat, ranges) in agg {
            wanted.push((*feat as u16, ranges));
        }
    }

    let (ids, _fixme) = fields.search(&request, &query_parser, &searcher).unwrap();
    let mut found = Vec::new();

    for id in ids {
        let recipe: CerberusRecipeModel =
            database.get(id)?.expect("Found recipe should exist on db");

        found.push(ResultRecipe {
            // TODO all straight from db
            id: recipe.recipe_id,
            name: recipe.name,
            info_url: recipe.crawl_url,
            features: HashMap::new(),
        });
    }

    let response = SearchResponse {
        hits: found,
        // FIXME
        num_hits: 0,
        page: 1,
        // FIXME
        num_pages: 0,
        // FIXME agg: agg.into(),
        agg: HashMap::new(),
    };

    println!("{}", serde_json::to_string_pretty(&response).unwrap());

    Ok(())
}
