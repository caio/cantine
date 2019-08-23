use std::{collections::HashMap, io, path::Path};

use crate::{
    database::BincodeDatabase,
    search::{FeatureIndexFields, QueryParser, SearchRequest},
    CerberusRecipeModel, Feature,
};

use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use serde_json;
use tantivy::tokenizer::TokenizerManager;

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
    // XXX Ideally I would be able to deserialize mapping
    //     from Feature to usize already
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

    let (ids, agg) = fields.search(&request, &query_parser, &searcher).unwrap();
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

    // XXX Can I have a renderer or smth to make this cheaper?
    //     Like... custom serializer maybe?
    let mut cute_agg: HashMap<Feature, Vec<u16>> = HashMap::new();
    for feat in Feature::VALUES.iter() {
        let idx = *feat as usize;
        if let Some(counts) = &agg[idx] {
            // i.e.: I want to avoid this clone(). Cow maybe?
            cute_agg.insert(*feat, counts.clone());
        }
    }

    let response = SearchResponse {
        hits: found,
        num_hits: 0,
        page: 1,
        num_pages: 0,
        agg: cute_agg,
    };

    println!("{}", serde_json::to_string_pretty(&response).unwrap());

    Ok(())
}
