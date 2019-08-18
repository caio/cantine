use std::{collections::HashMap, io, path::Path};

use crate::{
    database::BincodeDatabase,
    search::{Feature, FeatureCollector, FeatureIndexFields, QueryParser, SearchRequest},
    CerberusRecipeModel,
};

use clap::{value_t, ArgMatches};
use serde::{Deserialize, Serialize};
use serde_json;
use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery},
    schema::{
        Field, FieldType, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
        Value, FAST, INDEXED, STORED,
    },
    tokenizer::{
        Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, Tokenizer,
        TokenizerManager,
    },
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

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

    let json_query = matches.value_of("query").unwrap();
    let request: SearchRequest<Feature> = serde_json::from_str(json_query).unwrap();

    let db_path = base_path.join("database");
    let database = BincodeDatabase::new(&db_path).unwrap();

    let index_path = base_path.join("tantivy");
    let (schema, fields) = FeatureIndexFields::new(Feature::LENGTH);
    let index = Index::open_or_create(MmapDirectory::open(&index_path).unwrap(), schema).unwrap();

    let tokenizer = TokenizerManager::default()
        .get("en_stem")
        .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))
        .unwrap();

    let query_parser = QueryParser::new(fields.fulltext(), tokenizer);

    let iquery = fields.interpret_request(&request, &query_parser).unwrap();

    let reader = index.reader_builder().try_into().unwrap();
    let searcher = reader.searcher();

    let (hits, agg) = searcher
        .search(
            &iquery,
            &(
                TopDocs::with_limit(request.page_size.unwrap_or(10) as usize),
                FeatureCollector::for_field(
                    fields.feature_vector(),
                    Feature::LENGTH,
                    request.agg.unwrap_or(Vec::new()),
                ),
            ),
        )
        .unwrap();
    let mut found = Vec::new();

    for (_score, addr) in hits {
        let id = searcher
            .doc(addr)
            .unwrap()
            .get_first(fields.id())
            .expect("Found document without an id field")
            .u64_value();

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
        agg: agg.into(),
    };

    println!("{}", serde_json::to_string_pretty(&response).unwrap());

    Ok(())
}
