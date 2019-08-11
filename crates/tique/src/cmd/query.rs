use std::io;
use std::path::Path;

use super::{
    database::BincodeDatabase,
    search::{Feature, FeatureIndexFields, QueryParser, SearchQuery},
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
    tokenizer::TokenizerManager,
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

pub fn query(matches: &ArgMatches) -> io::Result<()> {
    let fulltext = Some(matches.value_of("query").unwrap().to_owned());
    let base_path = Path::new(matches.value_of("base_dir").unwrap());

    let db_path = base_path.join("database");
    let database = BincodeDatabase::new(&db_path).unwrap();

    let index_path = base_path.join("tantivy");
    let (schema, fields) = FeatureIndexFields::new();
    let index = Index::open_or_create(MmapDirectory::open(&index_path).unwrap(), schema).unwrap();

    let tokenizer = TokenizerManager::default()
        .get("en_stem")
        .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))
        .unwrap();

    let query_parser = QueryParser::new(fields.fulltext(), tokenizer);

    let sq = SearchQuery {
        fulltext,
        ..Default::default()
    };

    let iquery = fields.interpret_query(&sq, &query_parser).unwrap();

    let reader = index.reader_builder().try_into().unwrap();
    let searcher = reader.searcher();

    let hits = searcher.search(&iquery, &TopDocs::with_limit(10)).unwrap();
    let mut ids = Vec::with_capacity(hits.len());

    for (_score, addr) in hits {
        ids.push(
            searcher
                .doc(addr)
                .unwrap()
                .get_first(fields.id())
                .expect("Found document without an id field")
                .u64_value(),
        );
    }

    for (i, id) in ids.iter().enumerate() {
        let recipe: CerberusRecipeModel =
            database.get(*id)?.expect("Found recipe should exist on db");
        println!("{}. {}: {}", i, recipe.recipe_id, recipe.name);
    }

    Ok(())
}
