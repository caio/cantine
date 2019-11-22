use std::{
    convert::TryFrom,
    num::NonZeroU8,
    path::{Path, PathBuf},
    sync::Arc,
};

use structopt::StructOpt;
use tantivy::{
    self,
    collector::{Collector, SegmentCollector},
    fastfield::FastFieldReader,
    schema::{Field, Value},
    tokenizer::TokenizerManager,
    DocId, Index, IndexReader, Score, SegmentLocalId, SegmentReader,
};

use cantine::{
    database::BincodeDatabase,
    index::IndexFields,
    model::{
        FeaturesAggregationQuery, FeaturesAggregationResult, Recipe, RecipeCard, SearchQuery,
        SearchResult,
    },
};
use tique::queryparser::QueryParser;
use tique::top_collector::ConditionalTopCollector;

/// Queries data generated via `load`
#[derive(Debug, StructOpt)]
#[structopt(name = "query")]
pub struct QueryOptions {
    /// Maximum number of recipes to retrieve
    #[structopt(short, long, default_value = "3")]
    num_results: NonZeroU8,
    /// Path to the data directory that will be queries
    #[structopt(short, long)]
    base_path: PathBuf,

    /// What to search for
    query: String,
}

// XXX With some cerimony this can be made reusable:
//  1. A Feature reader trait to map <doc_id, segment_id> to a Feature
//  2. A Feature trait that points at the generated aggregation
//     query/result structs
pub struct Aggregator {
    query: FeaturesAggregationQuery,
    db: Arc<BincodeDatabase<Recipe>>,
    id_field: Field,
}

impl Collector for Aggregator {
    type Fruit = FeaturesAggregationResult;
    type Child = FeatureSegmentCollector;
    fn for_segment(
        &self,
        _segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let agg = FeaturesAggregationResult::from(&self.query);

        let id_reader = reader
            .fast_fields()
            .u64(self.id_field)
            .expect("id_field is u64 fast field");

        Ok(FeatureSegmentCollector {
            agg,
            id_reader,
            query: self.query.clone(),
            db: self.db.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        assert!(!fruits.is_empty());

        let mut iter = fruits.into_iter();
        let mut first: FeaturesAggregationResult = iter.next().expect("fruits is never empty");

        for fruit in iter {
            first.merge_same_size(&fruit);
        }

        Ok(first)
    }
}

pub struct FeatureSegmentCollector {
    query: FeaturesAggregationQuery,
    agg: FeaturesAggregationResult,
    db: Arc<BincodeDatabase<Recipe>>,
    id_reader: FastFieldReader<u64>,
}

impl SegmentCollector for FeatureSegmentCollector {
    type Fruit = FeaturesAggregationResult;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let id = self.id_reader.get(doc);
        let recipe = self.db.get_by_id(id).unwrap().unwrap();

        self.agg.collect(&self.query, &recipe.features);
    }

    fn harvest(self) -> Self::Fruit {
        self.agg
    }
}

pub struct Cantine {
    reader: IndexReader,
    fields: IndexFields,
    database: Arc<BincodeDatabase<Recipe>>,
    query_parser: QueryParser,
}

impl Cantine {
    pub fn open<P: AsRef<Path>>(base_path: P) -> tantivy::Result<Self> {
        let index = Index::open_in_dir(base_path.as_ref().join("tantivy"))?;

        let fields = IndexFields::try_from(&index.schema()).unwrap();
        let reader = index.reader()?;

        let query_parser = QueryParser::new(
            fields.fulltext,
            TokenizerManager::default().get("en_stem").unwrap(),
            true,
        );

        let database =
            Arc::new(BincodeDatabase::open(base_path.as_ref().join("database")).unwrap());

        Ok(Self {
            fields,
            reader,
            database,
            query_parser,
        })
    }

    pub fn search(&self, query: &SearchQuery) -> tantivy::Result<SearchResult> {
        let searcher = self.reader.searcher();

        let agg_query = if let Some(agg) = &query.agg {
            agg.clone()
        } else {
            FeaturesAggregationQuery::default()
        };

        // TODO total count
        let agg_collector = Aggregator {
            id_field: self.fields.id,
            db: self.database.clone(),
            query: agg_query.clone(),
        };

        let plain_query = if let Some(fulltext) = &query.fulltext {
            fulltext
        } else {
            ""
        };

        // FIXME interpret all
        let interpreted_query = self.query_parser.parse(plain_query).unwrap().unwrap();

        let (topdocs, agg_result) = searcher.search(
            &interpreted_query,
            &(
                ConditionalTopCollector::with_limit(query.num_items.unwrap_or(10) as usize, true),
                agg_collector,
            ),
        )?;

        let mut items: Vec<RecipeCard> = Vec::with_capacity(topdocs.len());
        for item in topdocs.iter() {
            let doc = searcher.doc(item.doc).unwrap();
            if let Some(&Value::U64(id)) = doc.get_first(self.fields.id) {
                items.push(self.database.get_by_id(id).unwrap().unwrap().into());
            } else {
                panic!("Found document without a stored id");
            }
        }

        Ok(SearchResult {
            items,
            agg: Some(agg_result),
            ..SearchResult::default()
        })
    }
}

pub fn main() -> Result<(), String> {
    let options = QueryOptions::from_args();
    println!("Started with {:?}", options);

    let cantine = Cantine::open(options.base_path).unwrap();

    let query = SearchQuery {
        fulltext: Some(options.query),
        num_items: Some(options.num_results.get()),
        ..SearchQuery::default()
    };

    let result = cantine.search(&query).unwrap();

    dbg!(result);
    Ok(())
}
