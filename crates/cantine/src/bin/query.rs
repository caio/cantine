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
    query::{AllQuery, BooleanQuery, Occur, Query},
    schema::{Field, Value},
    tokenizer::TokenizerManager,
    DocId, Index, IndexReader, Result, Score, SegmentLocalId, SegmentReader,
};

use cantine::{
    database::BincodeDatabase,
    index::IndexFields,
    model::{
        FeaturesAggregationQuery, FeaturesAggregationResult, FeaturesFilterQuery, Recipe,
        RecipeCard, SearchQuery, SearchResult,
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

pub struct Aggregator {
    query: FeaturesAggregationQuery,
    db: Arc<BincodeDatabase<Recipe>>,
    id_field: Field,
}

pub struct Aggregations {
    seen: u32,
    agg: FeaturesAggregationResult,
}

impl Aggregations {
    fn merge(&mut self, other: &Self) {
        self.seen += other.seen;
        self.agg.merge_same_size(&other.agg);
    }
}

impl Collector for Aggregator {
    type Fruit = Aggregations;
    type Child = AggregationsSegmentCollector;
    fn for_segment(
        &self,
        _segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> Result<Self::Child> {
        let aggregations = Aggregations {
            agg: FeaturesAggregationResult::from(&self.query),
            seen: 0,
        };

        let id_reader = reader
            .fast_fields()
            .u64(self.id_field)
            .expect("id_field is u64 fast field");

        Ok(AggregationsSegmentCollector {
            aggregations,
            id_reader,
            query: self.query.clone(),
            db: self.db.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        assert!(!fruits.is_empty());

        let mut iter = fruits.into_iter();
        let mut first = iter.next().expect("fruits is never empty");

        for fruit in iter {
            first.merge(&fruit);
        }

        Ok(first)
    }
}

pub struct AggregationsSegmentCollector {
    query: FeaturesAggregationQuery,
    aggregations: Aggregations,
    db: Arc<BincodeDatabase<Recipe>>,
    id_reader: FastFieldReader<u64>,
}

impl SegmentCollector for AggregationsSegmentCollector {
    type Fruit = Aggregations;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let id = self.id_reader.get(doc);
        let recipe = self.db.get_by_id(id).unwrap().unwrap();

        self.aggregations.seen += 1;
        self.aggregations.agg.collect(&self.query, &recipe.features);
    }

    fn harvest(self) -> Self::Fruit {
        self.aggregations
    }
}

pub struct Cantine {
    reader: IndexReader,
    fields: IndexFields,
    database: Arc<BincodeDatabase<Recipe>>,
    query_parser: QueryParser,
}

impl Cantine {
    pub fn open<P: AsRef<Path>>(base_path: P) -> Result<Self> {
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

    fn interpret_query(&self, query: &SearchQuery) -> Result<Box<dyn Query>> {
        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(fulltext) = &query.fulltext {
            if let Some(parsed) = self.query_parser.parse(fulltext.as_str())? {
                subqueries.push((Occur::Must, parsed));
            }
        }

        if let Some(filters) = &query.filters {
            for query in self.fields.features.interpret(filters).into_iter() {
                subqueries.push((Occur::Must, query));
            }
        }

        match subqueries.len() {
            0 => Ok(Box::new(AllQuery)),
            1 => Ok(subqueries.pop().expect("length has been checked").1),
            _ => Ok(Box::new(BooleanQuery::from(subqueries))),
        }
    }

    pub fn search(&self, query: &SearchQuery) -> Result<SearchResult> {
        let searcher = self.reader.searcher();

        let agg_query = if let Some(agg) = &query.agg {
            agg.clone()
        } else {
            FeaturesAggregationQuery::default()
        };

        let agg_collector = Aggregator {
            id_field: self.fields.id,
            db: self.database.clone(),
            query: agg_query.clone(),
        };

        let interpreted_query = self.interpret_query(query)?;
        // TODO sort

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
            total_found: agg_result.seen,
            agg: Some(agg_result.agg),
            ..SearchResult::default()
        })
    }
}

pub fn main() -> std::result::Result<(), String> {
    let options = QueryOptions::from_args();
    println!("Started with {:?}", options);

    let cantine = Cantine::open(options.base_path).unwrap();

    let query = SearchQuery {
        fulltext: Some(options.query),
        num_items: Some(options.num_results.get()),
        filters: Some(FeaturesFilterQuery {
            num_ingredients: Some(0..5),
            ..FeaturesFilterQuery::default()
        }),
        ..SearchQuery::default()
    };

    let result = cantine.search(&query).unwrap();

    dbg!(result);
    Ok(())
}
