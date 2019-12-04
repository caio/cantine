use std::{
    convert::TryFrom,
    io::{stdin, BufRead, BufReader},
    num::NonZeroU8,
    path::{Path, PathBuf},
    sync::Arc,
};

use bincode;
use serde_json;
use structopt::StructOpt;
use tantivy::{
    self,
    collector::{Collector, SegmentCollector},
    query::{AllQuery, BooleanQuery, Occur, Query},
    schema::Value,
    tokenizer::TokenizerManager,
    DocId, Index, IndexReader, Result, Score, SegmentLocalId, SegmentReader,
};

use cantine::{
    database::BincodeDatabase,
    index::IndexFields,
    model::{FeaturesCollector, Recipe, RecipeCard, SearchQuery, SearchResult, Sort},
};
use tique::{
    queryparser::QueryParser,
    top_collector::{ordered_by_u64_fast_field, ConditionalTopCollector},
};

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
    /// Only aggregate when found less recipes than given threshold
    #[structopt(short, long)]
    agg_threshold: Option<u32>,
}

pub struct CountCollector;

impl Collector for CountCollector {
    type Fruit = u32;
    type Child = CountSegmentCollector;

    fn for_segment(&self, _id: SegmentLocalId, _reader: &SegmentReader) -> Result<Self::Child> {
        Ok(CountSegmentCollector(0))
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> Result<Self::Fruit> {
        Ok(fruits.iter().sum())
    }
}

pub struct CountSegmentCollector(u32);

impl SegmentCollector for CountSegmentCollector {
    type Fruit = u32;

    fn collect(&mut self, _doc: DocId, _score: Score) {
        self.0 += 1;
    }

    fn harvest(self) -> Self::Fruit {
        self.0
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

        if let Some(filter) = &query.filter {
            for query in self.fields.features.interpret(filter).into_iter() {
                subqueries.push((Occur::Must, query));
            }
        }

        match subqueries.len() {
            0 => Ok(Box::new(AllQuery)),
            1 => Ok(subqueries.pop().expect("length has been checked").1),
            _ => Ok(Box::new(BooleanQuery::from(subqueries))),
        }
    }

    pub fn search(&self, query: &SearchQuery, agg_threshold: Option<u32>) -> Result<SearchResult> {
        let searcher = self.reader.searcher();

        let count_collector = CountCollector;

        let interpreted_query = self.interpret_query(query)?;
        let limit = query.num_items.unwrap_or(10) as usize;

        macro_rules! collect_unsigned {
            ($field:ident) => {{
                let top_collector =
                    ordered_by_u64_fast_field(self.fields.features.$field, limit, true);

                let (topdocs, total_found) =
                    searcher.search(&interpreted_query, &(top_collector, count_collector))?;

                let mut items: Vec<RecipeCard> = Vec::with_capacity(topdocs.len());
                for item in topdocs.iter() {
                    let doc = searcher.doc(item.doc).unwrap();
                    if let Some(&Value::U64(id)) = doc.get_first(self.fields.id) {
                        items.push(self.database.get_by_id(id).unwrap().unwrap().into());
                    } else {
                        panic!("Found document without a stored id");
                    }
                }

                (items, total_found)
            }};
        }

        let (items, total_found) = match query.sort.as_ref().unwrap_or(&Sort::Relevance) {
            Sort::Relevance => {
                let top_collector = ConditionalTopCollector::with_limit(limit, true);

                let (topdocs, total_found) =
                    searcher.search(&interpreted_query, &(top_collector, count_collector))?;

                let mut items: Vec<RecipeCard> = Vec::with_capacity(topdocs.len());
                for item in topdocs.iter() {
                    let doc = searcher.doc(item.doc).unwrap();
                    if let Some(&Value::U64(id)) = doc.get_first(self.fields.id) {
                        items.push(self.database.get_by_id(id).unwrap().unwrap().into());
                    } else {
                        panic!("Found document without a stored id");
                    }
                }

                (items, total_found)
            }
            Sort::Calories => collect_unsigned!(calories),
            Sort::NumIngredients => collect_unsigned!(num_ingredients),
            Sort::InstructionsLength => collect_unsigned!(instructions_length),
            Sort::TotalTime => collect_unsigned!(total_time),
            Sort::CookTime => collect_unsigned!(cook_time),
            Sort::PrepTime => collect_unsigned!(prep_time),
            _ => unimplemented!(),
        };

        let agg = if let Some(agg_query) = &query.agg {
            if total_found <= agg_threshold.unwrap_or(std::u32::MAX) {
                let features_field = self.fields.features_bincode;
                let collector =
                    FeaturesCollector::new(agg_query.clone(), move |reader: &SegmentReader| {
                        let features_reader = reader
                            .fast_fields()
                            .bytes(features_field)
                            .expect("bytes field is indexed");

                        move |doc: DocId| {
                            let buf = features_reader.get_bytes(doc);
                            bincode::deserialize(buf).unwrap()
                        }
                    });

                Some(searcher.search(&interpreted_query, &collector)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(SearchResult {
            items,
            total_found,
            agg,
            ..SearchResult::default()
        })
    }
}

pub fn main() -> std::result::Result<(), String> {
    let options = QueryOptions::from_args();
    let cantine = Cantine::open(options.base_path).unwrap();

    let stdin = stdin();
    let reader = BufReader::new(stdin.lock());

    for line in reader.lines() {
        let line = line.unwrap();
        let query = if let Ok(query) = serde_json::from_str(line.as_str()) {
            query
        } else {
            SearchQuery {
                fulltext: Some(line),
                num_items: Some(options.num_results.get()),
                ..SearchQuery::default()
            }
        };

        eprintln!("Executing query {:?}", &query);
        let result = cantine.search(&query, options.agg_threshold).unwrap();

        println!("{}", serde_json::to_string(&result).unwrap());
    }

    Ok(())
}
