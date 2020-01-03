use std::{
    convert::TryFrom,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
};

use env_logger;
use serde::Serialize;
use structopt::StructOpt;
use tique::queryparser::QueryParser;
use uuid::Uuid;

use actix_web::{
    http::StatusCode, middleware, web, App, HttpResponse, HttpServer, Result as ActixResult,
};

use tantivy::{
    query::{AllQuery, BooleanQuery, Occur, Query},
    Index, IndexReader, Result,
};

use cantine::{
    database::DatabaseReader,
    index::{After, RecipeIndex},
    model::{
        FeaturesAggregationQuery, FeaturesAggregationResult, Recipe, RecipeCard, RecipeId,
        RecipeInfo, SearchCursor, SearchQuery, SearchResult, Sort,
    },
};

#[derive(Debug, StructOpt)]
pub struct ApiOptions {
    /// Path to the data directory
    #[structopt(validator = is_dir)]
    base_path: PathBuf,
    /// Only aggregate when found less recipes than given threshold
    #[structopt(short, long)]
    agg_threshold: Option<usize>,
}

fn is_dir(dir_path: String) -> StdResult<(), String> {
    if Path::new(dir_path.as_str()).is_dir() {
        Ok(())
    } else {
        Err("Not a directory".to_owned())
    }
}

type RecipeDatabase = Arc<DatabaseReader<Recipe>>;

pub async fn recipe(
    database: web::Data<RecipeDatabase>,
    uuid: web::Path<Uuid>,
) -> ActixResult<HttpResponse> {
    if let Some(recipe) = database
        .find_by_uuid(&uuid)
        .transpose()
        .expect("db operational")
    {
        Ok(HttpResponse::Ok().json(RecipeInfo::from(recipe)))
    } else {
        Ok(HttpResponse::new(StatusCode::NOT_FOUND))
    }
}

#[derive(Serialize, Clone)]
pub struct IndexInfo {
    pub total_recipes: u64,
    pub features: FeaturesAggregationResult,
}

pub async fn index_info(info: web::Data<IndexInfo>) -> ActixResult<HttpResponse> {
    Ok(HttpResponse::Ok().json(info.get_ref()))
}

pub async fn search(
    query: web::Json<SearchQuery>,
    state: web::Data<Arc<SearchState>>,
    database: web::Data<RecipeDatabase>,
) -> ActixResult<HttpResponse> {
    let after = match &query.after {
        None => After::START,
        Some(cursor) => {
            if let Some(recipe_id) = database.id_for_uuid(&Uuid::from_bytes(cursor.1)) {
                After::new(cursor.0, *recipe_id)
            } else {
                return Ok(HttpResponse::new(StatusCode::BAD_REQUEST));
            }
        }
    };

    let (total_found, recipe_ids, after, agg) =
        web::block(move || -> Result<ExecuteResult> { state.search(query.0, after) }).await?;

    let num_results = recipe_ids.len();
    let mut items = Vec::with_capacity(num_results);
    for recipe_id in recipe_ids {
        let recipe: Recipe = database
            .find_by_id(recipe_id)
            .expect("item in the index always present in the db")?;
        items.push(RecipeCard::from(recipe));
    }

    let next = after.map(|cursor| {
        let last = &items[num_results - 1];
        SearchCursor::new(cursor.score(), &last.uuid)
    });

    Ok(HttpResponse::Ok().json(SearchResult {
        total_found,
        items,
        next,
        agg,
    }))
}

type ExecuteResult = (
    usize,
    Vec<RecipeId>,
    Option<After>,
    Option<FeaturesAggregationResult>,
);

pub struct SearchState {
    reader: IndexReader,
    recipe_index: RecipeIndex,
    query_parser: QueryParser,
    agg_threshold: usize,
}

impl SearchState {
    pub fn search(&self, query: SearchQuery, after: After) -> Result<ExecuteResult> {
        let limit = query.num_items.unwrap_or(10) as usize;

        let searcher = self.reader.searcher();
        let interpreted_query = self.interpret_query(&query)?;

        let (total_found, recipe_ids, after) = self.recipe_index.search(
            &searcher,
            &interpreted_query,
            limit,
            query.sort.unwrap_or(Sort::Relevance),
            after,
        )?;

        let agg = if total_found <= self.agg_threshold {
            query
                .agg
                .map(|agg_query| {
                    self.recipe_index
                        .aggregate_features(&searcher, &interpreted_query, agg_query)
                })
                .transpose()?
        } else {
            None
        };

        Ok((total_found, recipe_ids, after, agg))
    }

    fn interpret_query(&self, query: &SearchQuery) -> Result<Box<dyn Query>> {
        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(fulltext) = &query.fulltext {
            if let Some(parsed) = self.query_parser.parse(fulltext.as_str())? {
                subqueries.push((Occur::Must, parsed));
            }
        }

        if let Some(filter) = &query.filter {
            for query in self.recipe_index.features.interpret(filter).into_iter() {
                subqueries.push((Occur::Must, query));
            }
        }

        match subqueries.len() {
            0 => Ok(Box::new(AllQuery)),
            1 => Ok(subqueries.pop().expect("length has been checked").1),
            _ => Ok(Box::new(BooleanQuery::from(subqueries))),
        }
    }

    pub fn index_info(&self) -> Result<IndexInfo> {
        let searcher = self.reader.searcher();
        let features = self.recipe_index.aggregate_features(
            &searcher,
            &AllQuery,
            FeaturesAggregationQuery::full_range(),
        )?;

        Ok(IndexInfo {
            total_recipes: searcher.num_docs(),
            features,
        })
    }
}

#[actix_rt::main]
async fn main() -> Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let options = ApiOptions::from_args();

    let index_path = options.base_path.join("tantivy");
    let db_path = options.base_path.join("database");

    let index = Index::open_in_dir(&index_path)?;
    let recipe_index = RecipeIndex::try_from(&index.schema())?;
    let query_parser = QueryParser::new(
        recipe_index.fulltext,
        index.tokenizer_for_field(recipe_index.fulltext)?,
        true,
    );

    let agg_threshold = options.agg_threshold.unwrap_or(std::usize::MAX);
    let reader = index.reader()?;
    let search_state = Arc::new(SearchState {
        reader,
        recipe_index,
        query_parser,
        agg_threshold,
    });

    let database: RecipeDatabase = Arc::new(DatabaseReader::open(&db_path)?);

    let info = search_state.index_info()?;

    HttpServer::new(move || {
        App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::Data::new(search_state.clone()))
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(info.clone()))
            .data(web::JsonConfig::default().limit(4096))
            .service(web::resource("/recipe/{uuid}").route(web::get().to(recipe)))
            .service(web::resource("/search").route(web::post().to(search)))
            .service(web::resource("/info").route(web::get().to(index_info)))
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await?;

    Ok(())
}
