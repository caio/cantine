use std::{
    convert::TryFrom,
    io::Result as IoResult,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
    time::Duration,
};

use actix_web::{http::StatusCode, middleware, web, App, HttpResponse, HttpServer, Result};
use env_logger;
use structopt::StructOpt;
use tokio::time::timeout;
use uuid::Uuid;

use tantivy::{
    query::{AllQuery, BooleanQuery, Occur, Query},
    Index, IndexReader, Result as TantivyResult, Searcher,
};

use cantine::{
    database::DatabaseReader,
    index::{After, RecipeIndex},
    model::{
        FeaturesAggregationResult, Recipe, RecipeCard, RecipeId, RecipeInfo, SearchCursor,
        SearchQuery, SearchResult, Sort,
    },
};

use tique::queryparser::QueryParser;

#[derive(Debug, StructOpt)]
#[structopt(name = "api")]
pub struct ApiOptions {
    /// Path to the data directory
    #[structopt(validator = is_dir)]
    base_path: PathBuf,
    /// Only aggregate when found less recipes than given threshold
    #[structopt(short, long)]
    agg_threshold: Option<usize>,
    /// Search execution timeout in ms
    #[structopt(short, long, default_value = "2000")]
    timeout: u64,
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
) -> Result<HttpResponse> {
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

pub async fn search(
    search_query: web::Json<SearchQuery>,
    search_state: web::Data<SearchState>,
    database: web::Data<RecipeDatabase>,
) -> Result<HttpResponse> {
    let after = match &search_query.after {
        None => After::START,
        Some(cursor) => {
            if let Some(recipe_id) = database.id_for_uuid(&Uuid::from_bytes(cursor.1)) {
                After::new(cursor.0, *recipe_id)
            } else {
                return Ok(HttpResponse::new(StatusCode::BAD_REQUEST));
            }
        }
    };

    let search_timeout = search_state.timeout;

    let search_future = web::block(move || -> TantivyResult<ExecuteResult> {
        let searcher = search_state.reader.searcher();
        Ok(execute_search(
            &searcher,
            &search_state.cantine,
            &search_state.query_parser,
            search_query.0,
            after,
            search_state.threshold,
        )?)
    });

    let (total_found, recipe_ids, after, agg) = {
        if let Ok(search_future_result) =
            timeout(Duration::from_millis(search_timeout), search_future).await
        {
            search_future_result?
        } else {
            return Ok(HttpResponse::new(StatusCode::GATEWAY_TIMEOUT));
        }
    };

    let num_results = recipe_ids.len();
    let mut items = Vec::with_capacity(num_results);
    for recipe_id in recipe_ids {
        let recipe: Recipe = database
            .find_by_id(recipe_id)
            .expect("db operational")
            .expect("item in the index always present in the db");
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

fn interpret_query(
    cantine: &RecipeIndex,
    query_parser: &QueryParser,
    query: &SearchQuery,
) -> TantivyResult<Box<dyn Query>> {
    let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

    if let Some(fulltext) = &query.fulltext {
        if let Some(parsed) = query_parser.parse(fulltext.as_str())? {
            subqueries.push((Occur::Must, parsed));
        }
    }

    if let Some(filter) = &query.filter {
        for query in cantine.features.interpret(filter).into_iter() {
            subqueries.push((Occur::Must, query));
        }
    }

    match subqueries.len() {
        0 => Ok(Box::new(AllQuery)),
        1 => Ok(subqueries.pop().expect("length has been checked").1),
        _ => Ok(Box::new(BooleanQuery::from(subqueries))),
    }
}

fn execute_search(
    searcher: &Searcher,
    cantine: &RecipeIndex,
    query_parser: &QueryParser,
    query: SearchQuery,
    after: After,
    agg_threshold: usize,
) -> TantivyResult<ExecuteResult> {
    let interpreted_query = interpret_query(&cantine, query_parser, &query)?;
    let limit = query.num_items.unwrap_or(10) as usize;

    let (total_found, recipe_ids, after) = cantine.search(
        &searcher,
        &interpreted_query,
        limit,
        query.sort.unwrap_or(Sort::Relevance),
        after,
    )?;

    let agg = if total_found <= agg_threshold {
        query
            .agg
            .map(|agg_query| cantine.aggregate_features(&searcher, &interpreted_query, agg_query))
            .transpose()?
    } else {
        None
    };

    Ok((total_found, recipe_ids, after, agg))
}

pub struct SearchState {
    cantine: Arc<RecipeIndex>,
    reader: IndexReader,
    query_parser: Arc<QueryParser>,
    threshold: usize,
    timeout: u64,
}

#[actix_rt::main]
async fn main() -> IoResult<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let options = ApiOptions::from_args();

    let cantine_path = options.base_path.join("tantivy");
    let db_path = options.base_path.join("database");

    let index = Index::open_in_dir(&cantine_path).unwrap();
    let cantine = RecipeIndex::try_from(&index.schema()).unwrap();
    let query_parser = Arc::new(QueryParser::new(
        cantine.fulltext,
        index.tokenizer_for_field(cantine.fulltext).unwrap(),
        true,
    ));

    let reader = index.reader().unwrap();
    let cantine = Arc::new(cantine);

    let threshold = options.agg_threshold.unwrap_or(std::usize::MAX);
    let timeout = options.timeout;

    let database: RecipeDatabase = Arc::new(DatabaseReader::open(&db_path)?);

    HttpServer::new(move || {
        let search_state = SearchState {
            cantine: cantine.clone(),
            reader: reader.clone(),
            query_parser: query_parser.clone(),
            threshold,
            timeout,
        };

        App::new()
            .wrap(middleware::Logger::default())
            .app_data(web::Data::new(database.clone()))
            .app_data(web::Data::new(search_state))
            .data(web::JsonConfig::default().limit(4096))
            .service(web::resource("/recipe/{uuid}").route(web::get().to(recipe)))
            .service(web::resource("/search").route(web::post().to(search)))
    })
    .bind("127.0.0.1:8080")?
    .start()
    .await
}
