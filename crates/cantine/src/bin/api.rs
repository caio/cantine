use std::{
    io::Result as IoResult,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
};

use actix_web::{http::StatusCode, web, App, HttpResponse, HttpServer, Result};
use structopt::StructOpt;
use tantivy::{IndexReader, Result as TantivyResult, Searcher};
use uuid::Uuid;

use cantine::{
    database::{BincodeConfig, DatabaseReader},
    index::Cantine,
    model::{
        FeaturesAggregationResult, Recipe, RecipeId, RecipeInfo, SearchCursor, SearchQuery,
        SearchResult, Sort,
    },
};

#[derive(Debug, StructOpt)]
#[structopt(name = "api")]
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

type RecipeDatabase<'a> = Arc<DatabaseReader<'a, Recipe, BincodeConfig<Recipe>>>;

pub async fn recipe(
    database: web::Data<RecipeDatabase<'_>>,
    uuid: web::Path<Uuid>,
) -> Result<HttpResponse> {
    if let Some(recipe) = database.find_by_uuid(&uuid).expect("db operational") {
        Ok(HttpResponse::Ok().json(RecipeInfo::from(recipe)))
    } else {
        Ok(HttpResponse::new(StatusCode::NOT_FOUND))
    }
}

pub async fn search(
    search_query: web::Json<SearchQuery>,
    search_state: web::Data<SearchState>,
    database: web::Data<RecipeDatabase<'_>>,
) -> Result<HttpResponse> {
    let (total_found, recipe_ids, after, agg) =
        web::block(move || -> TantivyResult<ExecuteResult> {
            let searcher = search_state.reader.searcher();
            Ok(execute_search(
                &searcher,
                &search_state.cantine,
                search_query.0,
                search_state.threshold,
            )?)
        })
        .await?;

    let mut items = Vec::with_capacity(recipe_ids.len());
    for recipe_id in recipe_ids {
        let recipe: Recipe = database
            .find_by_id(recipe_id)
            .expect("db operational")
            .expect("item in the index always present in the db");
        items.push(recipe.into());
    }

    Ok(HttpResponse::Ok().json(SearchResult {
        total_found,
        items,
        after,
        agg,
    }))
}

type ExecuteResult = (
    usize,
    Vec<RecipeId>,
    Option<SearchCursor>,
    Option<FeaturesAggregationResult>,
);

fn execute_search(
    searcher: &Searcher,
    cantine: &Cantine,
    query: SearchQuery,
    agg_threshold: Option<usize>,
) -> TantivyResult<ExecuteResult> {
    let interpreted_query = cantine.interpret_query(&query)?;
    let limit = query.num_items.unwrap_or(10) as usize;

    let (total_found, recipe_ids, after) = cantine.search(
        &searcher,
        &interpreted_query,
        limit,
        query.sort.unwrap_or(Sort::Relevance),
        query.after.unwrap_or(SearchCursor::START),
    )?;

    let agg = if let Some(agg_query) = query.agg {
        if total_found <= agg_threshold.unwrap_or(std::usize::MAX) {
            Some(cantine.aggregate_features(&searcher, &interpreted_query, agg_query)?)
        } else {
            None
        }
    } else {
        None
    };

    Ok((total_found, recipe_ids, after, agg))
}

pub struct SearchState {
    cantine: Arc<Cantine>,
    reader: IndexReader,
    threshold: Option<usize>,
}

#[actix_rt::main]
async fn main() -> IoResult<()> {
    let options = ApiOptions::from_args();

    let cantine_path = options.base_path.join("tantivy");
    let db_path = options.base_path.join("database");

    let (index, cantine) = Cantine::open(&cantine_path).unwrap();
    let reader = index.reader().unwrap();
    let cantine = Arc::new(cantine);
    let threshold = options.agg_threshold;

    let database: RecipeDatabase = Arc::new(DatabaseReader::open(&db_path, BincodeConfig::new())?);

    HttpServer::new(move || {
        let search_state = SearchState {
            cantine: cantine.clone(),
            reader: reader.clone(),
            threshold,
        };

        App::new()
            .register_data(web::Data::new(database.clone()))
            .register_data(web::Data::new(search_state))
            .data(web::JsonConfig::default().limit(4096))
            .service(web::resource("/recipe/{uuid}").route(web::get().to(recipe)))
            .service(web::resource("/search").route(web::post().to(search)))
    })
    .bind("127.0.0.1:8080")?
    .start()
    .await
}
