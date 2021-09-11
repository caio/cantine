use std::{convert::TryFrom, env, io, path::Path, str::FromStr, sync::Arc};

use serde::Serialize;
use tique::QueryParser;
use uuid::Uuid;

use actix_web::{
    http::StatusCode, middleware::Logger, web, App, HttpResponse, HttpServer, Result as ActixResult,
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
    pub sort: Vec<Sort>,
}

pub async fn index_info(info: web::Data<IndexInfo>) -> ActixResult<HttpResponse> {
    Ok(HttpResponse::Ok().json(info.get_ref()))
}

fn cursor_to_after(database: &RecipeDatabase, cursor: &SearchCursor) -> Option<After> {
    database
        .id_for_uuid(&Uuid::from_bytes(*cursor.uuid()))
        .map(|id| match &cursor {
            SearchCursor::Relevance(score, _) => After::Relevance(*score, *id),
            SearchCursor::U64Field(score, _) => After::U64Field(*score, *id),
            SearchCursor::F64Field(score, _) => After::F64Field(*score, *id),
        })
}

pub async fn search(
    query: web::Json<SearchQuery>,
    state: web::Data<Arc<SearchState>>,
    database: web::Data<RecipeDatabase>,
) -> ActixResult<HttpResponse> {
    let after = if let Some(cursor) = &query.after {
        let checked_after = cursor_to_after(&database, &cursor);
        if checked_after.is_none() {
            return Ok(HttpResponse::new(StatusCode::BAD_REQUEST));
        }
        checked_after
    } else {
        None
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

    let next = after.map(|after| {
        let last_uuid = &items[num_results - 1].uuid;

        match after {
            After::Relevance(score, _) => SearchCursor::Relevance(score, *last_uuid.as_bytes()),
            After::U64Field(score, _) => SearchCursor::U64Field(score, *last_uuid.as_bytes()),
            After::F64Field(score, _) => SearchCursor::F64Field(score, *last_uuid.as_bytes()),
        }
    });

    Ok(HttpResponse::Ok().json(SearchResult {
        items,
        total_found,
        agg,
        next,
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
    pub fn search(&self, query: SearchQuery, after: Option<After>) -> Result<ExecuteResult> {
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
            if let Some(parsed) = self.query_parser.parse_dixmax(fulltext.as_str(), 0.1) {
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

        let sort = Sort::VALUES.to_vec();

        Ok(IndexInfo {
            total_recipes: searcher.num_docs(),
            features,
            sort,
        })
    }
}

const BASE_DIR: &str = "BASE_DIR";
const AGG_THRESHOLD: &str = "AGG_THRESHOLD";

fn get_env(key: &str) -> Result<String> {
    env::var(key).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, key).into())
}

#[actix_rt::main]
async fn main() -> Result<()> {
    env_logger::init();

    let base_dir = get_env(BASE_DIR)?;
    let threshold = get_env(AGG_THRESHOLD)
        .ok()
        .map(|v| usize::from_str(&v).expect("valid usize"));

    log::info!(
        "Starting with base_dir={} agg_threshold={:?}",
        base_dir,
        threshold
    );

    let base_path = Path::new(&base_dir);
    let index_path = base_path.join("tantivy");
    let db_path = base_path.join("database");

    let index = Index::open_in_dir(&index_path)?;
    let recipe_index = RecipeIndex::try_from(&index.schema())?;
    let mut query_parser = QueryParser::new(
        &index,
        vec![
            recipe_index.name,
            recipe_index.ingredients,
            recipe_index.instructions,
        ],
    )?;

    // XXX This is as scientific as "4" is random
    // Reduce importance of instructions match
    query_parser.set_boost(recipe_index.instructions, Some(0.7));
    // And make name matches slightly more important than ingredient
    query_parser.set_boost(recipe_index.name, Some(1.15));

    let reader = index.reader()?;
    let search_state = Arc::new(SearchState {
        reader,
        recipe_index,
        query_parser,
        agg_threshold: threshold.unwrap_or(std::usize::MAX),
    });

    let database: RecipeDatabase = Arc::new(DatabaseReader::open(&db_path)?);

    let info = search_state.index_info()?;

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
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
