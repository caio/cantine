use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use actix_web::{error, web, App, HttpResponse, HttpServer, Responder, Result as ActixResult};

use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Document, Field, SchemaBuilder, STORED, TEXT};
use tantivy::ReloadPolicy;
use tantivy::{Index, IndexReader};

struct AppState {
    counter: Arc<AtomicU16>,
}

struct SearchState {
    index_reader: IndexReader,
    query_parser: QueryParser,
    id_field: Field,
}

#[derive(Serialize, Deserialize, Debug)]
struct SearchQuery {
    q: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SearchResult {
    recipe_ids: Vec<u64>,
}

fn index_(state: web::Data<AppState>) -> impl Responder {
    let cur = state.counter.fetch_add(1, Ordering::Relaxed) + 1; // Monotonic
    format!("Trololo {}!", cur)
}

fn search(
    state: web::Data<SearchState>,
    query: web::Json<SearchQuery>,
) -> ActixResult<HttpResponse> {
    println!("Searching: {:?}", query);

    let parsed_query = state
        .query_parser
        .parse_query(&*query.q)
        .map_err(|_e| error::ErrorBadRequest("failed to parse"))?;

    let searcher = state.index_reader.searcher();

    let found_ids = searcher
        .search(&parsed_query, &TopDocs::with_limit(10))
        .map_err(|_e| error::ErrorInternalServerError("failed to search"))?
        .iter()
        .map(|(_, addr)| {
            searcher
                .doc(*addr)
                .expect("search found doc that doesnt exist?")
                .get_first(state.id_field)
                .expect("id field empty?")
                .u64_value()
        })
        .collect();

    Ok(HttpResponse::Ok().json(SearchResult {
        recipe_ids: found_ids,
    }))
}

fn init_search() -> SearchState {
    let mut schema_builder = SchemaBuilder::default();

    let id = schema_builder.add_u64_field("id", STORED);
    let title = schema_builder.add_text_field("title", TEXT);

    let schema = schema_builder.build();
    let index = Index::create_in_ram(schema.clone());
    let mut writer = index.writer(5_000_000).unwrap();

    let make_doc = |i: u64, t: &str| -> Document {
        let mut doc = Document::new();

        doc.add_u64(id, i);
        doc.add_text(title, t);
        doc
    };

    writer.add_document(make_doc(1, "caio"));
    writer.add_document(make_doc(2, "caio romao"));
    writer.add_document(make_doc(3, "caio romao costa nascimento"));

    writer.commit().unwrap();

    SearchState {
        index_reader: index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()
            .unwrap(),
        query_parser: QueryParser::for_index(&index, vec![title]),
        id_field: id,
    }
}

fn main() -> Result<(), std::io::Error> {
    HttpServer::new(|| {
        App::new()
            .data(AppState {
                counter: Arc::new(AtomicU16::new(0)),
            })
            .data(init_search())
            .route("/", web::get().to(index_))
            .route("/search", web::post().to(search))
    })
    .bind("0.0.0.0:42000")?
    .run()
}
