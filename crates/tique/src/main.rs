use actix_web::{web, App, HttpResponse, HttpServer, Responder, Result};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

struct AppState {
    counter: Arc<AtomicU16>,
}

#[derive(Serialize, Deserialize, Debug)]
struct SearchQuery {
    q: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct SearchResult {
    recipe_ids: Vec<i32>,
}

fn index(state: web::Data<AppState>) -> impl Responder {
    let cur = state.counter.fetch_add(1, Ordering::SeqCst) + 1;
    format!("Trololo {}!", cur)
}

fn search(query: web::Json<SearchQuery>) -> Result<HttpResponse> {
    println!("Searching: {:?}", query.0);
    Ok(HttpResponse::Ok().json(SearchResult {
        recipe_ids: vec![1, 2, 3, 4, 5],
    }))
}

fn main() -> std::result::Result<(), std::io::Error> {
    HttpServer::new(|| {
        App::new()
            .data(AppState {
                counter: Arc::new(AtomicU16::new(0)),
            })
            .route("/", web::get().to(index))
            .route("/search", web::post().to(search))
    })
    .bind("0.0.0.0:42000")?
    .run()
}
