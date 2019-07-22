use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;

use actix_web::{web, App, HttpServer, Responder};

mod database;
mod search;

struct AppState {
    counter: Arc<AtomicU16>,
}

fn index_(state: web::Data<AppState>) -> impl Responder {
    let cur = state.counter.fetch_add(1, Ordering::Relaxed) + 1; // Monotonic
    format!("Trololo {}!", cur)
}

fn main() -> Result<(), std::io::Error> {
    HttpServer::new(|| {
        App::new()
            .data(AppState {
                counter: Arc::new(AtomicU16::new(0)),
            })
            .route("/", web::get().to(index_))
    })
    .bind("0.0.0.0:42000")?
    .run()?;
    Ok(())
}
