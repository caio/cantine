use std::io;

mod mapped_file;
mod database;

pub use database::BytesDatabase;

pub type Result<T> = io::Result<T>;
