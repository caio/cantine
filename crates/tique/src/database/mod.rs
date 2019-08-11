use std::io;

mod database;
mod mapped_file;

pub use database::BincodeDatabase;

pub type Result<T> = io::Result<T>;
