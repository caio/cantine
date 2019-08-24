use std::io;

mod bincodedb;
mod mapped_file;

pub use bincodedb::BincodeDatabase;

pub type Result<T> = io::Result<T>;
