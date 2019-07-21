use std::io;

mod mapped_file;
mod recipe_database;
#[allow(dead_code, unused)]
mod recipe_generated;

pub use recipe_database::RecipeDatabase;

pub type Result<T> = io::Result<T>;
