use std::io;

mod mapped_file;
mod recipe_database;

pub use recipe_database::RecipeDatabase;

pub type Result<T> = io::Result<T>;
