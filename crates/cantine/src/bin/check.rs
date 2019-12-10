use std::{
    io::{stdin, BufRead, Result},
    num::NonZeroUsize,
    path::{Path, PathBuf},
    result::Result as StdResult,
    sync::Arc,
    thread::spawn,
    time::Instant,
};

use crossbeam_channel::unbounded;
use serde_json;
use structopt::StructOpt;

use cantine::database::{BincodeConfig, DatabaseReader};
use cantine::model::Recipe;

/// Checks data generated via `load` against a stream of recipes
#[derive(Debug, StructOpt)]
#[structopt(name = "check")]
pub struct CheckOptions {
    /// Number of worker threads to start
    #[structopt(short, long, default_value = "4")]
    num_checkers: NonZeroUsize,
    /// Path to the data directory that will be checked
    #[structopt(validator = is_dir)]
    base_path: PathBuf,
}

fn is_dir(dir_path: String) -> StdResult<(), String> {
    if Path::new(dir_path.as_str()).is_dir() {
        Ok(())
    } else {
        Err("Not a directory".to_owned())
    }
}

fn check(options: CheckOptions) -> Result<()> {
    println!("Started with {:?}", &options);
    let cur = Instant::now();

    let db_path = options.base_path.join("database");
    // Nothing to check at the tantivy index atm

    println!("Loading database");
    let database = DatabaseReader::open(db_path, BincodeConfig::<Recipe>::new())?;
    let db = Arc::new(database);

    let mut workers = Vec::new();
    let (line_sender, line_receiver) = unbounded::<String>();

    println!("Creating workers");
    for _ in 0..options.num_checkers.get() {
        let line_receiver = line_receiver.clone();

        let db = db.clone();
        workers.push(spawn(move || {
            for line in line_receiver {
                let recipe: Recipe =
                    serde_json::from_str(line.as_ref()).expect("valid recipe json");

                let db_recipe = db.find_by_uuid(&recipe.uuid).unwrap().unwrap();
                db.find_by_id(recipe.recipe_id).unwrap().unwrap();

                if recipe != db_recipe {
                    panic!(
                        "Recipe {} from stdin differs from the one in the db",
                        recipe.recipe_id
                    );
                }
            }
        }))
    }
    drop(line_receiver);

    println!("Checking against input from stdin");
    for line in stdin().lock().lines().filter_map(Result::ok) {
        line_sender.send(line).unwrap();
    }
    drop(line_sender);

    for worker in workers {
        worker.join().unwrap();
    }

    println!("Pass. Done in {} seconds", cur.elapsed().as_secs());

    Ok(())
}

fn main() -> Result<()> {
    check(CheckOptions::from_args())
}
