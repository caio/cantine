use std::io::{self, BufRead};
use std::path::Path;
use std::sync::mpsc::channel;
use std::thread::spawn;

use super::{
    database::{BincodeDatabase, Database},
    CerberusRecipeModel,
};

use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use serde_json;

pub fn check_database(matches: &ArgMatches) -> io::Result<()> {
    let db_path = Path::new(matches.value_of("database_dir").unwrap());

    // validator?
    if !db_path.join("log.bin").exists() {
        panic!("Not a db path");
    }

    // XXX maybe split into open/new ?
    let db = BincodeDatabase::new::<CerberusRecipeModel>(&db_path)?;

    let (line_sender, lines) = channel();
    spawn(move || {
        let stdin = io::stdin();

        for line in stdin.lock().lines() {
            line_sender.send(line.unwrap()).unwrap();
        }
    });

    let mut num_checked = 0;
    println!("Started!");
    for line in lines {
        let recipe: CerberusRecipeModel = serde_json::from_str(line.as_str()).unwrap();

        if let Some(db_recipe) = db.get(recipe.recipe_id)? {
            if db_recipe != recipe {
                panic!(
                    "Input recipe {} is different from the one in the db",
                    recipe.recipe_id
                );
            }
        } else {
            panic!("Couldn't find recipe {} in db", recipe.recipe_id);
        }
        num_checked += 1;

        if num_checked % 50_000 == 0 {
            println!("Checked {} recipes so far", num_checked);
        }
    }

    println!("All {} recipes are good!", num_checked);

    Ok(())
}
