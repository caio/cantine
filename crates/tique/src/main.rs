use std::collections::HashMap;
use std::io;
use std::path::Path;

mod cmd;
mod database;
mod search;

use clap::{App, Arg, SubCommand};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Hash, Eq, PartialEq, Clone, Copy)]
pub enum Feature {
    NumIngredients = 0,

    Calories,
    FatContent,
    ProteinContent,
    CarbContent,

    CookTime,
    PrepTime,
    TotalTime,

    DietKeto,
    DietLowCarb,
    DietVegan,
    DietVegetarian,
    DietPaleo,
}

impl Feature {
    pub const LENGTH: usize = 13;

    pub const EMPTY_BUFFER: [u8; Self::LENGTH * 2] = [std::u8::MAX; Self::LENGTH * 2];

    pub const VALUES: [Feature; Feature::LENGTH] = [
        Feature::NumIngredients,
        Feature::Calories,
        Feature::FatContent,
        Feature::ProteinContent,
        Feature::CarbContent,
        Feature::CookTime,
        Feature::PrepTime,
        Feature::TotalTime,
        Feature::DietKeto,
        Feature::DietLowCarb,
        Feature::DietVegan,
        Feature::DietVegetarian,
        Feature::DietPaleo,
        // TODO Feature::InstructionsLength
    ];
}

impl Into<usize> for Feature {
    fn into(self) -> usize {
        self as usize
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CerberusRecipeModel {
    // XXX Am I happy with this model?
    recipe_id: u64,
    name: String,
    slug: String,
    site_name: String,
    crawl_url: String,
    ingredients: Vec<String>,
    instructions: Vec<String>,
    diets: HashMap<String, f64>,

    // TODO bump featurevec to u32?
    prep_time: Option<u32>,
    total_time: Option<u32>,
    cook_time: Option<u32>,

    calories: Option<f64>,
    fat_content: Option<f64>,
    carbohydrate_content: Option<f64>,
    protein_content: Option<f64>,
    // FIXME I'll be ignoring similar_recipe_ids because fun :-)
    //       And also because I need something to allow me moving away from lucene
    //       Decide what to do. Options:
    //          1. Create a minhashes (and boost with FeatureVector similarity)
    //          2. ...?
    similar_recipe_ids: Vec<u64>,
}

fn does_not_exist(dir_path: String) -> Result<(), String> {
    if Path::new(dir_path.as_str()).exists() {
        Err("Path already exists".to_owned())
    } else {
        Ok(())
    }
}

fn main() -> io::Result<()> {
    let matches = App::new("cantine")
        .subcommand(
            SubCommand::with_name("check_database")
                .about("Verifies input from STDIN can be found in the database")
                .arg(
                    Arg::with_name("database_dir")
                        .index(1)
                        .required(true)
                        .help("Path to the output database directory"),
                ),
        )
        .subcommand(
            SubCommand::with_name("query")
                .about("Search for recipes, from the command line!")
                .arg(
                    Arg::with_name("base_dir")
                        .short("b")
                        .long("base-dir")
                        .takes_value(true)
                        .required(true)
                        .help("Path to the data built by `load`"),
                )
                .arg(
                    Arg::with_name("query")
                        .index(1)
                        .required(true)
                        .help("SearchRequest as json"),
                ),
        )
        .subcommand(
            SubCommand::with_name("load")
                .about("Loads data from STDIN into the search index")
                .arg(
                    Arg::with_name("buffer_size")
                        .short("b")
                        .long("buffer-size")
                        .default_value("400")
                        .takes_value(true)
                        .help("Size of the buffer for the writer. In MBs"),
                )
                .arg(
                    Arg::with_name("commit_every")
                        .short("c")
                        .long("commit-every")
                        .default_value("50000")
                        .takes_value(true)
                        .help("Controls how often to commit"),
                )
                .arg(
                    Arg::with_name("output_dir")
                        .index(1)
                        .required(true)
                        .validator(does_not_exist)
                        .help("Path to the output directory that should be created"),
                ),
        )
        .get_matches();

    if let Some(load_matches) = matches.subcommand_matches("load") {
        cmd::load(load_matches)?;
    } else if let Some(dbm) = matches.subcommand_matches("check_database") {
        cmd::check_database(dbm)?;
    } else if let Some(sm) = matches.subcommand_matches("query") {
        cmd::query(sm)?;
    }

    Ok(())
}
