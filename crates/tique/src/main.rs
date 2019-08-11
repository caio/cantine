use std::collections::HashMap;
use std::io::{self, BufRead};
use std::path::Path;
use std::sync::mpsc::channel;
use std::thread::spawn;
use std::time::Instant;

mod database;
mod search;

use database::{BincodeDatabase, Database};
use search::{Feature, FeatureIndexFields};

use clap::{value_t, App, Arg, ArgMatches, SubCommand};
use serde::{Deserialize, Serialize};
use serde_json;

use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery},
    schema::{
        Field, FieldType, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
        Value, FAST, INDEXED, STORED,
    },
    tokenizer::TokenizerManager,
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

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

fn check_database(matches: &ArgMatches) -> io::Result<()> {
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

fn load_data(matches: &ArgMatches) -> io::Result<()> {
    let output_dir = matches.value_of("output_dir").unwrap();
    let buf_size = value_t!(matches, "buffer_size", usize).unwrap();
    let commit_every = value_t!(matches, "commit_every", u64).unwrap();

    let base_path = Path::new(output_dir);
    let db_path = base_path.join("database");
    let index_path = base_path.join("tantivy");
    std::fs::create_dir_all(&db_path)?;
    std::fs::create_dir(&index_path)?;

    let (line_sender, lines) = channel();

    spawn(move || {
        println!("StdinLines: started!");
        let stdin = io::stdin();

        for line in stdin.lock().lines() {
            line_sender.send(line.unwrap()).unwrap();
        }

        println!("StdinLines: finished!!");
    });

    let (schema, fields) = FeatureIndexFields::new();
    let index = Index::open_or_create(MmapDirectory::open(&index_path).unwrap(), schema).unwrap();

    let mut writer = index.writer(buf_size * 1_000_000).unwrap();
    println!("IndexWriter: Buffer Size = {} MB", buf_size);

    let (doc_sender, docs) = channel();

    let doc_factory = fields;

    spawn(move || {
        println!("FeatureDocuments: started!");
        let mut database = BincodeDatabase::new::<CerberusRecipeModel>(&db_path).unwrap();

        for line in lines {
            let recipe: CerberusRecipeModel = serde_json::from_str(line.as_str()).unwrap();

            database.add(recipe.recipe_id, &recipe).unwrap();

            let mut fulltext = Vec::new();
            let mut feats = Vec::new();
            feats.push((Feature::NumIngredients, recipe.ingredients.len() as u16));

            fulltext.push(recipe.name);
            for ing in recipe.ingredients {
                fulltext.push(ing);
            }
            for ins in recipe.instructions {
                fulltext.push(ins);
            }

            if let Some(kcal) = recipe.calories {
                feats.push((Feature::Calories, kcal as u16))
            }
            if let Some(fat) = recipe.fat_content {
                feats.push((Feature::FatContent, fat as u16))
            }
            if let Some(carbs) = recipe.carbohydrate_content {
                feats.push((Feature::CarbContent, carbs as u16))
            }
            if let Some(prot) = recipe.protein_content {
                feats.push((Feature::ProteinContent, prot as u16))
            }
            if let Some(prep) = recipe.prep_time {
                feats.push((Feature::PrepTime, prep as u16))
            }
            if let Some(cook) = recipe.cook_time {
                feats.push((Feature::CookTime, cook as u16))
            }
            if let Some(total) = recipe.total_time {
                feats.push((Feature::PrepTime, total as u16))
            }

            let bucket_threshold = |f: f64| {
                let res = (f * 100.0) as u16;
                assert!(res <= 100);
                res
            };

            for (diet, score) in recipe.diets {
                match diet.as_str() {
                    "keto" => feats.push((Feature::DietKeto, bucket_threshold(score))),
                    "lowcarb" => feats.push((Feature::DietLowCarb, bucket_threshold(score))),
                    "paleo" => feats.push((Feature::DietPaleo, bucket_threshold(score))),
                    "vegan" => feats.push((Feature::DietVegan, bucket_threshold(score))),
                    "vegetarian" => feats.push((Feature::DietVegetarian, bucket_threshold(score))),
                    _ => panic!("off!"),
                }
            }

            doc_sender
                .send(doc_factory.make_document(recipe.recipe_id, fulltext.join("\n"), Some(feats)))
                .unwrap();
        }

        println!("FeatureDocuments: finished!");
    });

    let mut docs_added = 0;
    let mut cur = Instant::now();

    for doc in docs {
        FeatureIndexFields::add_document(&mut writer, doc);
        docs_added += 1;

        if docs_added % commit_every == 0 {
            println!("IndexWriter: Comitting {} documents...", commit_every);
            writer.commit().unwrap();

            let new = Instant::now();
            let elapsed = cur.elapsed();
            cur = new;

            let rate = commit_every / elapsed.as_secs();
            println!(
                "IndexWriter: {} Documents so far ({} / sec).",
                docs_added, rate
            );
        }
    }

    println!("DocumentWriter: Comitting...");
    writer.commit().unwrap();

    println!("Done!");
    Ok(())
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
        load_data(load_matches)?;
    } else if let Some(dbm) = matches.subcommand_matches("check_database") {
        check_database(dbm)?;
    }

    Ok(())
}
