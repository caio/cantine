use std::collections::HashMap;
use std::io::{self, BufRead, Result};
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::{mpsc::channel, Arc, RwLock};
use std::thread::spawn;
use std::time::Instant;

use crossbeam_channel::unbounded;
use serde::{Deserialize, Serialize};
use serde_json;
use structopt::StructOpt;

use tantivy::{
    self,
    directory::MmapDirectory,
    schema::{self, Field, SchemaBuilder},
    Document, Index,
};

use cantine::database::BincodeDatabase;

#[derive(Deserialize, Serialize, Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CerberusRecipeModel {
    recipe_id: u64,
    name: String,
    slug: String,
    site_name: String,
    crawl_url: String,
    ingredients: Vec<String>,
    instructions: Vec<String>,
    diets: HashMap<String, f64>,

    prep_time: Option<u32>,
    total_time: Option<u32>,
    cook_time: Option<u32>,

    calories: Option<f64>,
    fat_content: Option<f64>,
    carbohydrate_content: Option<f64>,
    protein_content: Option<f64>,
    similar_recipe_ids: Vec<u64>,
}

#[derive(Debug, StructOpt)]
#[structopt(name = "load")]
pub struct LoadOptions {
    #[structopt(short, long, default_value = "400")]
    buffer_size: NonZeroUsize,
    #[structopt(short, long, default_value = "100000")]
    commit_every: NonZeroUsize,
    #[structopt(short, long, default_value = "4")]
    num_producers: NonZeroUsize,
    #[structopt(validator = does_not_exist)]
    output_dir: String,
}

fn does_not_exist(dir_path: String) -> std::result::Result<(), String> {
    if Path::new(dir_path.as_str()).exists() {
        Err("Path already exists".to_owned())
    } else {
        Ok(())
    }
}

fn make_document(fields: &IndexFields, recipe: &CerberusRecipeModel) -> Document {
    let mut doc = Document::new();
    //
    doc.add_u64(fields.id, recipe.recipe_id);

    let mut fulltext = Vec::new();

    fulltext.push(recipe.name.as_str());
    for ingredient in &recipe.ingredients {
        fulltext.push(ingredient.as_str());
    }
    for instruction in &recipe.instructions {
        fulltext.push(instruction.as_str());
    }
    doc.add_text(fields.fulltext, fulltext.join("\n").as_str());

    // FIXME actually index the features
    // feats.push((Feature::NumIngredients, recipe.ingredients.len() as u16));

    if let Some(kcal) = recipe.calories {
        // feats.push((Feature::Calories, kcal as u16))
    }
    if let Some(fat) = recipe.fat_content {
        // feats.push((Feature::FatContent, fat as u16))
    }
    if let Some(carbs) = recipe.carbohydrate_content {
        // feats.push((Feature::CarbContent, carbs as u16))
    }
    if let Some(prot) = recipe.protein_content {
        // feats.push((Feature::ProteinContent, prot as u16))
    }
    if let Some(prep) = recipe.prep_time {
        // feats.push((Feature::PrepTime, prep as u16))
    }
    if let Some(cook) = recipe.cook_time {
        // feats.push((Feature::CookTime, cook as u16))
    }
    if let Some(total) = recipe.total_time {
        // feats.push((Feature::PrepTime, total as u16))
    }

    for (diet, score) in &recipe.diets {
        // match diet.as_str() {
        //     "keto" => feats.push((Feature::DietKeto, bucket_threshold(score))),
        //     "lowcarb" => feats.push((Feature::DietLowCarb, bucket_threshold(score))),
        //     "paleo" => feats.push((Feature::DietPaleo, bucket_threshold(score))),
        //     "vegan" => feats.push((Feature::DietVegan, bucket_threshold(score))),
        //     "vegetarian" => feats.push((Feature::DietVegetarian, bucket_threshold(score))),
        //     _ => panic!("off!"),
        // }
    }

    doc
}

#[derive(Clone)]
struct IndexFields {
    id: Field,
    fulltext: Field,
}

impl IndexFields {
    fn from_builder(builder: &mut SchemaBuilder) -> Self {
        IndexFields {
            id: builder.add_u64_field("id", schema::STORED),
            fulltext: builder.add_text_field("fulltext", schema::TEXT),
        }
    }
}

pub fn load(options: LoadOptions) -> Result<()> {
    println!("Started with {:?}", &options);

    let base_path = Path::new(options.output_dir.as_str());
    let db_path = base_path.join("database");
    let index_path = base_path.join("tantivy");

    std::fs::create_dir_all(&db_path)?;
    std::fs::create_dir(&index_path)?;

    let mut builder = SchemaBuilder::new();

    let fields = IndexFields::from_builder(&mut builder);

    let index =
        Index::open_or_create(MmapDirectory::open(&index_path).unwrap(), builder.build()).unwrap();

    // A SpMc channel to paralellize decode and index preparation
    let (line_sender, line_receiver) = unbounded::<String>();
    // A MpSc channel to control index commit and write to db
    let (recipe_sender, recipe_receiver) = channel();

    let buffer_size = options.buffer_size.get();
    let writer = Arc::new(RwLock::new(index.writer(buffer_size * 1_000_000).unwrap()));

    let num_producers = options.num_producers.get();
    let mut workers = Vec::with_capacity(num_producers);
    for _ in 0..num_producers {
        let receiver = line_receiver.clone();
        let writer = writer.clone();
        let recipe_sender = recipe_sender.clone();

        let fields = fields.clone();
        workers.push(spawn(move || {
            for line in receiver.iter() {
                let recipe: CerberusRecipeModel =
                    serde_json::from_str(line.as_ref()).expect("valid recipe json");

                writer
                    .read()
                    .unwrap()
                    .add_document(make_document(&fields, &recipe));

                recipe_sender.send(recipe).unwrap();
            }
        }))
    }

    let disk_writer = spawn(move || {
        let mut db = BincodeDatabase::new(&db_path).unwrap();

        let cur = Instant::now();
        let mut num_recipes = 0;

        for recipe in recipe_receiver {
            num_recipes += 1;
            db.add(recipe.recipe_id, &recipe).unwrap();

            if num_recipes % options.commit_every.get() == 0 {
                writer.write().unwrap().commit().unwrap();

                println!(
                    "DiskWriter: {} Documents so far (@ {} secs).",
                    num_recipes,
                    cur.elapsed().as_secs()
                );
            }
        }

        writer.write().unwrap().commit().unwrap();

        println!(
            "DiskWriter: Wrote {} documents in {} seconds",
            num_recipes,
            cur.elapsed().as_secs()
        );
    });

    for line in io::stdin().lock().lines().filter_map(Result::ok) {
        line_sender.send(line).unwrap();
    }

    drop(line_sender);

    for worker in workers.into_iter() {
        worker.join().unwrap();
    }

    drop(recipe_sender);

    disk_writer.join().unwrap();

    println!("Done!");

    Ok(())
}

fn main() -> Result<()> {
    load(LoadOptions::from_args())
}
