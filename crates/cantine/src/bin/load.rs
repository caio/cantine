use std::{
    io::{self, BufRead, Result},
    num::{NonZeroU64, NonZeroUsize},
    path::Path,
    result::Result as StdResult,
    sync::{mpsc::channel, Arc, RwLock},
    thread::spawn,
    time::Instant,
};

use crossbeam_channel::unbounded;
use serde_json;
use structopt::StructOpt;

use tantivy::{self, directory::MmapDirectory, schema::SchemaBuilder, Document, Index};

use cantine::database::BincodeDatabase;
use cantine::index::IndexFields;
use cantine::model::Recipe;

/// Loads recipes as json into cantine's database and index
#[derive(Debug, StructOpt)]
#[structopt(name = "load")]
pub struct LoadOptions {
    /// Size for tantivy's writer buffer in MBs
    #[structopt(short, long, default_value = "400")]
    buffer_size: NonZeroUsize,
    /// How many recipes to ingest before comitting
    #[structopt(short, long, default_value = "100000")]
    commit_every: NonZeroUsize,
    /// Number of worker threads to start
    #[structopt(short, long, default_value = "4")]
    num_producers: NonZeroUsize,
    /// Size in MBs to pre-allocate the database
    #[structopt(short, long, default_value = "1000")]
    database_size: NonZeroU64,
    /// Path to a non-existing directory
    #[structopt(validator = does_not_exist)]
    output_dir: String,
}

fn does_not_exist(dir_path: String) -> StdResult<(), String> {
    if Path::new(dir_path.as_str()).exists() {
        Err("Path already exists".to_owned())
    } else {
        Ok(())
    }
}

fn make_document(fields: &IndexFields, recipe: &Recipe) -> Document {
    let mut doc = Document::new();
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

    fields.features.add_to_doc(&mut doc, &recipe.features);
    doc
}

fn load(options: LoadOptions) -> Result<()> {
    println!("Started with {:?}", &options);

    let base_path = Path::new(options.output_dir.as_str());
    let db_path = base_path.join("database");
    let index_path = base_path.join("tantivy");

    std::fs::create_dir_all(&db_path)?;
    std::fs::create_dir(&index_path)?;

    let mut builder = SchemaBuilder::new();

    let fields = IndexFields::from(&mut builder);

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
            for line in receiver {
                let recipe: Recipe =
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
        let mut db =
            BincodeDatabase::create(db_path, options.database_size.get() * 1024 * 1024).unwrap();

        let cur = Instant::now();
        let mut num_recipes = 0;

        for recipe in recipe_receiver {
            num_recipes += 1;
            db.add(&recipe).unwrap();

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
