use std::{
    env,
    io::{self, BufRead},
    path::Path,
    str::FromStr,
    sync::{mpsc::channel, Arc, RwLock},
    thread::spawn,
    time::Instant,
};

use crossbeam_channel::unbounded;
use serde_json;

use tantivy::{self, directory::MmapDirectory, schema::SchemaBuilder, Index, Result};

use cantine::database::DatabaseWriter;
use cantine::index::RecipeIndex;
use cantine::model::Recipe;

/// Loads recipes as json into cantine's database and index
#[derive(Debug)]
pub struct LoadOptions {
    /// Size for tantivy's writer buffer in MBs
    buffer_size: usize,
    /// How many recipes to ingest before comitting
    commit_every: usize,
    /// Number of worker threads to start
    num_producers: usize,
    /// Path to a non-existing directory
    output_dir: String,
}

fn load(options: LoadOptions) -> Result<()> {
    log::info!("Started with {:?}", &options);

    let base_path = Path::new(options.output_dir.as_str());
    let db_path = base_path.join("database");
    let index_path = base_path.join("tantivy");

    std::fs::create_dir_all(&db_path)?;
    std::fs::create_dir(&index_path)?;

    let mut builder = SchemaBuilder::new();

    let fields = RecipeIndex::from(&mut builder);

    let index = Index::open_or_create(MmapDirectory::open(&index_path)?, builder.build())?;

    // A SpMc channel to paralellize decode and index preparation
    let (line_sender, line_receiver) = unbounded::<String>();
    // A MpSc channel to control index commit and write to db
    let (recipe_sender, recipe_receiver) = channel();

    let buffer_size = options.buffer_size;
    let writer = Arc::new(RwLock::new(index.writer(buffer_size * 1_000_000)?));

    let num_producers = options.num_producers;
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
                    .add_document(fields.make_document(&recipe));

                recipe_sender.send(recipe).expect("send always works");
            }
        }))
    }

    let disk_writer = spawn(move || -> Result<()> {
        let mut db = DatabaseWriter::new(db_path)?;

        let cur = Instant::now();
        let mut num_recipes = 0;

        for recipe in recipe_receiver {
            num_recipes += 1;
            db.append(&recipe)?;

            if num_recipes % options.commit_every == 0 {
                writer.write()?.commit()?;

                log::info!(
                    "DiskWriter: {} Documents so far (@ {} secs).",
                    num_recipes,
                    cur.elapsed().as_secs()
                );
            }
        }

        writer.write()?.commit()?;

        log::info!(
            "DiskWriter: Wrote {} documents in {} seconds",
            num_recipes,
            cur.elapsed().as_secs()
        );

        Ok(())
    });

    for line in io::stdin().lock().lines() {
        line_sender.send(line?).unwrap();
    }

    drop(line_sender);

    for worker in workers.into_iter() {
        worker.join().unwrap();
    }

    drop(recipe_sender);

    disk_writer.join().unwrap()?;

    log::info!("Done!");

    Ok(())
}

const BUFFER_SIZE: &str = "BUFFER_SIZE";
const COMMIT_EVERY: &str = "COMMIT_EVERY";
const NUM_PRODUCERS: &str = "NUM_PRODUCERS";

fn get_usize_from_env_or(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .map(|v| usize::from_str(&v).expect("valid usize"))
        .unwrap_or(default)
}

fn main() -> Result<()> {
    let output_dir = env::args()
        .nth(1)
        .expect("First parameter must be the output directory");

    let buffer_size = get_usize_from_env_or(BUFFER_SIZE, 1000);

    let commit_every = get_usize_from_env_or(COMMIT_EVERY, 300_000);

    let num_producers = get_usize_from_env_or(NUM_PRODUCERS, 4);

    let options = LoadOptions {
        output_dir,
        buffer_size,
        commit_every,
        num_producers,
    };

    load(options)
}
