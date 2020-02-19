use std::{
    convert::TryFrom,
    env,
    path::Path,
    sync::{mpsc, Arc},
    thread::spawn,
};

use crossbeam_channel;
use tantivy::{schema::Term, Index, Result};

use cantine::{
    database::DatabaseReader,
    index::RecipeIndex,
    model::{Recipe, RecipeId, Sort},
};
use tique::topterms::TopTerms;

#[derive(Debug)]
struct Checked {
    id: RecipeId,
    num_found: usize,
    recall: (usize, usize),
    position: Option<usize>,
    top_pretty: Vec<String>,
}

fn main() -> Result<()> {
    let base_dir = env::args()
        .nth(1)
        .expect("First parameter is a path to a directory");

    let base_path = Path::new(&base_dir);
    let index_path = base_path.join("tantivy");
    let db_path = base_path.join("database");

    let index = Index::open_in_dir(&index_path)?;
    let reader = index.reader()?;

    let recipe_index = Arc::new(RecipeIndex::try_from(&index.schema())?);
    let database = Arc::new(DatabaseReader::<Recipe>::open(&db_path)?);
    let topterms = Arc::new(TopTerms::new(&index, vec![recipe_index.fulltext])?);

    let (id_sender, id_receiver) = crossbeam_channel::unbounded();
    let (checked_sender, checked_receiver) = mpsc::channel();

    let mut workers = Vec::new();
    for _ in 0..4 {
        let receiver = id_receiver.clone();
        let database = database.clone();
        let recipe_index = recipe_index.clone();
        let topterms = topterms.clone();
        let searcher = reader.searcher();
        let checked_sender = checked_sender.clone();

        workers.push(spawn(move || -> Result<()> {
            for id in receiver {
                let recipe = database
                    .find_by_id(id)
                    .map(|res| res.ok())
                    .flatten()
                    .expect("ids are valid and db is healthy");

                let mut input = Vec::new();

                input.push(recipe.name.as_str());
                for ingredient in &recipe.ingredients {
                    input.push(ingredient.as_str());
                }
                for instruction in &recipe.instructions {
                    input.push(instruction.as_str());
                }

                let keywords = topterms.extract_filtered(
                    20,
                    input.join("\n").as_str(),
                    |term: &Term, _tf, doc_freq, _num_docs| {
                        // I haven't put any effort in the tokenization step,
                        // so there's plenty of "relevant rubbish" in the
                        // index like "100g", "tbsp", unicode fractions, etc.
                        // These heuristics are just an attempt of reducing the
                        // garbage, but a decent injection pipeline should be getting
                        // rid of these, not an ad-hoc filter
                        let text = term.text();
                        doc_freq > 5 && text.chars().count() > 4 && !text.ends_with("tbsp")
                    },
                );

                let top_pretty = keywords
                    .terms()
                    .take(5)
                    .map(|term| term.text().to_string())
                    .collect::<Vec<_>>();

                let (_num_matching, more_like_this_ids, _after) = recipe_index.search(
                    &searcher,
                    &keywords.into_query(),
                    11,
                    Sort::Relevance,
                    None,
                )?;

                let position = more_like_this_ids
                    .iter()
                    .position(|&sim_id| recipe.recipe_id == sim_id);

                let mut hit = 0;
                for id in recipe.similar_recipe_ids.iter() {
                    if more_like_this_ids.iter().any(|found_id| found_id == id) {
                        hit += 1;
                    }
                }

                let checked = Checked {
                    id: recipe.recipe_id,
                    num_found: more_like_this_ids.len(),
                    position,
                    recall: (hit, recipe.similar_recipe_ids.len()),
                    top_pretty,
                };

                checked_sender.send(checked).expect("send() always works");
            }

            Ok(())
        }));
    }
    drop(id_receiver);
    drop(checked_sender);

    for &id in database.ids() {
        id_sender.send(id).expect("send() always works");
    }
    drop(id_sender);

    for checked in checked_receiver {
        println!(
            "{},{},{},{},{},{}",
            checked.id,
            checked.top_pretty.join(";"),
            checked.num_found,
            checked.recall.0,
            checked.recall.1,
            checked.position.map(|p| p as isize).unwrap_or(-1)
        );
    }

    for worker in workers.into_iter() {
        worker.join().expect("join() always works")?;
    }

    Ok(())
}
