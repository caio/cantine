use serde_json;

use once_cell::sync::Lazy;
use std::{
    collections::{HashMap, HashSet},
    convert::TryFrom,
};
use tantivy::{query::AllQuery, schema::SchemaBuilder, Index, Result};

use cantine::{
    index::{Cantine, IndexFields},
    model::{Recipe, RecipeId, SearchCursor, Sort},
};

struct GlobalData {
    index: Index,
    cantine: Cantine,
    db: HashMap<RecipeId, Recipe>,
}

static GLOBAL: Lazy<GlobalData> = Lazy::new(|| {
    let mut builder = SchemaBuilder::new();
    let fields = IndexFields::from(&mut builder);
    let index = Index::create_in_ram(builder.build());

    let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();

    let sample_recipes = include_str!("sample_recipes.jsonlines");

    let mut db = HashMap::with_capacity(INDEX_SIZE);
    for line in sample_recipes.lines() {
        let recipe: Recipe = serde_json::from_str(line).expect("valid recipe json");

        writer.add_document(fields.make_document(&recipe));
        db.insert(recipe.recipe_id, recipe);
    }

    writer.commit().unwrap();

    let cantine = Cantine::try_from(&index).unwrap();

    GlobalData { index, cantine, db }
});

const INDEX_SIZE: usize = 295;

#[test]
fn global_state_ok() -> Result<()> {
    assert_eq!(INDEX_SIZE, GLOBAL.db.len());

    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();
    assert_eq!(INDEX_SIZE as u64, searcher.num_docs());

    Ok(())
}

#[test]
fn pagination_works() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let mut after = SearchCursor::START;
    let mut seen = HashSet::with_capacity(INDEX_SIZE);

    loop {
        let (_total, found_ids, next) =
            GLOBAL
                .cantine
                .search(&searcher, &AllQuery, 10, Sort::Relevance, after)?;

        for id in found_ids {
            seen.insert(id);
        }

        if let Some(new_after) = next {
            after = new_after;
        } else {
            break;
        }
    }

    assert_eq!(INDEX_SIZE, seen.len());

    Ok(())
}

#[test]
fn sort_works() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let (_total, found_ids, _next) = GLOBAL.cantine.search(
        &searcher,
        &AllQuery,
        INDEX_SIZE,
        Sort::NumIngredients,
        SearchCursor::START,
    )?;

    let mut last_num_ingredients = std::u8::MAX;
    for id in found_ids {
        let recipe = GLOBAL.db.get(&id).unwrap();
        let num_ingredients = recipe.features.num_ingredients;
        assert!(num_ingredients <= last_num_ingredients);
        last_num_ingredients = num_ingredients;
    }

    Ok(())
}