use serde_json;

use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use tantivy::{query::AllQuery, schema::SchemaBuilder, Index, Result};

use cantine::{
    index::{After, RecipeIndex},
    model::{Recipe, RecipeId, Sort},
};

struct GlobalData {
    index: Index,
    cantine: RecipeIndex,
    db: HashMap<RecipeId, Recipe>,
}

static GLOBAL: Lazy<GlobalData> = Lazy::new(|| {
    let mut builder = SchemaBuilder::new();
    let cantine = RecipeIndex::from(&mut builder);
    let index = Index::create_in_ram(builder.build());

    let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();

    let sample_recipes = include_str!("sample_recipes.jsonlines");

    let mut db = HashMap::with_capacity(INDEX_SIZE);
    for line in sample_recipes.lines() {
        let recipe: Recipe = serde_json::from_str(line).expect("valid recipe json");

        writer.add_document(cantine.make_document(&recipe));
        db.insert(recipe.recipe_id, recipe);
    }

    writer.commit().unwrap();

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

    let mut after = After::START;
    let mut seen = HashSet::with_capacity(INDEX_SIZE);

    loop {
        let (_total, found_ids, next) =
            GLOBAL
                .cantine
                .search(&searcher, &AllQuery, 10, Sort::Relevance, false, after)?;

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
        false,
        After::START,
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

#[test]
fn float_field_sorting() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let (_total, found_ids, _next) = GLOBAL.cantine.search(
        &searcher,
        &AllQuery,
        INDEX_SIZE,
        Sort::ProteinContent,
        false,
        After::START,
    )?;

    let mut last_protein = std::f32::MAX;
    for id in found_ids {
        let recipe = GLOBAL.db.get(&id).unwrap();
        if let Some(protein_content) = recipe.features.protein_content {
            assert!(protein_content <= last_protein);
            last_protein = protein_content;
        }
    }

    assert!(last_protein < std::f32::MAX);

    Ok(())
}

#[test]
fn ascending_sort() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let (_total, found_ids, _next) = GLOBAL.cantine.search(
        &searcher,
        &AllQuery,
        INDEX_SIZE,
        Sort::InstructionsLength,
        true,
        After::START,
    )?;

    let mut last_len = 0;
    for id in found_ids {
        let recipe = GLOBAL.db.get(&id).unwrap();
        let cur_len = recipe.features.instructions_length;
        assert!(cur_len >= last_len);
        last_len = cur_len;
    }

    Ok(())
}
