use serde_json;

use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};
use tantivy::{
    query::{AllQuery, RangeQuery},
    schema::SchemaBuilder,
    Index, Result,
};

use cantine::{
    index::RecipeIndex,
    model::{Recipe, RecipeId, Sort},
};

use tique::queryparser::QueryParser;

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

    let mut after = None;
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
            after = Some(new_after);
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
        None,
    )?;

    let mut last = None;
    for id in found_ids {
        let recipe = GLOBAL.db.get(&id).unwrap();
        let current = recipe.features.num_ingredients;
        if let Some(prev) = last {
            assert!(current <= prev);
        }
        last = Some(current)
    }

    let (_total, asc_found_ids, _next) = GLOBAL.cantine.search(
        &searcher,
        &AllQuery,
        INDEX_SIZE,
        Sort::NumIngredients,
        true,
        None,
    )?;

    let mut last = None;
    for id in asc_found_ids {
        let recipe = GLOBAL.db.get(&id).unwrap();
        let current = recipe.features.num_ingredients;
        if let Some(prev) = last {
            assert!(current >= prev);
        }
        last = Some(current)
    }

    Ok(())
}

macro_rules! stress_sort_pagination {
    ($name: ident, $sort: expr, $field: ident, $type: ident, $range: ident) => {
        #[test]
        fn $name() -> Result<()> {
            let reader = GLOBAL.index.reader()?;
            let searcher = reader.searcher();

            // Ensure we only get hits that actually have the feature
            let query = RangeQuery::$range(
                GLOBAL.cantine.features.$field,
                std::$type::MIN..std::$type::MAX,
            );

            // Descending
            let mut after = None;
            loop {
                let (_total, found_ids, next) = GLOBAL
                    .cantine
                    .search(&searcher, &query, 10, $sort, false, after)?;

                let mut last_val = None;
                for id in found_ids {
                    let recipe = GLOBAL.db.get(&id).unwrap();
                    let current = recipe.features.$field.unwrap();
                    if let Some(last) = last_val {
                        assert!(current <= last)
                    }
                    last_val = Some(current);
                }

                if let Some(new_after) = next {
                    after = Some(new_after);
                } else {
                    break;
                }
            }

            // Ascending
            let mut after = None;
            loop {
                let (_total, found_ids, next) = GLOBAL
                    .cantine
                    .search(&searcher, &query, 10, $sort, true, after)?;

                let mut last_val = None;
                for id in found_ids {
                    let recipe = GLOBAL.db.get(&id).unwrap();
                    let current = recipe.features.$field.unwrap();
                    if let Some(last) = last_val {
                        assert!(current >= last)
                    }
                    last_val = Some(current);
                }

                if let Some(new_after) = next {
                    after = Some(new_after);
                } else {
                    break;
                }
            }

            Ok(())
        }
    };
}

stress_sort_pagination!(sort_total_time, Sort::TotalTime, total_time, u64, new_u64);
stress_sort_pagination!(sort_cook_time, Sort::CookTime, cook_time, u64, new_u64);
stress_sort_pagination!(sort_prep_time, Sort::PrepTime, prep_time, u64, new_u64);
stress_sort_pagination!(sort_calories, Sort::Calories, calories, u64, new_u64);

stress_sort_pagination!(
    sort_fat_content,
    Sort::FatContent,
    fat_content,
    f64,
    new_f64
);

stress_sort_pagination!(
    sort_carb_content,
    Sort::CarbContent,
    carb_content,
    f64,
    new_f64
);

stress_sort_pagination!(
    sort_protein_content,
    Sort::ProteinContent,
    protein_content,
    f64,
    new_f64
);

#[test]
fn ascending_sort_works_for_relevance() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let parser = QueryParser::new(
        GLOBAL.cantine.fulltext,
        GLOBAL.index.tokenizer_for_field(GLOBAL.cantine.fulltext)?,
        true,
    );

    let query = parser.parse("potato cheese")?.unwrap();

    let (_total, found_ids, _next) =
        GLOBAL
            .cantine
            .search(&searcher, &query, INDEX_SIZE, Sort::Relevance, false, None)?;

    let (total, mut asc_found_ids, _next) =
        GLOBAL
            .cantine
            .search(&searcher, &query, INDEX_SIZE, Sort::Relevance, true, None)?;

    assert!(total > 5);
    // NOTE Flaky test: the only reason the reverse check works
    //      here is because every matching doc has a distinct
    //      score.
    //      The reverse() logic doesn't work when scores are
    //      the same because the topk breaks even by id
    asc_found_ids.reverse();
    assert_eq!(found_ids, asc_found_ids);

    Ok(())
}
