use once_cell::sync::Lazy;
use std::cmp::Ordering;
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

use tique::QueryParser;

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
                .search(&searcher, &AllQuery, 10, Sort::Relevance, after)?;

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
fn num_ingredients_sort() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let (_total, found_ids, _next) =
        GLOBAL
            .cantine
            .search(&searcher, &AllQuery, INDEX_SIZE, Sort::NumIngredients, None)?;

    let mut last = None;
    for id in found_ids {
        let recipe = GLOBAL.db.get(&id).unwrap();
        let current = recipe.features.num_ingredients;
        if let Some(prev) = last {
            assert!(current <= prev);
        }
        last = Some(current)
    }

    let (_total, found_ids_asc, _next) = GLOBAL.cantine.search(
        &searcher,
        &AllQuery,
        INDEX_SIZE,
        Sort::NumIngredientsAsc,
        None,
    )?;

    let mut last = None;
    for id in found_ids_asc {
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
    ($name: ident, $sort: expr, $field: ident, $type: ident, $range: ident, $order: expr) => {
        #[test]
        fn $name() -> Result<()> {
            let reader = GLOBAL.index.reader()?;
            let searcher = reader.searcher();

            // Ensure we only get hits that actually have the feature
            let query = RangeQuery::$range(
                GLOBAL.cantine.features.$field,
                std::$type::MIN..std::$type::MAX,
            );

            let mut after = None;
            loop {
                let (_total, found_ids, next) =
                    GLOBAL.cantine.search(&searcher, &query, 10, $sort, after)?;

                let mut last_val = None;
                for id in found_ids {
                    let recipe = GLOBAL.db.get(&id).unwrap();
                    let current = recipe.features.$field.unwrap();
                    if let Some(last) = last_val {
                        let order = current.partial_cmp(&last).unwrap();
                        if order != Ordering::Equal {
                            assert_eq!($order, order);
                        }
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

stress_sort_pagination!(
    sort_total_time,
    Sort::TotalTime,
    total_time,
    u64,
    new_u64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_total_time_asc,
    Sort::TotalTimeAsc,
    total_time,
    u64,
    new_u64,
    Ordering::Greater
);

stress_sort_pagination!(
    sort_cook_time,
    Sort::CookTime,
    cook_time,
    u64,
    new_u64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_cook_time_asc,
    Sort::CookTimeAsc,
    cook_time,
    u64,
    new_u64,
    Ordering::Greater
);

stress_sort_pagination!(
    sort_prep_time,
    Sort::PrepTime,
    prep_time,
    u64,
    new_u64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_prep_time_asc,
    Sort::PrepTimeAsc,
    prep_time,
    u64,
    new_u64,
    Ordering::Greater
);

stress_sort_pagination!(
    sort_calories,
    Sort::Calories,
    calories,
    u64,
    new_u64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_calories_asc,
    Sort::CaloriesAsc,
    calories,
    u64,
    new_u64,
    Ordering::Greater
);

stress_sort_pagination!(
    sort_fat_content,
    Sort::FatContent,
    fat_content,
    f64,
    new_f64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_fat_content_asc,
    Sort::FatContentAsc,
    fat_content,
    f64,
    new_f64,
    Ordering::Greater
);

stress_sort_pagination!(
    sort_carb_content,
    Sort::CarbContent,
    carb_content,
    f64,
    new_f64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_carb_content_asc,
    Sort::CarbContentAsc,
    carb_content,
    f64,
    new_f64,
    Ordering::Greater
);

stress_sort_pagination!(
    sort_protein_content,
    Sort::ProteinContent,
    protein_content,
    f64,
    new_f64,
    Ordering::Less
);
stress_sort_pagination!(
    sort_protein_content_asc,
    Sort::ProteinContentAsc,
    protein_content,
    f64,
    new_f64,
    Ordering::Greater
);

#[test]
fn ascending_sort_works_for_relevance() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let parser = QueryParser::new(
        &GLOBAL.index,
        vec![
            GLOBAL.cantine.name,
            GLOBAL.cantine.ingredients,
            GLOBAL.cantine.instructions,
        ],
    )?;

    let query = parser.parse("+potato +cheese").unwrap();

    let (_total, found_ids, _next) =
        GLOBAL
            .cantine
            .search(&searcher, &query, INDEX_SIZE, Sort::Relevance, None)?;

    let (total, mut found_ids_asc, _next) =
        GLOBAL
            .cantine
            .search(&searcher, &query, INDEX_SIZE, Sort::RelevanceAsc, None)?;

    assert!(total > 5);
    // NOTE Flaky test: the only reason the reverse check works
    //      here is because every matching doc has a distinct
    //      score.
    //      The reverse() logic doesn't work when scores are
    //      the same because the topk breaks even by id
    found_ids_asc.reverse();
    assert_eq!(found_ids, found_ids_asc);

    Ok(())
}
