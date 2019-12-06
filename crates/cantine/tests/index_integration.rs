use serde_json;

use once_cell::sync::Lazy;
use std::convert::TryFrom;
use tantivy::{query::AllQuery, schema::SchemaBuilder, Index, Result};

use cantine::{
    index::Cantine,
    index::IndexFields,
    model::{Recipe, SearchCursor, Sort},
};

struct GlobalData {
    index: Index,
    cantine: Cantine,
}

static GLOBAL: Lazy<GlobalData> = Lazy::new(|| {
    let mut builder = SchemaBuilder::new();
    let fields = IndexFields::from(&mut builder);
    let index = Index::create_in_ram(builder.build());

    let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();

    let sample_recipes = include_str!("sample_recipes.jsonlines");

    for line in sample_recipes.lines() {
        let recipe: Recipe = serde_json::from_str(line).expect("valid recipe json");

        writer.add_document(fields.make_document(&recipe));
    }

    writer.commit().unwrap();

    let cantine = Cantine::try_from(&index).unwrap();

    GlobalData { index, cantine }
});

const INDEX_SIZE: usize = 295;

#[test]
fn index_has_recipes() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    assert_eq!(INDEX_SIZE as u64, searcher.num_docs());

    Ok(())
}

#[test]
fn pagination_works() -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    let (total, found_ids, next) = GLOBAL.cantine.search(
        &searcher,
        &AllQuery,
        10,
        Sort::Relevance,
        SearchCursor::START,
    )?;

    assert_eq!(INDEX_SIZE, total);
    assert!(next.is_some());

    let mut after = next.unwrap();
    let mut total_found = found_ids.len();

    loop {
        let (_total, found_ids, next) =
            GLOBAL
                .cantine
                .search(&searcher, &AllQuery, 10, Sort::Relevance, after)?;

        total_found += found_ids.len();

        if let Some(new_after) = next {
            after = new_after;
        } else {
            break;
        }
    }

    assert_eq!(INDEX_SIZE, total_found);

    Ok(())
}
