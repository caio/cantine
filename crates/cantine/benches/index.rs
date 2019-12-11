use criterion::{black_box, criterion_group, criterion_main, Criterion};

use once_cell::sync::Lazy;
use std::{env, path::Path};
use tantivy::{query::AllQuery, Index, Result};

use cantine::{
    index::Cantine,
    model::{SearchCursor, Sort},
};

struct GlobalData {
    index: Index,
    cantine: Cantine,
}

static GLOBAL: Lazy<GlobalData> = Lazy::new(|| {
    let (index, cantine) =
        Cantine::open(Path::new(env::var("CANTINE_PATH").unwrap().as_str())).unwrap();

    GlobalData { index, cantine }
});

fn search_all(num_items: usize) -> Result<()> {
    let reader = GLOBAL.index.reader()?;
    let searcher = reader.searcher();

    GLOBAL
        .cantine
        .search(
            &searcher,
            &AllQuery,
            num_items,
            Sort::Relevance,
            SearchCursor::START,
        )
        .unwrap();

    Ok(())
}

fn basic_all_query_search(c: &mut Criterion) {
    let reader = GLOBAL.index.reader().unwrap();
    let searcher = reader.searcher();

    let index_size = searcher.num_docs();

    c.bench_function(
        format!("search_allquery_{}_{}", index_size, 20).as_str(),
        |b| b.iter(|| search_all(black_box(20))),
    );
}

criterion_group!(benches, basic_all_query_search);
criterion_main!(benches);
