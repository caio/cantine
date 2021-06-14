use std::{cmp::Ordering, ops::Neg};

use tantivy::{
    collector::TopDocs,
    fastfield::FastFieldReader,
    query::AllQuery,
    schema::{SchemaBuilder, Value, FAST, STORED},
    Document, Index, Result, SegmentReader,
};

use tique::conditional_collector::{Ascending, Descending, TopCollector};

pub fn main() -> Result<()> {
    // First, we create a test index with a couple of fields
    // And with some documents already in.
    let mut builder = SchemaBuilder::new();

    let id_field = builder.add_u64_field("id", FAST | STORED);
    let rank_field = builder.add_f64_field("rank", FAST);

    let index = Index::create_in_ram(builder.build());
    let mut writer = index.writer_with_num_threads(1, 3_000_000)?;

    const NUM_DOCS: i32 = 100;
    const PAGE_SIZE: usize = 10;

    for i in 0..NUM_DOCS {
        let mut doc = Document::new();
        doc.add_f64(rank_field, f64::from(i.neg()));
        doc.add_u64(id_field, i as u64);
        writer.add_document(doc);
    }

    writer.commit()?;
    let reader = index.reader()?;
    let searcher = reader.searcher();

    // Know that we have an index and a way to search it, let's
    // create our collectors:

    // Let's use one from tantivy to make sure things work as stated
    let tantivy_collector = TopDocs::with_limit(PAGE_SIZE);

    // Now create a conditional_collector that behaves like the one
    // above. The first `_` is `tantivy::Score`, but it gets inferred.
    let tique_collector = TopCollector::<_, Descending, _>::new(PAGE_SIZE, true);

    let (tantivy_top, tique_top) =
        searcher.search(&AllQuery, &(tantivy_collector, tique_collector))?;

    assert_eq!(tantivy_top.len(), tique_top.items.len());
    // Phew!

    // Noticed that we checked against `tique_top.items`? It's because
    // tique's collectors come with some extra metadata to make it more
    // useful.

    // We know how many documents matched the *query*, (not
    // necessarily the range), just like a count collector would.
    // So we expect it to be the number of documents in the index
    // given our query.
    assert_eq!(NUM_DOCS as usize, tique_top.total);

    // We also know if there would have been more items if we
    // asked for:
    assert!(tique_top.has_next());

    // This in useful information because it tells us that
    // we can keep searching easily.

    // One simple way to get the next page is to ask for more
    // results and shift. It's a super fast way that can become
    // problematic for digging deep into very large indices.
    let tantivy_next_collector = TopDocs::with_limit(PAGE_SIZE * 2);

    // Our conditional_collector types know how to paginate based
    // on their own results, which allows you to keep memory stable
    // while spending more CPU time doing comparisons:

    let last_result = tique_top.items.into_iter().last().unwrap();
    let tique_next_collector = TopCollector::<_, Descending, _>::new(PAGE_SIZE, last_result);

    // One disadvantage of this approach is that you can't simply
    // skip to an arbitrary page. When that's a requirement, the
    // best idea is to use the "memory hungry" approach until a
    // certain threshold, then switch to cursor-based.
    // You can even use tantivy's result to paginate:

    let last_tantivy_result = tantivy_top.into_iter().last().unwrap();
    let tique_next_collector_via_tantivy =
        TopCollector::<_, Descending, _>::new(PAGE_SIZE, last_tantivy_result);

    let (tantivy_until_next, tique_next, tique_same_next) = searcher.search(
        &AllQuery,
        &(
            tantivy_next_collector,
            tique_next_collector,
            tique_next_collector_via_tantivy,
        ),
    )?;

    assert_eq!(tique_next.items, tique_same_next.items);
    assert_eq!(tantivy_until_next[PAGE_SIZE..], tique_next.items[..]);

    // We can also sort by the fast fields we indexed:

    let min_rank_collector =
        TopCollector::<f64, Ascending, _>::new(3, true).top_fast_field(rank_field);

    let top_ids_collector =
        TopCollector::<u64, Descending, _>::new(3, true).top_fast_field(id_field);

    let (min_rank, top_ids) =
        searcher.search(&AllQuery, &(min_rank_collector, top_ids_collector))?;

    assert_eq!(
        vec![99, 98, 97],
        top_ids
            .items
            .into_iter()
            .map(|(score, _addr)| score)
            .collect::<Vec<u64>>()
    );

    assert_eq!(
        vec![-99.0, -98.0, -97.0],
        min_rank
            .items
            .into_iter()
            .map(|(score, _addr)| score)
            .collect::<Vec<f64>>()
    );

    // There's more to conditions than booleans and `(T, DocAddress)`,
    // by the way. It's whatever implements the trait
    // `tique::conditional_collector::traits::ConditionForSegment`

    // So let's say we decide to make a pagination feature public
    // but very understandably don't want to expose DocAddress.
    // We can always retrieve a STORED field via a DocAddress,
    // so returning a public id from a search result is easy.

    // For the search part we can do something like this:

    let first_page_collector =
        TopCollector::<f64, Descending, _>::new(PAGE_SIZE, true).top_fast_field(rank_field);

    let page = searcher.search(&AllQuery, &first_page_collector)?;

    let mut result = Vec::new();
    for (score, addr) in page.items.iter() {
        let doc = searcher.doc(*addr)?;
        if let Some(Value::U64(public_id)) = doc.get_first(id_field) {
            result.push((*score, *public_id));
        }
    }

    assert!(page.has_next());
    // So whenever `page.has_next()` is true, `result.last()` will
    // contain the cursor for our next page.
    let (ref_score, ref_id) = *result.last().unwrap();

    // And you can keep paginating beaking even scores via the
    // public id as follows:
    let paginator = move |reader: &SegmentReader| {
        let id_reader = reader
            .fast_fields()
            .u64(id_field)
            .expect("id field is u64 FAST");

        move |_segment_id, doc_id, score, is_ascending: bool| {
            let public_id = id_reader.get(doc_id);

            match ref_score.partial_cmp(&score) {
                Some(Ordering::Greater) => !is_ascending,
                Some(Ordering::Less) => is_ascending,
                Some(Ordering::Equal) => ref_id < public_id,
                None => false,
            }
        }
    };

    let second_page_collector =
        TopCollector::<f64, Descending, _>::new(PAGE_SIZE, paginator).top_fast_field(rank_field);

    let two_pages_collector =
        TopCollector::<f64, Descending, _>::new(PAGE_SIZE * 2, true).top_fast_field(rank_field);

    let (two_pages, second_page) =
        searcher.search(&AllQuery, &(two_pages_collector, second_page_collector))?;

    assert_eq!(two_pages.items[PAGE_SIZE..], second_page.items[..]);

    Ok(())
}
