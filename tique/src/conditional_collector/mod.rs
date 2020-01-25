//! Top-K Collector, with ordering and condition support.
//!
//! # Tutorial
//!
//! ```rust
//! use std::{cmp::Ordering, ops::Neg};
//!
//! use tantivy::{
//!     collector::TopDocs,
//!     query::AllQuery,
//!     schema::{Field, Value, SchemaBuilder, FAST, STORED},
//!     Index, SegmentReader, Document, Score
//! };
//!
//! use tique::conditional_collector::{
//!     TopCollector,
//!     Ascending, Descending
//! };
//!
//! const NUM_DOCS: i32 = 100;
//! const K: usize = 10;
//!
//! // Let's create a collector that behaves like tantivy's `TopCollector`
//! // The first `_` is `Score`, but it can be inferred:
//! let tique_collector =
//!     TopCollector::<_, Descending, _>::new(K, true);
//!
//! // Let's double check :)
//! let tantivy_collector = TopDocs::with_limit(K);
//!
//! // Now let's create a simple test index
//! let mut builder = SchemaBuilder::new();
//! let rank_field = builder.add_f64_field("rank", FAST);
//! let id_field = builder.add_u64_field("id_field", FAST | STORED);
//!
//! let index = Index::create_in_ram(builder.build());
//! let mut writer = index.writer_with_num_threads(1, 3_000_000)?;
//!
//! for i in 0..NUM_DOCS {
//!     let mut doc = Document::new();
//!     doc.add_f64(rank_field, f64::from(i.neg()));
//!     doc.add_u64(id_field, i as u64);
//!     writer.add_document(doc);
//! }
//!
//! writer.commit()?;
//!
//! // Now let's search our index
//! let reader = index.reader()?;
//! let searcher = reader.searcher();
//!
//! let (tantivy_top, tique_top) = searcher.search(
//!     &AllQuery, &(tantivy_collector, tique_collector))?;
//!
//! assert_eq!(tantivy_top.len(), tique_top.items.len());
//! // Phew!
//!
//! // Noticed that we checked against `tique_top.items`? It's because
//! // tique's collectors come with some extra metadata to make it more
//! // useful.
//!
//! // We know how many documents matched the *query*, (not
//! // necessarily the range), just like a count collector would.
//! // So we expect it to be the number of documents in the index
//! // given our query.
//! assert_eq!(NUM_DOCS as usize, tique_top.total);
//!
//! // We also know if there would have been more items if we
//! // asked for:
//! assert!(tique_top.has_next());
//!
//! // This in useful information because it tells us that
//! // we can keep searching easily.
//!
//! // One simple way to get the next page is to ask for more
//! // results and shift. It's a super fast way that can become
//! // problematic for digging deep into very large indices.
//! let tantivy_next_collector = TopDocs::with_limit(K * 2);
//!
//! // The `tique::conditional_collector` collections know how
//! // to paginate based on their own results, which allows you
//! // to keep memory stable while spending more CPU time doing
//! // comparisons:
//!
//! let last_result = tique_top.items.into_iter().last().unwrap();
//! let tique_next_collector = TopCollector::<_, Descending, _>::new(K, last_result);
//!
//! // One disadvantage of this approach is that you can't simply
//! // skip to an arbitrary page. When that's a requirement, the
//! // best idea is to use the "memory hungry" approach until a
//! // certain threshold, then switch to cursor-based.
//! // You can even use tantivy's result to paginate:
//!
//! let last_tantivy_result = tantivy_top.into_iter().last().unwrap();
//! let tique_next_collector_via_tantivy =
//!     TopCollector::<_, Descending, _>::new(K, last_tantivy_result);
//!
//! let (tantivy_until_next, tique_next, tique_same_next) = searcher.search(
//!     &AllQuery,
//!     &(tantivy_next_collector,tique_next_collector, tique_next_collector_via_tantivy))?;
//!
//! assert_eq!(tique_next.items, tique_same_next.items);
//! assert_eq!(tantivy_until_next[K..], tique_next.items[..]);
//!
//! // We can also sort by the fast fields we indexed:
//!
//! let min_rank_collector =
//!     TopCollector::<f64, Ascending, _>::new(3, true)
//!         .top_fast_field(rank_field);
//!
//! let top_ids_collector =
//!     TopCollector::<u64, Descending, _>::new(3, true)
//!         .top_fast_field(id_field);
//!
//! let (min_rank, top_ids) =
//!     searcher.search(&AllQuery, &(min_rank_collector, top_ids_collector))?;
//!
//! assert_eq!(
//!     vec![99, 98, 97],
//!     top_ids.items.into_iter().map(|(score, _addr)| score).collect::<Vec<u64>>()
//! );
//!
//! assert_eq!(
//!     vec![-99.0, -98.0, -97.0],
//!     min_rank.items.into_iter().map(|(score, _addr)| score).collect::<Vec<f64>>()
//! );
//!
//! // There's more to conditions than booleans and `(T, DocAddress)`,
//! // by the way. It's whatever implements the trait
//! // `tique::conditional_collector::traits::ConditionForSegment`
//!
//! // So let's say we decide to make a pagination feature public
//! // but very understandably don't want to expose DocAddress.
//! // We can always retrieve a STORED field via a DocAddress,
//! // so returning a public id from a search result is easy.
//!
//! // For the search part we can do something like this:
//!
//! const PAGE_SIZE: usize = 15;
//! let first_page_collector =
//!     TopCollector::<f64, Descending, _>::new(PAGE_SIZE, true)
//!         .top_fast_field(rank_field);
//!
//! let page = searcher.search(&AllQuery, &first_page_collector)?;
//!
//! let mut result : Vec<(f64, u64)> = Vec::new();
//! for (score, addr) in page.items.iter() {
//!     let doc = searcher.doc(*addr)?;
//!     if let Some(&Value::U64(public_id)) = doc.get_first(id_field) {
//!         result.push((*score, public_id));
//!     }
//! }
//!
//! // So whenever `page.has_next()` is true, `result.last()` will
//! // contain the cursor for our next page.
//! assert!(page.has_next());
//! let (ref_score, ref_id) = *result.last().unwrap();
//!
//! // And you can keep paginating beaking even via the
//! // public id as follows:
//!
//! let paginator = move |reader: &SegmentReader| {
//!     let id_reader = reader.fast_fields().u64(id_field)
//!         .expect("id field is u64 FAST");
//!
//!     move |_segment_id, doc_id, score, is_ascending: bool| {
//!         let public_id = id_reader.get(doc_id);
//!
//!         match ref_score.partial_cmp(&score) {
//!             Some(Ordering::Greater) => !is_ascending,
//!             Some(Ordering::Less) => is_ascending,
//!             Some(Ordering::Equal) => ref_id < public_id,
//!             None => false,
//!         }
//!     }
//! };
//!
//! let second_page_collector =
//!     TopCollector::<f64, Descending, _>::new(PAGE_SIZE, paginator)
//!         .top_fast_field(rank_field);
//!
//! let two_pages_collector =
//!     TopCollector::<f64, Descending, _>::new(PAGE_SIZE * 2, true)
//!         .top_fast_field(rank_field);
//!
//! let (two_pages, second_page) = searcher.search(
//!     &AllQuery,
//!     &(two_pages_collector, second_page_collector))?;
//!
//! assert_eq!(two_pages.items[PAGE_SIZE..], second_page.items[..]);
//!
//! # Ok::<(), tantivy::Error>(())
//! ```
mod custom_score;
mod top_collector;
mod topk;

pub mod traits;

pub use custom_score::CustomScoreTopCollector;
pub use top_collector::{CollectionResult, TopCollector};
pub use topk::{Ascending, Descending};
