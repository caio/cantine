//! Top-K Collectors, with ordering and condition support.
//!
//! This is a collection of collectors that provide top docs
//! rank functionality very similar to `tantivy::TopDocs`, with
//! added support for declaring the ordering (ascending or
//! descending) and collection-time conditions.
//!
//! ```rust
//! # use tique::conditional_collector::{Descending,TopCollector};
//! # use tantivy::Score;
//! # let condition_for_segment = true;
//! let collector =
//!     TopCollector::<Score, Descending, _>::new(10, condition_for_segment);
//! ```
//!
//! NOTE: Usually the score type (`Score` above, a `f32`) is inferred
//! so there's no need to specify it.
//!
//! # Ordering Support
//!
//! When constructing a top collector you *must* specify how to
//! actually order the items: in ascending or descending order.
//!
//! You simply choose `Ascending` or `Descending` and let the
//! compiler know:
//!
//! ```rust
//! # use tique::conditional_collector::{Ascending,TopCollector};
//! # use tantivy::Score;
//! # let limit = 10;
//! # let condition_for_segment = true;
//! let collector =
//!     TopCollector::<Score, Ascending, _>::new(limit, condition_for_segment);
//! ```
//!
//! # Condition Support
//!
//! A "condition" is simply a way to tell the collector that
//! a document is a valid candidate to the top. It behaves
//! just like a query filter would, but does not limit the
//! candidates before the collector sees them.
//!
//! This is a valid condition that accepts everything:
//!
//! ```rust
//! let condition_for_segment = true;
//! ```
//!
//! Generally speaking, a `condition` is anything that implements
//! the `ConditionForSegment` trait and you can use closures as a
//! shortcut:
//!
//! ```rust
//! # use tantivy::{Score,SegmentReader};
//! # use tique::conditional_collector::{TopCollector,Ascending};
//! # let limit = 10;
//! let condition_for_segment = move |reader: &SegmentReader| {
//!     // Fetch useful stuff from the `reader`, then:
//!     move |segment_id, doc_id, score, is_ascending| {
//!         // Express whatever logic you want
//!         true
//!     }
//! };
//!
//! let collector =
//!     TopCollector::<Score, Ascending, _>::new(limit, condition_for_segment);
//! ```
//!
//! ## Aside: Pagination with Constant Memory
//!
//! If you've been using `tantivy` for a while, you're probably
//! used to seeing tuples like `(T, DocAddress)` (T is usually
//! `tantivy::Score`, but changes if you customize the score
//! somehow).
//!
//! You can also use these tuples as a condition and they act
//! like a cursor for pagination, so when you do something like:
//!
//! ```rust
//! # use tantivy::DocAddress;
//! # use tique::conditional_collector::{TopCollector,Descending};
//! let limit = 10;
//! let condition_for_segment = (0.42, DocAddress(0, 1));
//! let collector =
//!     TopCollector::<_, Descending, _>::new(limit, condition_for_segment);
//! ```
//!
//! What you are asking for is the top `limit` documents that appear
//! *after* (because you chose the `Descending` order) documents
//! that scored `0.42` at whatever query you throw at it (and in
//! case multiple docs score the name, the collector knows to
//! break even by the `DocAddress`).
//!
//! The results that you get after your search will contain more
//! `(T, DocAddress)` tuples you can use to keep pagination
//! going without ever having to increase `limit`.
//!
//! Check `examples/conditional_collector_tutorial.rs` for more details.
mod custom_score;
mod top_collector;
mod topk;

pub mod traits;

pub use custom_score::CustomScoreTopCollector;
pub use top_collector::{CollectionResult, TopCollector};
pub use topk::{Ascending, Descending};
