mod top_collector;
mod topk;
mod traits;

pub use top_collector::{CollectionResult, TopCollector, TopSegmentCollector};
pub use topk::{Ascending, Descending};
pub use traits::{CheckCondition, ConditionForSegment};
