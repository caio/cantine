use std::{convert::TryFrom, num::NonZeroUsize, path::PathBuf, sync::Arc};

use structopt::StructOpt;
use tantivy::{
    self,
    collector::{Collector, SegmentCollector},
    fastfield::FastFieldReader,
    schema::{Field, Value},
    tokenizer::TokenizerManager,
    DocId, Index, Score, SegmentLocalId, SegmentReader,
};

use cantine::{
    database::BincodeDatabase,
    index::IndexFields,
    model::{FeaturesAggregationQuery, FeaturesAggregationResult, Recipe, RecipeCard},
};
use tique::queryparser::QueryParser;
use tique::top_collector::ConditionalTopCollector;

/// Queries data generated via `load`
#[derive(Debug, StructOpt)]
#[structopt(name = "query")]
pub struct QueryOptions {
    /// Maximum number of recipes to retrieve
    #[structopt(short, long, default_value = "3")]
    num_results: NonZeroUsize,
    /// Path to the data directory that will be queries
    #[structopt(short, long)]
    base_path: PathBuf,

    /// What to search for
    query: String,
}

// XXX With some cerimony this can be made reusable:
//  1. A Feature reader trait to map <doc_id, segment_id> to a Feature
//  2. A Feature trait that points at the generated aggregation
//     query/result structs
pub struct FeatureCollector {
    query: FeaturesAggregationQuery,
    db: Arc<BincodeDatabase<Recipe>>,
    id_field: Field,
}

impl Collector for FeatureCollector {
    type Fruit = FeaturesAggregationResult;
    type Child = FeatureSegmentCollector;
    fn for_segment(
        &self,
        _segment_id: SegmentLocalId,
        reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let agg = FeaturesAggregationResult::from(&self.query);

        let id_reader = reader
            .fast_fields()
            .u64(self.id_field)
            .expect("id_field is u64 fast field");

        Ok(FeatureSegmentCollector {
            agg,
            id_reader,
            query: self.query.clone(),
            db: self.db.clone(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        assert!(!fruits.is_empty());

        let mut iter = fruits.into_iter();
        let mut first: FeaturesAggregationResult = iter.next().expect("fruits is never empty");

        for fruit in iter {
            first.merge_same_size(&fruit);
        }

        Ok(first)
    }
}

pub struct FeatureSegmentCollector {
    query: FeaturesAggregationQuery,
    agg: FeaturesAggregationResult,
    db: Arc<BincodeDatabase<Recipe>>,
    id_reader: FastFieldReader<u64>,
}

impl SegmentCollector for FeatureSegmentCollector {
    type Fruit = FeaturesAggregationResult;

    fn collect(&mut self, doc: DocId, _score: Score) {
        let id = self.id_reader.get(doc);
        let recipe = self.db.get_by_id(id).unwrap().unwrap();

        self.agg.collect(&self.query, &recipe.features);
    }

    fn harvest(self) -> Self::Fruit {
        self.agg
    }
}

pub fn main() -> Result<(), String> {
    let options = QueryOptions::from_args();
    println!("Started with {:?}", options);

    let index = Index::open_in_dir(options.base_path.join("tantivy")).unwrap();
    let fields = IndexFields::try_from(&index.schema())?;

    let parser = QueryParser::new(
        fields.fulltext,
        TokenizerManager::default().get("en_stem").unwrap(),
        true,
    );

    let reader = index.reader().unwrap();
    let searcher = reader.searcher();

    let db: Arc<BincodeDatabase<Recipe>> =
        Arc::new(BincodeDatabase::open(options.base_path.join("database")).unwrap());

    let agg_query = FeaturesAggregationQuery {
        num_ingredients: vec![0..6, 5..11, 10..std::u8::MAX],
        cook_time: vec![0..30, 30..120],
        diet_lowcarb: vec![0.5..0.75, 0.75..0.85, 0.85..0.999, 1.0..std::f32::MAX],
        ..FeaturesAggregationQuery::default()
    };

    let feat_collector = FeatureCollector {
        id_field: fields.id,
        db: db.clone(),
        query: agg_query,
    };

    let (topdocs, agg_result) = searcher
        .search(
            // FIXME errors
            &parser.parse(&options.query.as_str()).unwrap().unwrap(),
            &(
                ConditionalTopCollector::with_limit(options.num_results.get(), true),
                feat_collector,
            ),
        )
        .unwrap();

    let mut recipes: Vec<RecipeCard> = Vec::new();
    for item in topdocs.iter() {
        let doc = searcher.doc(item.doc).unwrap();
        if let Some(&Value::U64(id)) = doc.get_first(fields.id) {
            recipes.push(db.get_by_id(id).unwrap().unwrap().into());
        } else {
            return Err(format!("Found doc without id: {:?}", doc));
        }
    }

    dbg!(recipes);
    dbg!(agg_result);

    Ok(())
}
