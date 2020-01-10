use std::{cmp::Ordering, convert::TryFrom, ops::Neg};

use bincode;
use serde::{Deserialize, Serialize};
use tantivy::{
    self,
    fastfield::FastFieldReader,
    query::Query,
    schema::{Field, Schema, SchemaBuilder, Value, FAST, STORED, TEXT},
    DocId, Document, Result, Score, Searcher, SegmentLocalId, SegmentReader, TantivyError,
};

use crate::model::{
    FeaturesAggregationQuery, FeaturesAggregationResult, FeaturesCollector, FeaturesFilterFields,
    Recipe, RecipeId, Sort,
};

use tique::top_collector::{
    fastfield, CheckCondition, ConditionForSegment, ConditionalTopCollector,
    CustomScoreTopCollector, ScorerForSegment, SearchMarker, TweakedScoreTopCollector,
};

#[derive(Clone)]
pub struct RecipeIndex {
    pub id: Field,
    pub fulltext: Field,
    pub features_bincode: Field,
    pub features: FeaturesFilterFields,
}

const FIELD_ID: &str = "id";
const FIELD_FULLTEXT: &str = "fulltext";
const FIELD_FEATURES_BINCODE: &str = "features_bincode";
const INVALID_RECIPE_ID: RecipeId = 0;

impl RecipeIndex {
    pub fn make_document(&self, recipe: &Recipe) -> Document {
        let mut doc = Document::new();
        doc.add_u64(self.id, recipe.recipe_id);

        let mut fulltext = Vec::new();

        fulltext.push(recipe.name.as_str());
        for ingredient in &recipe.ingredients {
            fulltext.push(ingredient.as_str());
        }
        for instruction in &recipe.instructions {
            fulltext.push(instruction.as_str());
        }
        doc.add_text(self.fulltext, fulltext.join("\n").as_str());

        doc.add_bytes(
            self.features_bincode,
            bincode::serialize(&recipe.features).unwrap(),
        );

        self.features.add_to_doc(&mut doc, &recipe.features);
        doc
    }

    fn addresses_to_ids<T>(
        &self,
        searcher: &Searcher,
        addresses: &[SearchMarker<T>],
    ) -> Result<Vec<RecipeId>> {
        let mut items = Vec::with_capacity(addresses.len());

        for addr in addresses.iter() {
            let doc = searcher.doc(addr.doc)?;
            if let Some(&Value::U64(id)) = doc.get_first(self.id) {
                items.push(id);
            } else {
                panic!("Found document without a stored id");
            }
        }

        Ok(items)
    }

    pub fn search(
        &self,
        searcher: &Searcher,
        query: &dyn Query,
        limit: usize,
        sort: Sort,
        ascending: bool,
        after: After,
    ) -> Result<(usize, Vec<RecipeId>, Option<After>)> {
        macro_rules! collect {
            ($topk: ident, $field:ident) => {{
                if ascending {
                    self.$topk(
                        searcher,
                        query,
                        limit,
                        after,
                        fastfield::ascending(self.features.$field),
                    )
                } else {
                    self.$topk(
                        searcher,
                        query,
                        limit,
                        after,
                        fastfield::descending(self.features.$field),
                    )
                }
            }};
        }

        macro_rules! collect_unsigned {
            ($field:ident) => {{
                collect!(topk_u64, $field)
            }};
        }

        macro_rules! collect_float {
            ($field:ident) => {{
                collect!(topk_f64, $field)
            }};
        }

        match sort {
            Sort::Relevance => {
                if ascending {
                    let condition = Paginator::new(self.id, after);
                    let top_collector =
                        TweakedScoreTopCollector::new(limit, condition, |_: &SegmentReader| {
                            |_doc, score: Score| score.neg()
                        });

                    let result = searcher.search(query, &top_collector)?;
                    let items = self.addresses_to_ids(&searcher, &result.items)?;

                    let num_items = items.len();
                    let cursor = if result.visited.saturating_sub(num_items) > 0 {
                        let last_score = result.items[num_items - 1].score;
                        let last_id = items[num_items - 1];
                        Some((last_score, last_id).into())
                    } else {
                        None
                    };

                    Ok((result.total, items, cursor))
                } else {
                    let condition = Paginator::new(self.id, after);
                    let top_collector = ConditionalTopCollector::with_limit(limit, condition);

                    let result = searcher.search(query, &top_collector)?;
                    let items = self.addresses_to_ids(&searcher, &result.items)?;

                    let num_items = items.len();
                    let cursor = if result.visited.saturating_sub(num_items) > 0 {
                        let last_score = result.items[num_items - 1].score;
                        let last_id = items[num_items - 1];
                        Some((last_score, last_id).into())
                    } else {
                        None
                    };

                    Ok((result.total, items, cursor))
                }
            }
            Sort::NumIngredients => collect_unsigned!(num_ingredients),
            Sort::InstructionsLength => collect_unsigned!(instructions_length),
            Sort::TotalTime => collect_unsigned!(total_time),
            Sort::CookTime => collect_unsigned!(cook_time),
            Sort::PrepTime => collect_unsigned!(prep_time),
            Sort::Calories => collect_unsigned!(calories),
            Sort::FatContent => collect_float!(fat_content),
            Sort::CarbContent => collect_float!(carb_content),
            Sort::ProteinContent => collect_float!(protein_content),
        }
    }

    pub fn aggregate_features(
        &self,
        searcher: &Searcher,
        query: &dyn Query,
        agg_query: FeaturesAggregationQuery,
    ) -> Result<FeaturesAggregationResult> {
        let features_field = self.features_bincode;
        let collector = FeaturesCollector::new(agg_query, move |reader: &SegmentReader| {
            let features_reader = reader
                .fast_fields()
                .bytes(features_field)
                .expect("bytes field is indexed");

            move |doc, query, agg| {
                let buf = features_reader.get_bytes(doc);
                let features = bincode::deserialize(buf).unwrap();
                agg.collect(query, &features);
            }
        });

        Ok(searcher.search(query, &collector)?)
    }
}

macro_rules! impl_typed_topk_fn {
    ($name: ident, $type: ty, $paginator: ident) => {
        impl RecipeIndex {
            fn $name<S>(
                &self,
                searcher: &Searcher,
                query: &dyn Query,
                limit: usize,
                after: After,
                scorer: S,
            ) -> Result<(usize, Vec<RecipeId>, Option<After>)>
            where
                S: 'static + ScorerForSegment<$type>,
            {
                let condition = Paginator::$paginator(self.id, after);
                let top_collector = CustomScoreTopCollector::new(limit, condition, scorer);

                let result = searcher.search(query, &top_collector)?;
                let items = self.addresses_to_ids(&searcher, &result.items)?;

                let num_items = items.len();
                let cursor = if result.visited.saturating_sub(num_items) > 0 {
                    let last_score = result.items[num_items - 1].score;
                    let last_id = items[num_items - 1];
                    Some((last_score, last_id).into())
                } else {
                    None
                };

                Ok((result.total, items, cursor))
            }
        }
    };
}

impl_typed_topk_fn!(topk_u64, u64, new_u64);
impl_typed_topk_fn!(topk_f64, f64, new_f64);

impl From<&mut SchemaBuilder> for RecipeIndex {
    fn from(builder: &mut SchemaBuilder) -> Self {
        RecipeIndex {
            id: builder.add_u64_field(FIELD_ID, STORED | FAST),
            fulltext: builder.add_text_field(FIELD_FULLTEXT, TEXT),
            features_bincode: builder.add_bytes_field(FIELD_FEATURES_BINCODE),
            features: FeaturesFilterFields::from(builder),
        }
    }
}

impl TryFrom<&Schema> for RecipeIndex {
    type Error = TantivyError;

    fn try_from(schema: &Schema) -> Result<Self> {
        let id = schema
            .get_field(FIELD_ID)
            .ok_or_else(|| TantivyError::SchemaError(format!("Missing field {}", FIELD_ID)))?;

        let fulltext = schema.get_field(FIELD_FULLTEXT).ok_or_else(|| {
            TantivyError::SchemaError(format!("Missing field {}", FIELD_FULLTEXT))
        })?;

        let features_bincode = schema.get_field(FIELD_FEATURES_BINCODE).ok_or_else(|| {
            TantivyError::SchemaError(format!("Missing field {}", FIELD_FEATURES_BINCODE))
        })?;

        Ok(RecipeIndex {
            id,
            fulltext,
            features_bincode,
            features: FeaturesFilterFields::try_from(schema)?,
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum After {
    Start,
    Relevance(Score, RecipeId),
    F64Field(f64, RecipeId),
    U64Field(u64, RecipeId),
}

impl From<(Score, RecipeId)> for After {
    fn from(src: (Score, RecipeId)) -> Self {
        After::Relevance(src.0, src.1)
    }
}

impl From<(f64, RecipeId)> for After {
    fn from(src: (f64, RecipeId)) -> Self {
        After::F64Field(src.0, src.1)
    }
}

impl From<(u64, RecipeId)> for After {
    fn from(src: (u64, RecipeId)) -> Self {
        After::U64Field(src.0, src.1)
    }
}

#[derive(Clone)]
struct PaginationCondition<T> {
    id_reader: FastFieldReader<RecipeId>,
    is_start: bool,
    ref_id: RecipeId,
    ref_score: T,
}

impl<T> CheckCondition<T> for PaginationCondition<T>
where
    T: 'static + PartialOrd + Clone,
{
    fn check(&self, _sid: SegmentLocalId, doc_id: DocId, score: T) -> bool {
        if self.is_start {
            return true;
        }
        let recipe_id = self.id_reader.get(doc_id);
        match self.ref_score.partial_cmp(&score) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => self.ref_id < recipe_id,
            _ => false,
        }
    }
}

#[derive(Clone)]
struct Paginator<T>(Field, bool, RecipeId, T);

impl Paginator<u64> {
    pub fn new_u64(field: Field, after: After) -> Self {
        match after {
            After::Start => Paginator(field, true, INVALID_RECIPE_ID, 0),
            After::U64Field(score, id) => Paginator(field, false, id, score),
            rest => panic!("Can't handle {:?}", rest),
        }
    }
}

impl Paginator<f64> {
    pub fn new_f64(field: Field, after: After) -> Self {
        match after {
            After::Start => Paginator(field, true, INVALID_RECIPE_ID, 0.0),
            After::F64Field(score, id) => Paginator(field, false, id, score),
            rest => panic!("Can't handle {:?}", rest),
        }
    }
}

impl Paginator<f32> {
    pub fn new(field: Field, after: After) -> Self {
        match after {
            After::Start => Paginator(field, true, INVALID_RECIPE_ID, 0.0),
            After::Relevance(score, id) => Paginator(field, false, id, score),
            rest => panic!("Can't handle {:?}", rest),
        }
    }
}

impl<T> ConditionForSegment<T> for Paginator<T>
where
    T: 'static + PartialOrd + Copy,
{
    type Type = PaginationCondition<T>;

    fn for_segment(&self, reader: &SegmentReader) -> Self::Type {
        let id_reader = reader
            .fast_fields()
            .u64(self.0)
            .expect("id field is indexed with the FAST flag");

        PaginationCondition {
            id_reader,
            is_start: self.1,
            ref_id: self.2,
            ref_score: self.3,
        }
    }
}
