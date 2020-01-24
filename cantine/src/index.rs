use std::{cmp::Ordering, convert::TryFrom};

use bincode;
use serde::{Deserialize, Serialize};
use tantivy::{
    self,
    collector::Collector,
    fastfield::FastFieldReader,
    query::Query,
    schema::{Field, Schema, SchemaBuilder, Value, FAST, STORED, TEXT},
    DocId, Document, Result, Score, Searcher, SegmentLocalId, SegmentReader, TantivyError,
};

use crate::model::{
    FeaturesAggregationQuery, FeaturesAggregationResult, FeaturesCollector, FeaturesFilterFields,
    Recipe, RecipeId, Sort,
};

use tique::conditional_collector::{
    traits::{CheckCondition, ConditionForSegment},
    Ascending, CollectionResult, Descending, TopCollector,
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

    pub fn search(
        &self,
        searcher: &Searcher,
        query: &dyn Query,
        limit: usize,
        sort: Sort,
        after: Option<After>,
    ) -> Result<(usize, Vec<RecipeId>, Option<After>)> {
        macro_rules! collect {
            ($type: ty, $field:ident, $order:ident) => {
                if let Some(after) = after {
                    let top_collector =
                        TopCollector::<$type, $order, _>::new(limit, after.as_paginator(self.id))
                            .top_fast_field(self.features.$field);

                    self.render::<$type, _>(&searcher, query, top_collector)
                } else {
                    let top_collector = TopCollector::<$type, $order, _>::new(limit, true)
                        .top_fast_field(self.features.$field);

                    self.render::<$type, _>(&searcher, query, top_collector)
                }
            };

            ($order:ident) => {
                if let Some(after) = after {
                    let top_collector =
                        TopCollector::<_, $order, _>::new(limit, after.as_paginator(self.id));

                    self.render::<Score, _>(&searcher, query, top_collector)
                } else {
                    let top_collector = TopCollector::<_, $order, _>::new(limit, true);

                    self.render::<Score, _>(&searcher, query, top_collector)
                }
            };
        }

        match sort {
            Sort::Relevance => collect!(Descending),
            Sort::RelevanceAsc => collect!(Ascending),
            Sort::NumIngredients => collect!(u64, num_ingredients, Descending),
            Sort::InstructionsLength => collect!(u64, instructions_length, Descending),
            Sort::TotalTime => collect!(u64, total_time, Descending),
            Sort::CookTime => collect!(u64, cook_time, Descending),
            Sort::PrepTime => collect!(u64, prep_time, Descending),
            Sort::Calories => collect!(u64, calories, Descending),
            Sort::FatContent => collect!(f64, fat_content, Descending),
            Sort::CarbContent => collect!(f64, carb_content, Descending),
            Sort::ProteinContent => collect!(f64, protein_content, Descending),
            Sort::NumIngredientsAsc => collect!(u64, num_ingredients, Ascending),
            Sort::InstructionsLengthAsc => collect!(u64, instructions_length, Ascending),
            Sort::TotalTimeAsc => collect!(u64, total_time, Ascending),
            Sort::CookTimeAsc => collect!(u64, cook_time, Ascending),
            Sort::PrepTimeAsc => collect!(u64, prep_time, Ascending),
            Sort::CaloriesAsc => collect!(u64, calories, Ascending),
            Sort::FatContentAsc => collect!(f64, fat_content, Ascending),
            Sort::CarbContentAsc => collect!(f64, carb_content, Ascending),
            Sort::ProteinContentAsc => collect!(f64, protein_content, Ascending),
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

    fn render<T, C>(
        &self,
        searcher: &Searcher,
        query: &dyn Query,
        collector: C,
    ) -> Result<(usize, Vec<RecipeId>, Option<After>)>
    where
        T: 'static + Sync + Send + Copy + AsAfter,
        C: Collector<Fruit = CollectionResult<T>>,
    {
        let result = searcher.search(query, &collector)?;
        let mut recipe_ids = Vec::with_capacity(result.items.len());

        let has_next = result.has_next();
        let last = result
            .items
            .into_iter()
            .flat_map(|(score, addr)| {
                searcher.doc(addr).map(|doc| {
                    if let Some(&Value::U64(recipe_id)) = doc.get_first(self.id) {
                        recipe_ids.push(recipe_id);
                        (score, recipe_id)
                    } else {
                        panic!("Found doc with non-U64 id field");
                    }
                })
            })
            .last();

        let cursor = if has_next {
            last.map(|(score, id)| score.as_after(id))
        } else {
            None
        };

        Ok((result.total, recipe_ids, cursor))
    }
}

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

pub trait AsAfter {
    fn as_after(self, id: RecipeId) -> After;
}

impl AsAfter for u64 {
    fn as_after(self, id: RecipeId) -> After {
        (self, id).into()
    }
}

impl AsAfter for f64 {
    fn as_after(self, id: RecipeId) -> After {
        (self, id).into()
    }
}

impl AsAfter for f32 {
    fn as_after(self, id: RecipeId) -> After {
        (self, id).into()
    }
}

#[derive(Clone)]
pub struct PaginationCondition<T> {
    id_reader: FastFieldReader<RecipeId>,
    ref_id: RecipeId,
    ref_score: T,
}

impl<T> CheckCondition<T> for PaginationCondition<T>
where
    T: 'static + PartialOrd + Clone,
{
    fn check(&self, _sid: SegmentLocalId, doc_id: DocId, score: T, ascending: bool) -> bool {
        let recipe_id = self.id_reader.get(doc_id);
        match self.ref_score.partial_cmp(&score) {
            Some(Ordering::Greater) => !ascending,
            Some(Ordering::Less) => ascending,
            Some(Ordering::Equal) => self.ref_id < recipe_id,
            None => false,
        }
    }
}

#[derive(Clone)]
pub struct Paginator<T>(Field, bool, RecipeId, T);

impl Paginator<u64> {
    pub fn new_u64(field: Field, after: After) -> Self {
        match after {
            After::U64Field(score, id) => Paginator(field, false, id, score),
            rest => panic!("Can't handle {:?}", rest),
        }
    }
}

impl Paginator<f64> {
    pub fn new_f64(field: Field, after: After) -> Self {
        match after {
            After::F64Field(score, id) => Paginator(field, false, id, score),
            rest => panic!("Can't handle {:?}", rest),
        }
    }
}

impl Paginator<f32> {
    pub fn new(field: Field, after: After) -> Self {
        match after {
            After::Relevance(score, id) => Paginator(field, false, id, score),
            rest => panic!("Can't handle {:?}", rest),
        }
    }
}

pub trait AsPaginator<T> {
    fn as_paginator(self, field: Field) -> Paginator<T>;
}

impl AsPaginator<u64> for After {
    fn as_paginator(self, field: Field) -> Paginator<u64> {
        Paginator::new_u64(field, self)
    }
}

impl AsPaginator<f64> for After {
    fn as_paginator(self, field: Field) -> Paginator<f64> {
        Paginator::new_f64(field, self)
    }
}

impl AsPaginator<f32> for After {
    fn as_paginator(self, field: Field) -> Paginator<f32> {
        Paginator::new(field, self)
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
            ref_id: self.2,
            ref_score: self.3,
        }
    }
}
