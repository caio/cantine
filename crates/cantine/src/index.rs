use std::{cmp::Ordering, convert::TryFrom, path::Path};

use bincode;
use tantivy::{
    self,
    query::{AllQuery, BooleanQuery, Occur, Query},
    schema::{Field, Schema, SchemaBuilder, Value, FAST, STORED, TEXT},
    tokenizer::TokenizerManager,
    DocId, Document, Index, IndexReader, Result, Searcher, SegmentReader, TantivyError,
};

use crate::model::{
    FeaturesAggregationQuery, FeaturesAggregationResult, FeaturesCollector, FeaturesFilterFields,
    Recipe, SearchCursor, SearchQuery, Sort,
};

use tique::{
    queryparser::QueryParser,
    top_collector::{ordered_by_u64_fast_field, ConditionalTopCollector, SearchMarker},
};

#[derive(Clone)]
pub struct IndexFields {
    pub id: Field,
    pub fulltext: Field,
    pub features_bincode: Field,
    pub features: FeaturesFilterFields,
}

const FIELD_ID: &str = "id";
const FIELD_FULLTEXT: &str = "fulltext";
const FIELD_FEATURES_BINCODE: &str = "features_bincode";

impl IndexFields {
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
}

impl From<&mut SchemaBuilder> for IndexFields {
    fn from(builder: &mut SchemaBuilder) -> Self {
        IndexFields {
            id: builder.add_u64_field(FIELD_ID, STORED | FAST),
            fulltext: builder.add_text_field(FIELD_FULLTEXT, TEXT),
            features_bincode: builder.add_bytes_field(FIELD_FEATURES_BINCODE),
            features: FeaturesFilterFields::from(builder),
        }
    }
}

impl TryFrom<&Schema> for IndexFields {
    // TODO better error
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

        Ok(IndexFields {
            id,
            fulltext,
            features_bincode,
            features: FeaturesFilterFields::try_from(schema)?,
        })
    }
}

pub struct Cantine {
    reader: IndexReader,
    fields: IndexFields,
    query_parser: QueryParser,
}

pub type CantineSearchResult = (
    usize,
    Vec<u64>,
    Option<SearchCursor>,
    Option<FeaturesAggregationResult>,
);

impl Cantine {
    pub fn open<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let index = Index::open_in_dir(base_path.as_ref())?;

        let fields = IndexFields::try_from(&index.schema()).unwrap();
        let reader = index.reader()?;

        let query_parser = QueryParser::new(
            fields.fulltext,
            TokenizerManager::default().get("en_stem").unwrap(),
            true,
        );

        Ok(Self {
            fields,
            reader,
            query_parser,
        })
    }

    fn interpret_query(&self, query: &SearchQuery) -> Result<Box<dyn Query>> {
        let mut subqueries: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(fulltext) = &query.fulltext {
            if let Some(parsed) = self.query_parser.parse(fulltext.as_str())? {
                subqueries.push((Occur::Must, parsed));
            }
        }

        if let Some(filter) = &query.filter {
            for query in self.fields.features.interpret(filter).into_iter() {
                subqueries.push((Occur::Must, query));
            }
        }

        match subqueries.len() {
            0 => Ok(Box::new(AllQuery)),
            1 => Ok(subqueries.pop().expect("length has been checked").1),
            _ => Ok(Box::new(BooleanQuery::from(subqueries))),
        }
    }

    fn addresses_to_ids<T>(
        &self,
        searcher: &Searcher,
        addresses: &[SearchMarker<T>],
    ) -> Result<Vec<u64>> {
        let mut items = Vec::with_capacity(addresses.len());

        for addr in addresses.iter() {
            let doc = searcher.doc(addr.doc)?;
            if let Some(&Value::U64(id)) = doc.get_first(self.fields.id) {
                items.push(id);
            } else {
                panic!("Found document without a stored id");
            }
        }

        Ok(items)
    }

    fn basic_search(
        &self,
        searcher: &Searcher,
        interpreted_query: &dyn Query,
        limit: usize,
        sort: Sort,
        after: SearchCursor,
    ) -> Result<(usize, Vec<u64>, Option<SearchCursor>)> {
        macro_rules! condition_from_score {
            ($score:expr) => {{
                let after_score = $score;
                let after_id = after.recipe_id();
                let is_start = after.is_start();

                let id_field = self.fields.id;
                move |reader: &SegmentReader| {
                    let id_reader = reader
                        .fast_fields()
                        .u64(id_field)
                        .expect("id field is indexed with the FAST flag");

                    move |_segment_id, doc_id, score| {
                        if is_start {
                            return true;
                        }

                        let recipe_id = id_reader.get(doc_id);
                        match after_score.partial_cmp(&score) {
                            Some(Ordering::Greater) => true,
                            Some(Ordering::Equal) => after_id < recipe_id,
                            _ => false,
                        }
                    }
                }
            }};
        }

        macro_rules! collect_unsigned {
            ($field:ident) => {{
                let condition = condition_from_score!(after.score());
                let top_collector =
                    ordered_by_u64_fast_field(self.fields.features.$field, limit, condition);

                let result = searcher.search(interpreted_query, &top_collector)?;
                let items = self.addresses_to_ids(&searcher, &result.items)?;

                let num_items = items.len();
                let cursor = if result.visited.saturating_sub(num_items) > 0 {
                    let last_score = result.items[num_items - 1].score;
                    let last_id = items[num_items - 1];
                    Some(SearchCursor::new(last_score, last_id))
                } else {
                    None
                };

                Ok((result.total, items, cursor))
            }};
        }

        match sort {
            Sort::Relevance => {
                let condition = condition_from_score!(after.score_f32());
                let top_collector = ConditionalTopCollector::with_limit(limit, condition);

                let result = searcher.search(interpreted_query, &top_collector)?;
                let items = self.addresses_to_ids(&searcher, &result.items)?;

                let num_items = items.len();
                let cursor = if result.visited.saturating_sub(num_items) > 0 {
                    let last_score = result.items[num_items - 1].score;
                    let last_id = items[num_items - 1];
                    Some(SearchCursor::from_f32(last_score, last_id))
                } else {
                    None
                };

                Ok((result.total, items, cursor))
            }
            Sort::Calories => collect_unsigned!(calories),
            Sort::NumIngredients => collect_unsigned!(num_ingredients),
            Sort::InstructionsLength => collect_unsigned!(instructions_length),
            Sort::TotalTime => collect_unsigned!(total_time),
            Sort::CookTime => collect_unsigned!(cook_time),
            Sort::PrepTime => collect_unsigned!(prep_time),
            _ => unimplemented!(),
        }
    }

    fn compute_aggregations(
        &self,
        searcher: &Searcher,
        interpreted_query: &dyn Query,
        agg_query: FeaturesAggregationQuery,
    ) -> Result<FeaturesAggregationResult> {
        let features_field = self.fields.features_bincode;
        let collector = FeaturesCollector::new(agg_query, move |reader: &SegmentReader| {
            let features_reader = reader
                .fast_fields()
                .bytes(features_field)
                .expect("bytes field is indexed");

            move |doc: DocId| {
                let buf = features_reader.get_bytes(doc);
                bincode::deserialize(buf).unwrap()
            }
        });

        Ok(searcher.search(interpreted_query, &collector)?)
    }

    pub fn search(
        &self,
        query: SearchQuery,
        agg_threshold: Option<usize>,
    ) -> Result<CantineSearchResult> {
        let searcher = self.reader.searcher();

        let interpreted_query = self.interpret_query(&query)?;
        let limit = query.num_items.unwrap_or(10) as usize;

        let (total_found, items, after) = self.basic_search(
            &searcher,
            &interpreted_query,
            limit,
            query.sort.unwrap_or(Sort::Relevance),
            query.after.unwrap_or(SearchCursor::START),
        )?;

        let agg = if let Some(agg_query) = query.agg {
            if total_found <= agg_threshold.unwrap_or(std::usize::MAX) {
                Some(self.compute_aggregations(&searcher, &interpreted_query, agg_query)?)
            } else {
                None
            }
        } else {
            None
        };

        Ok((total_found, items, after, agg))
    }
}
