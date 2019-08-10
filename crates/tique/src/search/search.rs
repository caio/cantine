use std::path::Path;

use serde::{Deserialize, Serialize};
use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery},
    schema::{
        Field, FieldType, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
        Value, FAST, INDEXED, STORED,
    },
    tokenizer::TokenizerManager,
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

use super::collector::FeatureCollector;
use super::features::{Feature, FeatureVector};
use super::model::{Range, SearchQuery};
use super::query_parser::QueryParser;

use super::Result;

pub struct RecipeIndex {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    query_parser: QueryParser,
    fields: FeatureIndexFields,
}

#[derive(Clone)]
struct FeatureIndexFields(Vec<Field>);

impl FeatureIndexFields {
    fn new() -> (Schema, FeatureIndexFields) {
        let mut builder = SchemaBuilder::new();
        let mut fields = Vec::with_capacity(3 + Feature::LENGTH);

        let indexing = TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_field_options = TextOptions::default().set_indexing_options(indexing);

        // 3 core fields
        fields.push(builder.add_u64_field("id", FAST | STORED));
        fields.push(builder.add_text_field("ft", text_field_options));
        fields.push(builder.add_bytes_field("fv"));

        // Feature::LENGTH extra ones, for filtering
        for feat in Feature::VALUES.iter() {
            fields.push(
                builder.add_u64_field(format!("feature_{}", (*feat as usize)).as_str(), INDEXED),
            );
        }

        (builder.build(), FeatureIndexFields(fields))
    }

    fn must_get(&self, idx: usize) -> Field {
        let FeatureIndexFields(inner) = self;
        *inner.get(idx).expect("every get works")
    }

    fn id(&self) -> Field {
        self.must_get(0)
    }

    fn fulltext(&self) -> Field {
        self.must_get(1)
    }

    fn feature_vector(&self) -> Field {
        self.must_get(2)
    }

    fn feature(&self, feat: Feature) -> Field {
        self.must_get(3 + feat as usize)
    }

    fn interpret_query(
        &self,
        query: &SearchQuery,
        query_parser: &QueryParser,
    ) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(metadata) = &query.metadata {
            for (feat, range) in metadata {
                clauses.push((
                    Occur::Must,
                    Box::new(RangeQuery::new_u64(self.feature(*feat), range.into())),
                ));
            }
        }

        if let Some(fulltext) = &query.fulltext {
            if let Some(boxed_query) = query_parser.parse(fulltext.as_ref())? {
                if clauses.len() == 0 {
                    return Ok(boxed_query);
                } else {
                    clauses.push((Occur::Must, boxed_query))
                }
            }
        }

        if clauses.len() == 0 {
            Ok(Box::new(AllQuery))
        } else {
            let bq: BooleanQuery = clauses.into();
            Ok(Box::new(bq))
        }
    }
}

pub struct FeatureDocument(Document);

pub trait FeatureDocumentFactory {
    fn make_document(
        &self,
        id: u64,
        fulltext: String,
        features: Option<Vec<(Feature, u16)>>,
    ) -> FeatureDocument;
}

impl FeatureDocumentFactory for FeatureIndexFields {
    fn make_document(
        &self,
        id: u64,
        fulltext: String,
        features: Option<Vec<(Feature, u16)>>,
    ) -> FeatureDocument {
        let mut doc = Document::default();

        doc.add_u64(self.id(), id);
        doc.add_text(self.fulltext(), fulltext.as_str());

        features.map(|feats| {
            let mut buf = Feature::EMPTY_BUFFER.to_vec();
            let mut fv = FeatureVector::parse(buf.as_mut_slice()).unwrap();
            for (feat, value) in feats {
                fv.set(&feat, value);

                doc.add_u64(self.feature(feat), value as u64);
            }
            doc.add_bytes(self.feature_vector(), fv.as_bytes().into());
        });

        FeatureDocument(doc)
    }
}

impl RecipeIndex {
    pub fn new(index_path: &Path) -> Result<RecipeIndex> {
        let (schema, fields) = FeatureIndexFields::new();

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, schema)?;
        let writer = index.writer(10_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let tokenizer = TokenizerManager::default()
            .get("en_stem")
            .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))?;

        let query_parser = QueryParser::new(fields.fulltext(), tokenizer);

        Ok(RecipeIndex {
            index,
            writer,
            reader,
            query_parser,
            fields,
        })
    }

    pub fn doc_factory(&self) -> impl FeatureDocumentFactory {
        self.fields.clone()
    }

    pub fn add(&self, feature_document: FeatureDocument) {
        let FeatureDocument(doc) = feature_document;
        self.writer.add_document(doc);
    }

    pub fn search(&self, query: &SearchQuery) -> Result<Vec<u64>> {
        let searcher = self.reader.searcher();
        let iquery = self.fields.interpret_query(query, &self.query_parser)?;

        let hits = searcher.search(&iquery, &TopDocs::with_limit(10))?;
        let mut ids = Vec::with_capacity(hits.len());

        for (_score, addr) in hits {
            ids.push(
                searcher
                    .doc(addr)?
                    .get_first(self.fields.id())
                    .expect("Found document without an id field")
                    .u64_value(),
            );
        }

        Ok(ids)
    }

    #[cfg(test)]
    fn reload_searchers(&self) -> Result<()> {
        self.reader.reload()
    }

    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    pub fn commit(&mut self) -> Result<()> {
        self.writer.commit()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    #[test]
    fn can_create_ok() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        RecipeIndex::new(tmpdir.path())?;
        Ok(())
    }

    #[test]
    fn can_commit_after_create() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut searcher = RecipeIndex::new(tmpdir.path())?;
        searcher.commit()?;
        Ok(())
    }

    #[test]
    fn num_docs_increases() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut index = RecipeIndex::new(tmpdir.path())?;

        assert_eq!(0, index.num_docs());

        index.add(index.doc_factory().make_document(1, "one".to_owned(), None));

        index.commit()?;
        index.reload_searchers()?;

        assert_eq!(1, index.num_docs());
        Ok(())
    }

    #[test]
    fn search_on_empty_works() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let searcher = RecipeIndex::new(tmpdir.path())?;

        assert_eq!(searcher.search(&SearchQuery::default())?, &[0u64; 0]);
        Ok(())
    }

    #[test]
    fn empty_query_is_all_docs() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut index = RecipeIndex::new(tmpdir.path())?;

        index.add(index.doc_factory().make_document(1, "one".to_owned(), None));

        index.commit()?;
        index.reload_searchers()?;

        assert_eq!(
            vec![1],
            index.search(&SearchQuery {
                fulltext: Some("".to_owned()),
                ..Default::default()
            })?
        );

        Ok(())
    }

    #[test]
    fn can_find_after_add() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut index = RecipeIndex::new(tmpdir.path())?;

        index.add(index.doc_factory().make_document(1, "one".to_owned(), None));

        index.commit()?;
        index.reload_searchers()?;

        assert_eq!(
            vec![1],
            index.search(&SearchQuery {
                fulltext: Some("one".to_owned()),
                ..Default::default()
            })?
        );
        Ok(())
    }

    #[test]
    fn basic_search() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut index = RecipeIndex::new(tmpdir.path())?;

        let factory = index.doc_factory();
        index.add(factory.make_document(1, "one".to_owned(), None));
        index.add(factory.make_document(2, "one two".to_owned(), None));
        index.add(factory.make_document(3, "one two three".to_owned(), None));

        index.commit()?;
        index.reload_searchers()?;

        let do_search = |term: &str| -> Result<Vec<u64>> {
            let query = SearchQuery {
                fulltext: Some(term.to_owned()),
                ..Default::default()
            };
            let mut result = index.search(&query)?;
            result.sort();
            Ok(result)
        };

        assert_eq!(vec![1, 2, 3], do_search("one")?);
        assert_eq!(vec![2, 3], do_search("two")?);
        assert_eq!(vec![3], do_search("three")?);

        assert_eq!(0, do_search("-one")?.len());
        assert_eq!(vec![1], do_search("-two")?);
        assert_eq!(vec![1, 2], do_search("-three")?);

        assert_eq!(0, do_search("four")?.len());
        assert_eq!(vec![1, 2, 3], do_search("-four")?);

        assert_eq!(vec![2, 3], do_search(" \"one two\" ")?);
        assert_eq!(vec![3], do_search(" \"two three\" ")?);

        assert_eq!(vec![1], do_search(" -\"one two\" ")?);
        assert_eq!(vec![1, 2], do_search(" -\"two three\" ")?);

        Ok(())
    }

    #[test]
    fn feature_search() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut index = RecipeIndex::new(tmpdir.path())?;

        const A: Feature = Feature::Calories;
        const B: Feature = Feature::CarbContent;

        let factory = index.doc_factory();
        let do_add = |id: u64, feats| {
            index.add(factory.make_document(id, "".to_owned(), Some(feats)));
        };

        do_add(1, vec![(A, 1)]);
        do_add(2, vec![(A, 10), (B, 1)]);
        do_add(3, vec![(A, 100), (B, 10)]);

        index.commit()?;
        index.reload_searchers()?;

        let do_search = |feats: Vec<(Feature, Range)>| -> Result<Vec<u64>> {
            let query = SearchQuery {
                metadata: Some(feats),
                ..Default::default()
            };
            let mut result = index.search(&query)?;
            result.sort();
            Ok(result)
        };

        // Searching on A ranges
        assert_eq!(vec![1, 2, 3], do_search(vec![(A, (1, 100).into())])?);
        assert_eq!(vec![1, 2], do_search(vec![(A, (0, 11).into())])?);
        assert_eq!(vec![1], do_search(vec![(A, (1, 1).into())])?);
        assert_eq!(0, do_search(vec![(A, (0, 0).into())])?.len());

        // Matches on A always pass, B varies:
        assert_eq!(
            vec![2, 3],
            do_search(vec![(A, (1, 100).into()), (B, (1, 100).into())])?
        );
        assert_eq!(
            vec![3],
            do_search(vec![(A, (1, 100).into()), (B, (5, 100).into())])?
        );
        assert_eq!(
            0,
            do_search(vec![(A, (1, 100).into()), (B, (100, 101).into())])?.len()
        );

        Ok(())
    }

    #[test]
    fn index_fields_structure() {
        let (schema, fields) = FeatureIndexFields::new();

        let mut iter = schema.fields().iter();

        // expected fields in order
        assert_eq!(schema.get_field_entry(fields.id()), iter.next().unwrap());
        assert_eq!(
            schema.get_field_entry(fields.fulltext()),
            iter.next().unwrap()
        );
        assert_eq!(
            schema.get_field_entry(fields.feature_vector()),
            iter.next().unwrap()
        );

        // Now come feature fields
        for feat in Feature::VALUES.iter() {
            assert_eq!(
                iter.next().unwrap(),
                schema.get_field_entry(fields.feature(*feat))
            );
        }

        // And there should be nothing else
        assert_eq!(None, iter.next());
    }

    fn check_doc(id: u64, fulltext: String, features: Vec<(Feature, u16)>) {
        let num_features = features.len();
        let expected_len: usize =
            // Id + Fulltext
            1 + 1
            // When a feature is set we add a field for the FeatureVector
            // and one for each set feature
            + (if num_features > 0 { num_features + 1 } else {0 });

        let opt_feats = if num_features > 0 {
            Some(features.clone())
        } else {
            None
        };

        let (_schema, fields) = FeatureIndexFields::new();
        let FeatureDocument(doc) = fields.make_document(id, fulltext.clone(), opt_feats);

        assert_eq!(expected_len, doc.len());

        if let &Value::U64(doc_id) = doc.get_first(fields.id()).unwrap() {
            assert_eq!(id, doc_id);
        } else {
            panic!("Id field should be U64(id)");
        }

        if let Value::Str(entry) = doc.get_first(fields.fulltext()).unwrap() {
            assert_eq!(&fulltext, entry);
        } else {
            panic!("Fulltext field should be Vec<String(text)>");
        };

        if num_features > 0 {
            if let Value::Bytes(bytes) = doc.get_first(fields.feature_vector()).unwrap() {
                let mut buf = Feature::EMPTY_BUFFER.to_vec();
                let mut fv = FeatureVector::parse(buf.as_mut_slice()).unwrap();

                // One for the serialized feature vector
                for (feat, value) in features {
                    fv.set(&feat, value);
                    // And one for every set feature
                }

                assert_eq!(fv.as_bytes(), bytes.as_slice());
            } else {
                panic!("FeatureVector field should be Bytes(data)");
            }
        }
    }

    #[test]
    fn feature_document_is_built_correctly() {
        let specs = vec![
            (1, "document one", vec![]),
            (2, "the second", vec![(Feature::NumIngredients, 10)]),
            (
                3,
                "a third is good too!",
                vec![(Feature::NumIngredients, 10), (Feature::Calories, 100)],
            ),
        ];

        for (id, fulltext, features) in specs {
            check_doc(id, fulltext.to_owned(), features);
        }
    }

}
