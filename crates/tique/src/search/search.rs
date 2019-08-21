use std::{
    marker::PhantomData,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery},
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, Value,
        FAST, INDEXED, STORED,
    },
    tokenizer::TokenizerManager,
    Document, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyError,
};

use crate::search::{collector::FeatureCollector, FeatureVector, IsUnset, QueryParser, Result};
use crate::Feature;

pub struct RecipeIndex {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    query_parser: QueryParser,
    fields: FeatureIndexFields<Feature>,
}

#[derive(Clone)]
pub struct FeatureIndexFields<T>(Vec<Field>, PhantomData<T>);

impl<T> FeatureIndexFields<T>
where
    T: Into<usize> + IsUnset<u16> + Copy,
{
    pub fn new(num_features: usize) -> (Schema, FeatureIndexFields<T>) {
        let mut builder = SchemaBuilder::new();
        let mut fields = Vec::with_capacity(3 + num_features);

        let indexing = TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_field_options = TextOptions::default().set_indexing_options(indexing);

        // 3 core fields
        fields.push(builder.add_u64_field("id", FAST | STORED));
        fields.push(builder.add_text_field("ft", text_field_options));
        fields.push(builder.add_bytes_field("fv"));

        // one for each feature, for filtering
        for i in 0..num_features {
            fields.push(builder.add_u64_field(format!("feature_{}", i).as_str(), INDEXED));
        }

        (builder.build(), FeatureIndexFields(fields, PhantomData))
    }

    pub fn id(&self) -> Field {
        self.0[0]
    }

    pub fn fulltext(&self) -> Field {
        self.0[1]
    }

    pub fn feature_vector(&self) -> Field {
        self.0[2]
    }

    pub fn num_features(&self) -> usize {
        self.0.len() - 3
    }

    pub fn feature(&self, feat: T) -> Option<Field> {
        let featno: usize = feat.into();
        if featno < self.num_features() {
            Some(self.0[3 + featno])
        } else {
            None
        }
    }

    pub fn search(
        &self,
        request: &SearchRequest<T>,
        query_parser: &QueryParser,
        searcher: &tantivy::Searcher,
    ) -> Result<(Vec<u64>, usize)> {
        let iquery = self.interpret_request(&request, &query_parser).unwrap();

        // let (hits, agg) = searcher
        // FeatureCollector::for_field(
        //     self.feature_vector(),
        //     self.num_features(),
        //     request.agg.unwrap_or(Vec::new()),
        // ),
        let hits = searcher
            .search(
                &iquery,
                &TopDocs::with_limit(request.page_size.unwrap_or(10) as usize),
            )
            .unwrap();
        let mut ids = Vec::new();

        for (_score, addr) in hits {
            ids.push(
                searcher
                    .doc(addr)
                    .unwrap()
                    .get_first(self.id())
                    .expect("Found document without an id field")
                    .u64_value(),
            );
        }

        Ok((ids, 0))
    }

    pub fn interpret_request(
        &self,
        req: &SearchRequest<T>,
        query_parser: &QueryParser,
    ) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(filters) = &req.filter {
            for spec in filters {
                clauses.push((
                    Occur::Must,
                    Box::new(RangeQuery::new_u64(
                        self.feature(spec.0).ok_or_else(|| {
                            TantivyError::SystemError("Unknown feature in filters".to_owned())
                        })?,
                        // inclusive range
                        (spec.1 as u64)..((spec.2 + 1) as u64),
                    )),
                ));
            }
        }

        if let Some(fulltext) = &req.query {
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

    pub fn make_document(
        &self,
        id: u64,
        fulltext: String,
        features: Option<Vec<(T, u16)>>,
    ) -> FeatureDocument {
        let mut doc = Document::default();

        doc.add_u64(self.id(), id);
        doc.add_text(self.fulltext(), fulltext.as_str());

        let num_features = self.num_features();
        let buf_size = 2 * self.num_features();

        features.map(|feats| {
            // XXX I could get rid of magic numbers with a bitset
            let mut buf = vec![std::u8::MAX; buf_size];
            let mut fv = FeatureVector::parse(num_features, buf.as_mut_slice()).unwrap();
            for (feat, value) in feats {
                fv.set(feat, value).unwrap();
                // XXX Blindly ignoring. Log? Error? Config?
                if let Some(feature) = self.feature(feat) {
                    doc.add_u64(feature, value as u64);
                }
            }
            doc.add_bytes(self.feature_vector(), fv.as_bytes().into());
        });

        FeatureDocument(doc)
    }

    pub fn add_document(&self, writer: &IndexWriter, fd: FeatureDocument) {
        let FeatureDocument(doc) = fd;
        writer.add_document(doc);
    }

    pub fn open_or_create(
        num_features: usize,
        base_dir: Option<PathBuf>,
    ) -> Result<(Index, FeatureIndexFields<T>)> {
        let (schema, fields): (Schema, FeatureIndexFields<T>) =
            FeatureIndexFields::new(num_features);

        let index = if let Some(path) = base_dir {
            Index::open_or_create(MmapDirectory::open(&path).unwrap(), schema)?
        } else {
            Index::create_in_ram(schema)
        };

        Ok((index, fields))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SearchRequest<T> {
    pub query: Option<String>,
    // default 1, non zero
    pub page: Option<u16>,
    // default 20, non zero also
    pub page_size: Option<u8>,
    pub filter: Option<FilterRequest<T>>,
    pub agg: Option<AggregationRequest<T>>,
}

impl Default for SearchRequest<usize> {
    fn default() -> Self {
        Self {
            query: None,
            page: None,
            page_size: None,
            filter: None,
            agg: None,
        }
    }
}

impl Default for SearchRequest<Feature> {
    fn default() -> Self {
        Self {
            query: None,
            page: None,
            page_size: None,
            filter: None,
            agg: None,
        }
    }
}

pub type FilterRequest<T> = Vec<(T, u16, u16)>;
pub type AggregationRequest<T> = Vec<(T, Vec<[u16; 2]>)>;

pub struct FeatureDocument(Document);

impl RecipeIndex {
    pub fn new(index_path: &Path) -> Result<RecipeIndex> {
        let (schema, fields) = FeatureIndexFields::new(Feature::LENGTH);

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, schema)?;

        let writer = index.writer(40_000_000)?;
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

    pub fn doc_factory(&self) -> FeatureIndexFields<Feature> {
        self.fields.clone()
    }

    pub fn add(&self, feature_document: FeatureDocument) {
        self.fields.add_document(&self.writer, feature_document);
    }

    pub fn search(&self, req: &SearchRequest<Feature>) -> Result<Vec<u64>> {
        let searcher = self.reader.searcher();
        let iquery = self.fields.interpret_request(req, &self.query_parser)?;

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

    mod feat {
        pub const A: usize = 0;
        pub const B: usize = 1;
        pub const C: usize = 2;
        pub const D: usize = 3;
    }

    // So I can use u8 as a feature
    impl IsUnset<u16> for u8 {
        fn is_unset(val: u16) -> bool {
            val == std::u8::MAX as u16
        }
    }

    #[test]
    fn can_open_in_ram() {
        let (_index, fields) = FeatureIndexFields::<u8>::open_or_create(1, None).unwrap();
        assert_eq!(1, fields.num_features());
    }

    #[test]
    fn can_create_ok() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let (_index, fields) =
            FeatureIndexFields::<u8>::open_or_create(2, Some(tmpdir.into_path())).unwrap();
        assert_eq!(2, fields.num_features());
    }

    #[test]
    fn fulltext_search() {
        let (index, fields) = FeatureIndexFields::<usize>::open_or_create(1, None).unwrap();

        let mut writer = index.writer_with_num_threads(1, 40_000_000).unwrap();

        fields.add_document(&mut writer, fields.make_document(1, "one".to_owned(), None));
        fields.add_document(
            &mut writer,
            fields.make_document(2, "one two".to_owned(), None),
        );
        fields.add_document(
            &mut writer,
            fields.make_document(3, "one two three".to_owned(), None),
        );

        writer.commit().unwrap();

        let tokenizer = TokenizerManager::default()
            .get("en_stem")
            .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))
            .unwrap();

        let query_parser = QueryParser::new(fields.fulltext(), tokenizer);

        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let do_search = |term: &str| -> Result<Vec<u64>> {
            let query: SearchRequest<usize> = SearchRequest {
                query: Some(term.to_owned()),
                ..Default::default()
            };
            let interpreted = fields.interpret_request(&query, &query_parser).unwrap();
            let (mut result, _agg) = fields.search(&query, &query_parser, &searcher).unwrap();
            result.sort();
            Ok(result)
        };

        assert_eq!(vec![1, 2, 3], do_search("one").unwrap());
        assert_eq!(vec![2, 3], do_search("two").unwrap());
        assert_eq!(vec![3], do_search("three").unwrap());

        assert_eq!(0, do_search("-one").unwrap().len());
        assert_eq!(vec![1], do_search("-two").unwrap());
        assert_eq!(vec![1, 2], do_search("-three").unwrap());

        assert_eq!(0, do_search("four").unwrap().len());
        assert_eq!(vec![1, 2, 3], do_search("-four").unwrap());

        assert_eq!(vec![2, 3], do_search(" \"one two\" ").unwrap());
        assert_eq!(vec![3], do_search(" \"two three\" ").unwrap());

        assert_eq!(vec![1], do_search(" -\"one two\" ").unwrap());
        assert_eq!(vec![1, 2], do_search(" -\"two three\" ").unwrap());
    }

    #[test]
    fn feature_search() -> Result<()> {
        let (index, fields) = FeatureIndexFields::<usize>::open_or_create(2, None).unwrap();

        const A: usize = 0;
        const B: usize = 1;

        let mut writer = index.writer_with_num_threads(1, 40_000_000).unwrap();

        let mut do_add = |id: u64, feats| {
            fields.add_document(
                &mut writer,
                fields.make_document(id, "".to_owned(), Some(feats)),
            );
        };

        do_add(1, vec![(A, 1)]);
        do_add(2, vec![(A, 10), (B, 1)]);
        do_add(3, vec![(A, 100), (B, 10)]);

        writer.commit()?;
        let reader = index.reader().unwrap();
        let searcher = reader.searcher();

        let tokenizer = TokenizerManager::default().get("en_stem").unwrap();

        let parser = QueryParser::new(fields.fulltext(), tokenizer);

        let do_search = |feats: FilterRequest<usize>| -> Result<Vec<u64>> {
            let query = SearchRequest {
                filter: Some(feats),
                ..Default::default()
            };
            let (mut result, _) = fields.search(&query, &parser, &searcher)?;
            result.sort();
            Ok(result)
        };

        // Searching on A ranges
        assert_eq!(vec![1, 2, 3], do_search(vec![(A, 1, 100)])?);
        assert_eq!(vec![1, 2], do_search(vec![(A, 0, 11)])?);
        assert_eq!(vec![1], do_search(vec![(A, 1, 1)])?);
        assert_eq!(0, do_search(vec![(A, 0, 0)])?.len());

        // Matches on A always pass, B varies:
        assert_eq!(
            vec![2, 3],
            do_search(vec![(A, 1, 100).into(), (B, 1, 100).into()])?
        );
        assert_eq!(
            vec![3],
            do_search(vec![(A, 1, 100).into(), (B, 5, 100).into()])?
        );
        assert_eq!(
            0,
            do_search(vec![(A, 1, 100).into(), (B, 100, 101).into()])?.len()
        );

        Ok(())
    }

    #[test]
    fn can_get_a_field_for_every_known_feature() {
        let num_features = 100;
        let (_schema, fields) = FeatureIndexFields::new(num_features);

        for feat in 0..num_features {
            assert!(fields.feature(feat).is_some())
        }
    }

    #[test]
    fn index_fields_structure() {
        let num_features = 10;
        let (schema, fields) = FeatureIndexFields::new(num_features);
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
        for feat in 0..num_features {
            assert_eq!(
                iter.next().unwrap(),
                schema.get_field_entry(fields.feature(feat).unwrap())
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

        let (_schema, fields) = FeatureIndexFields::new(Feature::LENGTH);
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
                let mut fv = FeatureVector::parse(Feature::LENGTH, buf.as_mut_slice()).unwrap();

                // One for the serialized feature vector
                for (feat, value) in features {
                    fv.set(feat, value).unwrap();
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
