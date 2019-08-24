use std::{ops::RangeInclusive, path::PathBuf};

use serde::{Deserialize, Serialize};

use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery},
    schema::{
        Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, FAST,
        INDEXED, STORED,
    },
    Document, Index, IndexWriter, TantivyError,
};

use crate::search::{
    collector::FeatureCollector, FeatureRanges, FeatureValue, FeatureVector, QueryParser, Result,
};

#[derive(Clone)]
pub struct FeatureIndexFields(Vec<Field>);

impl FeatureIndexFields where {
    pub fn new(num_features: usize, tokenizer: Option<&str>) -> (Schema, FeatureIndexFields) {
        assert!(num_features > 0);

        let mut builder = SchemaBuilder::new();
        let mut fields = Vec::with_capacity(3 + num_features);

        let indexing = TextFieldIndexing::default()
            .set_tokenizer(tokenizer.unwrap_or("en_stem"))
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

        (builder.build(), FeatureIndexFields(fields))
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

    pub fn feature(&self, feat: usize) -> Option<Field> {
        if feat < self.num_features() {
            Some(self.0[3 + feat])
        } else {
            None
        }
    }

    pub fn search(
        &self,
        request: &SearchRequest,
        query_parser: &QueryParser,
        searcher: &tantivy::Searcher,
    ) -> Result<(Vec<u64>, FeatureRanges<FeatureValue>)> {
        let iquery = self.interpret_request(&request, &query_parser).unwrap();

        let (hits, agg) = searcher
            .search(
                &iquery,
                &(
                    TopDocs::with_limit(request.page_size.unwrap_or(10) as usize),
                    FeatureCollector::for_field(
                        self.feature_vector(),
                        self.num_features(),
                        &request.agg.as_ref().unwrap_or(&Vec::new()),
                    ),
                ),
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

        Ok((ids, agg))
    }

    pub fn interpret_request(
        &self,
        req: &SearchRequest,
        query_parser: &QueryParser,
    ) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(filters) = &req.filter {
            for (feat, range) in filters {
                clauses.push((
                    Occur::Must,
                    Box::new(RangeQuery::new_u64(
                        self.feature(*feat).ok_or_else(|| {
                            TantivyError::SystemError("Unknown feature in filters".to_owned())
                        })?,
                        // XXX can't this be less awkward?
                        u64::from(*range.start())..u64::from(range.end() + 1),
                    )),
                ));
            }
        }

        if let Some(fulltext) = &req.query {
            if let Some(boxed_query) = query_parser.parse(fulltext.as_ref())? {
                if clauses.is_empty() {
                    return Ok(boxed_query);
                } else {
                    clauses.push((Occur::Must, boxed_query))
                }
            }
        }

        if clauses.is_empty() {
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
        features: Option<Vec<(usize, FeatureValue)>>,
    ) -> FeatureDocument {
        let mut doc = Document::default();

        doc.add_u64(self.id(), id);
        doc.add_text(self.fulltext(), fulltext.as_str());

        let num_features = self.num_features();
        let buf_size = 2 * self.num_features();

        let mut buf = vec![std::u8::MAX; buf_size];
        let mut fv = FeatureVector::parse(num_features, buf.as_mut_slice()).unwrap();

        if let Some(feats) = features {
            for (feat, value) in feats {
                fv.set(feat, value).unwrap();
                if let Some(feature) = self.feature(feat) {
                    doc.add_u64(feature, u64::from(value));
                }
            }
        }

        doc.add_bytes(self.feature_vector(), fv.as_bytes().into());
        FeatureDocument(doc)
    }

    pub fn add_document(&self, writer: &IndexWriter, fd: FeatureDocument) {
        let FeatureDocument(doc) = fd;
        writer.add_document(doc);
    }

    pub fn open_or_create(
        num_features: usize,
        base_dir: Option<PathBuf>,
        tokenizer: Option<&str>,
    ) -> Result<(Index, FeatureIndexFields)> {
        let (schema, fields) = FeatureIndexFields::new(num_features, tokenizer);

        let index = if let Some(path) = base_dir {
            Index::open_or_create(MmapDirectory::open(&path).unwrap(), schema)?
        } else {
            Index::create_in_ram(schema)
        };

        Ok((index, fields))
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct SearchRequest {
    pub query: Option<String>,
    // default 1, non zero
    pub page: Option<u16>,
    // default 20, non zero also
    pub page_size: Option<u8>,
    pub filter: Option<FilterRequest>,
    pub agg: Option<AggregationRequest>,
}

pub type FilterRequest = Vec<(usize, RangeInclusive<FeatureValue>)>;
pub type AggregationRequest = Vec<(usize, Vec<RangeInclusive<FeatureValue>>)>;

pub struct FeatureDocument(Document);

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use tantivy::{
        schema::{FieldType, Value},
        tokenizer::TokenizerManager,
    };

    #[test]
    fn can_open_in_ram() {
        let (_index, fields) = FeatureIndexFields::open_or_create(1, None, None).unwrap();
        assert_eq!(1, fields.num_features());
    }

    #[test]
    fn can_create_ok() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let (_index, fields) =
            FeatureIndexFields::open_or_create(2, Some(tmpdir.into_path()), None).unwrap();
        assert_eq!(2, fields.num_features());
    }

    #[test]
    fn can_set_tokenizer_name() {
        let (index, fields) = FeatureIndexFields::open_or_create(2, None, Some("custom")).unwrap();

        let schema = index.schema();
        let entry = schema.get_field_entry(fields.fulltext());

        if let FieldType::Str(opts) = entry.field_type() {
            assert_eq!(
                Some("custom"),
                opts.get_indexing_options().map(|o| o.tokenizer())
            )
        } else {
            panic!("Fulltext field should be text")
        }
    }

    #[test]
    fn fulltext_search() {
        let (index, fields) = FeatureIndexFields::open_or_create(1, None, None).unwrap();

        let mut writer = index.writer_with_num_threads(1, 40_000_000).unwrap();

        fields.add_document(&writer, fields.make_document(1, "one".to_owned(), None));
        fields.add_document(&writer, fields.make_document(2, "one two".to_owned(), None));
        fields.add_document(
            &writer,
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
            let query: SearchRequest = SearchRequest {
                query: Some(term.to_owned()),
                ..Default::default()
            };
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
        let (index, fields) = FeatureIndexFields::open_or_create(2, None, None).unwrap();

        const A: usize = 0;
        const B: usize = 1;

        let mut writer = index.writer_with_num_threads(1, 40_000_000).unwrap();

        let do_add = |id: u64, feats| {
            fields.add_document(
                &writer,
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

        let do_search = |feats: FilterRequest| -> Result<Vec<u64>> {
            let query = SearchRequest {
                filter: Some(feats),
                ..Default::default()
            };
            let (mut result, _) = fields.search(&query, &parser, &searcher)?;
            result.sort();
            Ok(result)
        };

        // Searching on A ranges
        assert_eq!(vec![1, 2, 3], do_search(vec![(A, 1..=100)])?);
        assert_eq!(vec![1, 2], do_search(vec![(A, 0..=11)])?);
        assert_eq!(vec![1], do_search(vec![(A, 1..=1)])?);
        assert_eq!(0, do_search(vec![(A, 0..=0)])?.len());

        // Matches on A always pass, B varies:
        assert_eq!(vec![2, 3], do_search(vec![(A, 1..=100), (B, 1..=100)])?);
        assert_eq!(vec![3], do_search(vec![(A, 1..=100), (B, 5..=100)])?);
        assert_eq!(0, do_search(vec![(A, 1..=100), (B, 100..=101)])?.len());

        Ok(())
    }

    #[test]
    fn can_get_a_field_for_every_known_feature() {
        let num_features = 100;
        let (_schema, fields) = FeatureIndexFields::new(num_features, None);

        for feat in 0..num_features {
            assert!(fields.feature(feat).is_some())
        }
    }

    #[test]
    fn index_fields_structure() {
        let num_features = 10;
        let (schema, fields) = FeatureIndexFields::new(num_features, None);
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

    fn check_doc(id: u64, fulltext: String, features: Vec<(usize, FeatureValue)>) {
        let num_features = features.len();
        let expected_len: usize =
            // Id + Fulltext + FeatureVector
            3
            // Plus one for each set feature
            + num_features;

        let opt_feats = if num_features > 0 {
            Some(features.clone())
        } else {
            None
        };

        let (_schema, fields) = FeatureIndexFields::new(2, None);
        let FeatureDocument(doc) = fields.make_document(id, fulltext.clone(), opt_feats);

        assert_eq!(expected_len, doc.len());

        if let Value::U64(doc_id) = doc.get_first(fields.id()).unwrap() {
            assert_eq!(&id, doc_id);
        } else {
            panic!("Id field should be U64(id)");
        }

        if let Value::Str(entry) = doc.get_first(fields.fulltext()).unwrap() {
            assert_eq!(&fulltext, entry);
        } else {
            panic!("Fulltext field should be Str(text)>");
        };

        if num_features > 0 {
            if let Value::Bytes(bytes) = doc.get_first(fields.feature_vector()).unwrap() {
                let mut buf = vec![std::u8::MAX; 4];
                let mut fv = FeatureVector::parse(2, buf.as_mut_slice()).unwrap();

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
        const A: usize = 0;
        const B: usize = 1;

        let specs = vec![
            (1, "document one", vec![]),
            (2, "the second", vec![(A, 10)]),
            (3, "a third is good too!", vec![(A, 10), (B, 100)]),
        ];

        for (id, fulltext, features) in specs {
            check_doc(id, fulltext.to_owned(), features);
        }
    }

}
