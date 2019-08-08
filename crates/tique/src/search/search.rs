use std::path::Path;

use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query, RangeQuery},
    schema::{
        Field, IndexRecordOption, SchemaBuilder, TextFieldIndexing, TextOptions, Value, FAST,
        INDEXED, STORED,
    },
    tokenizer::TokenizerManager,
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

use super::features::{Feature, FeatureVector};
use super::model::{Range, SearchQuery};
use super::query_parser::QueryParser;

use super::Result;

pub struct RecipeIndex {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    query_parser: QueryParser,
}

pub struct AddArgs {
    id: u64,
    fulltext: Vec<String>,
    features: Option<Vec<(Feature, u16)>>,
}

const ID_FIELD: &'static str = "id";
const FULLTEXT_FIELD: &'static str = "fulltext";
const FEATURES_FIELD: &'static str = "features";

impl RecipeIndex {
    pub fn new(index_path: &Path) -> Result<RecipeIndex> {
        let mut builder = SchemaBuilder::new();

        let indexing = TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_field_options = TextOptions::default().set_indexing_options(indexing);

        let fulltext_field = builder.add_text_field(FULLTEXT_FIELD, text_field_options);

        builder.add_u64_field(ID_FIELD, FAST | STORED);

        // Stores a Features struct, used for dynamic aggregations
        builder.add_bytes_field(FEATURES_FIELD);

        for feat in Feature::VALUES.iter() {
            builder.add_u64_field(&feat.to_string(), INDEXED);
        }

        let schema = builder.build();

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, schema)?;
        let writer = index.writer(10_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let tokenizer = TokenizerManager::default()
            .get("en_stem")
            .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))?;

        let parser = QueryParser::new(fulltext_field, tokenizer);

        Ok(RecipeIndex {
            index: index,
            writer: writer,
            reader: reader,
            query_parser: parser,
        })
    }

    fn field(&self, field_name: &str) -> Field {
        self.index
            .schema()
            .get_field(field_name)
            .expect("Field doesn't exist")
    }

    fn interpret_query(&self, query: &SearchQuery) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(metadata) = &query.metadata {
            for (feat, range) in metadata {
                clauses.push((
                    Occur::Must,
                    Box::new(RangeQuery::new_u64(
                        self.field(feat.to_string().as_str()),
                        range.into(),
                    )),
                ));
            }
        }

        if let Some(fulltext) = &query.fulltext {
            if let Some(boxed_query) = self.query_parser.parse(fulltext.as_ref())? {
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

    pub fn add(&self, args: AddArgs) {
        let mut doc = Document::default();

        for text in args.fulltext {
            doc.add_text(self.field(FULLTEXT_FIELD), text.as_str());
        }

        doc.add_u64(self.field(ID_FIELD), args.id);

        let mut buf = Feature::EMPTY_BUFFER.to_vec();
        let mut features = FeatureVector::parse(buf.as_mut_slice()).unwrap();

        args.features.map(|feats| {
            for (feat, value) in feats {
                features.set(&feat, value);

                doc.add_u64(self.field(feat.to_string().as_str()), value as u64);
            }
        });

        doc.add_bytes(self.field(FEATURES_FIELD), features.as_bytes().into());

        self.writer.add_document(doc);
    }

    pub fn search(&self, query: &SearchQuery) -> Result<Vec<u64>> {
        let searcher = self.reader.searcher();
        let iquery = self.interpret_query(query)?;

        let hits = searcher.search(&iquery, &TopDocs::with_limit(10))?;
        let mut ids = Vec::with_capacity(hits.len());

        for (_score, addr) in hits {
            ids.push(
                searcher
                    .doc(addr)?
                    .get_first(self.field(ID_FIELD))
                    .expect("Found document without an id field")
                    .u64_value(),
            );
        }

        Ok(ids)
    }

    // For testing
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

        index.add(AddArgs {
            id: 1,
            fulltext: vec!["one".to_owned()],
            features: None,
        });

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

        index.add(AddArgs {
            id: 1,
            fulltext: vec!["one".to_owned()],
            features: None,
        });

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

        index.add(AddArgs {
            id: 1,
            fulltext: vec!["one".to_owned()],
            features: None,
        });

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

        let mut do_add = |id: u64, name: &str| -> Result<()> {
            index.add(AddArgs {
                id: id,
                fulltext: vec![name.to_owned()],
                features: None,
            });
            index.commit()
        };

        do_add(1, "one")?;
        do_add(2, "one two")?;
        do_add(3, "one two three")?;

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

        let mut do_add = |id: u64, feats| {
            index.add(AddArgs {
                id: id,
                fulltext: vec![],
                features: Some(feats),
            });
            index.commit().unwrap();
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
}
