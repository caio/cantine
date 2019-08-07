use std::path::Path;

use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::{AllQuery, BooleanQuery, Occur, Query},
    schema::{
        Field, IndexRecordOption, SchemaBuilder, TextFieldIndexing, TextOptions, FAST, STORED,
    },
    tokenizer::TokenizerManager,
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

use serde::{self, Deserialize, Serialize};

use super::query_parser::QueryParser;
use super::SearchQuery;

use super::Result;

pub struct RecipeIndex {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    query_parser: QueryParser,
    fields: Fields,
}

struct Fields {
    id: Field,
    name: Field,
}

impl RecipeIndex {
    pub fn new(index_path: &Path) -> Result<RecipeIndex> {
        let mut builder = SchemaBuilder::new();

        let id_field = builder.add_u64_field("id", FAST | STORED);

        let indexing = TextFieldIndexing::default()
            .set_tokenizer("en_stem")
            .set_index_option(IndexRecordOption::WithFreqsAndPositions);
        let text_field_options = TextOptions::default().set_indexing_options(indexing);

        let name_field = builder.add_text_field("name", text_field_options);

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, builder.build())?;
        let writer = index.writer(10_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let tokenizer = TokenizerManager::default()
            .get("en_stem")
            .ok_or_else(|| tantivy::TantivyError::SystemError("Tokenizer not found".to_owned()))?;

        let parser = QueryParser::new(name_field, tokenizer);

        Ok(RecipeIndex {
            index: index,
            writer: writer,
            reader: reader,
            query_parser: parser,
            fields: Fields {
                id: id_field,
                name: name_field,
            },
        })
    }

    fn interpret_query(&self, query: &SearchQuery) -> Result<Box<dyn Query>> {
        let mut clauses: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        if let Some(fulltext) = &query.fulltext {
            let parsed = self.query_parser.parse(fulltext.as_ref())?;

            parsed.map(|r| clauses.push((Occur::Must, r)));
        }

        if clauses.len() == 0 {
            Ok(Box::new(AllQuery))
        } else {
            let bq: BooleanQuery = clauses.into();
            Ok(Box::new(bq))
        }
    }

    pub fn add(&mut self, id: u64, name: &str) {
        let mut doc = Document::default();

        doc.add_u64(self.fields.id, id);
        doc.add_text(self.fields.name, name);

        self.writer.add_document(doc);
    }

    pub fn search(&self, query: &SearchQuery) -> Result<Vec<u64>> {
        let interpreted = self.interpret_query(query)?;

        let searcher = self.reader.searcher();

        let hits = searcher.search(&interpreted, &TopDocs::with_limit(10))?;

        let mut ids = Vec::with_capacity(hits.len());
        for (_score, addr) in hits {
            ids.push(
                searcher
                    .doc(addr)?
                    .get_first(self.fields.id)
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
        let mut searcher = RecipeIndex::new(tmpdir.path())?;

        assert_eq!(0, searcher.num_docs());

        searcher.add(1, "potato");
        searcher.add(2, "apple");

        searcher.commit()?;
        searcher.reload_searchers()?;

        assert_eq!(2, searcher.num_docs());
        Ok(())
    }

    #[test]
    fn search_on_empty_works() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let searcher = RecipeIndex::new(tmpdir.path())?;

        assert_eq!(searcher.search(&SearchQuery::default())?, &[0u64; 0]);
        Ok(())
    }

    // Used instead of assert_eq! because ComparableDoc
    // is not stable (by design, it seems) and plain negation queries
    // (used in a few tests) lead to matching all docs with a
    // score of 1f, making results ordering unstable
    fn contains_all(searcher: &RecipeIndex, q: &str, needed: &[u64]) -> Result<()> {
        let query = SearchQuery {
            fulltext: Some(q.to_string()),
            ..Default::default()
        };
        let result = searcher.search(&query)?;
        for element in needed {
            assert!(result.contains(element));
        }
        assert_eq!(needed.len(), result.len());
        Ok(())
    }

    #[test]
    fn empty_query_is_all_docs() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut searcher = RecipeIndex::new(tmpdir.path())?;

        searcher.add(1, "one");
        searcher.add(2, "two");
        searcher.commit()?;
        searcher.reload_searchers()?;

        contains_all(&searcher, "", &[1, 2])?;

        Ok(())
    }

    #[test]
    fn basic_search() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut searcher = RecipeIndex::new(tmpdir.path())?;

        searcher.add(1, "one");
        searcher.add(2, "one two");
        searcher.add(3, "one two three");

        searcher.commit()?;
        searcher.reload_searchers()?;

        contains_all(&searcher, "one", &[1, 2, 3])?;
        contains_all(&searcher, "one", &[1, 2, 3])?;
        contains_all(&searcher, "two", &[2, 3])?;
        contains_all(&searcher, "three", &[3])?;

        contains_all(&searcher, "-one", &[0u64; 0])?;
        contains_all(&searcher, "-two", &[1])?;
        contains_all(&searcher, "-three", &[1, 2])?;

        contains_all(&searcher, "four", &[0u64; 0])?;
        contains_all(&searcher, "-four", &[2, 1, 3])?;

        contains_all(&searcher, "\"one two\"", &[2, 3])?;
        contains_all(&searcher, "\"two three\"", &[3])?;

        contains_all(&searcher, "-\"one two\"", &[1])?;
        contains_all(&searcher, "-\"two three\"", &[1, 2])?;

        Ok(())
    }
}
