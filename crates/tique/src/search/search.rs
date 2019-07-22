use std::path::Path;

use tantivy::{
    collector::TopDocs,
    directory::MmapDirectory,
    query::QueryParser,
    schema::{SchemaBuilder, FAST, STORED, TEXT},
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

use super::Result;

pub struct Searcher {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
    query_parser: QueryParser,
}

impl Searcher {
    pub fn new(index_path: &Path) -> Result<Searcher> {
        let mut builder = SchemaBuilder::new();

        builder.add_u64_field("id", FAST | STORED);
        let name_field = builder.add_text_field("name", TEXT);

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, builder.build())?;
        let writer = index.writer(10_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let parser = QueryParser::for_index(&index, vec![name_field]);

        Ok(Searcher {
            index: index,
            writer: writer,
            reader: reader,
            query_parser: parser,
        })
    }

    pub fn add(&mut self, id: u64, name: &str) {
        let mut doc = Document::default();

        doc.add_u64(self.index.schema().get_field("id").expect("impossible"), id);
        doc.add_text(
            self.index.schema().get_field("name").expect("impossible"),
            name,
        );

        self.writer.add_document(doc);
    }

    pub fn search(&self, query: &str) -> Result<(Vec<u64>)> {
        let query = self.query_parser.parse_query(query)?;
        let searcher = self.reader.searcher();

        let id_field = self.index.schema().get_field("id").expect("impossible");
        let hits = searcher.search(&query, &TopDocs::with_limit(10))?;

        let mut ids = Vec::with_capacity(hits.len());
        for (_score, addr) in hits {
            ids.push(
                searcher
                    .doc(addr)?
                    .get_first(id_field)
                    .expect("impossible")
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
        Searcher::new(tmpdir.path())?;
        Ok(())
    }

    #[test]
    fn can_commit_after_create() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut searcher = Searcher::new(tmpdir.path())?;
        searcher.commit()?;
        Ok(())
    }

    #[test]
    fn num_docs_increases() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut searcher = Searcher::new(tmpdir.path())?;

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
        let searcher = Searcher::new(tmpdir.path())?;

        assert_eq!(searcher.search("term")?, &[0u64; 0]);
        Ok(())
    }

    #[test]
    fn basic_search() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let mut searcher = Searcher::new(tmpdir.path())?;

        searcher.add(1, "one");
        searcher.add(2, "one two");
        searcher.add(3, "one two three");

        searcher.commit()?;
        searcher.reload_searchers()?;

        assert_eq!(searcher.search("one")?, &[1, 2, 3]);
        assert_eq!(searcher.search("two")?, &[2, 3]);
        assert_eq!(searcher.search("three")?, &[3]);
        assert_eq!(searcher.search("four")?, &[0u64; 0]);

        Ok(())
    }

}
