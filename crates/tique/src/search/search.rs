use std::path::Path;

use tantivy::{
    directory::MmapDirectory,
    schema::{SchemaBuilder, FAST, STORED, TEXT},
    Document, Index, IndexReader, IndexWriter, ReloadPolicy,
};

use super::Result;

pub struct Searcher {
    index: Index,
    reader: IndexReader,
    writer: IndexWriter,
}

impl Searcher {
    pub fn new(index_path: &Path) -> Result<Searcher> {
        let mut builder = SchemaBuilder::new();

        builder.add_u64_field("id", FAST | STORED);
        builder.add_text_field("name", TEXT);

        let index = Index::open_or_create(MmapDirectory::open(index_path)?, builder.build())?;
        let writer = index.writer(10_000_000)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;
        Ok(Searcher {
            index: index,
            writer: writer,
            reader: reader,
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
        searcher.add(2, "caio romao");

        searcher.commit()?;
        searcher.reload_searchers()?;

        assert_eq!(2, searcher.num_docs());
        Ok(())
    }

}
