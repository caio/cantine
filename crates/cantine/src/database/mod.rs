use std::collections::HashMap;
use std::io;
use std::path::Path;

mod mapped_file;
use mapped_file::AppendOnlyMappedFile;

#[allow(dead_code, unused_imports)]
mod recipe_generated;

pub type Error = io::Error;
pub type Result<T> = io::Result<T>;

pub struct Database {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, u64>,
}

impl Database {
    pub fn new(base_dir: &Path) -> Result<Database> {
        Ok(Database {
            log: AppendOnlyMappedFile::new(&base_dir.join("log.bin"))?,
            data: AppendOnlyMappedFile::new(&base_dir.join("data.bin"))?,
            index: HashMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    #[test]
    fn can_open_empty_db() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        Database::new(&tmpdir.path()).unwrap();
    }
}
