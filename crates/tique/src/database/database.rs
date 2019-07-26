use std::collections::HashMap;
use std::io;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::mapped_file::AppendOnlyMappedFile;

type Result<T> = super::Result<T>;

pub struct BytesDatabase {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, usize>,
}

impl BytesDatabase {
    pub fn new(base_dir: &Path) -> Result<BytesDatabase> {
        let mut index = HashMap::new();
        let mut max_offset = 0;

        let log = AppendOnlyMappedFile::new(&base_dir.join("log.bin"))?;
        log.each_chunk(16, |chunk| {
            let mut cursor = io::Cursor::new(&chunk);
            let id = cursor.read_u64::<LittleEndian>()?;

            // No removals, the offsets are always increasing
            max_offset = cursor.read_u64::<LittleEndian>()?;

            // So, when a id is already known it gets replaced
            index.insert(id, max_offset as usize);
            Ok(())
        })?;

        let data = AppendOnlyMappedFile::new(&base_dir.join("data.bin"))?;
        // TODO more checks

        if max_offset > 0 && max_offset as usize >= data.len() {
            // This shouldn't be possible via AppendOnlyMappedFile contract's
            // But maybe something touched it externally
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index points at unreachable",
            ))
        } else {
            let db = BytesDatabase {
                index: index,
                log: log,
                data: data,
            };

            Ok(db)
        }
    }

    pub fn add(&mut self, id: u64, data: &[u8]) -> Result<()> {
        let cur_offset = self.data.len();
        self.data.append(data)?;

        // XXX Awkward
        let mut buf = Vec::with_capacity(16);
        buf.write_u64::<LittleEndian>(id)?;
        buf.write_u64::<LittleEndian>(cur_offset as u64)?;

        self.log.append(buf.as_mut_slice())?;

        self.index.insert(id, cur_offset);

        Ok(())
    }

    pub fn get(&self, id: u64) -> Option<&[u8]> {
        self.index
            .get(&id)
            .map_or(None, |offset| self.data.from_offset(*offset).ok())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use bincode::{deserialize, serialize};
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Recipe<'a> {
        pub id: u64,
        name: &'a str,
    }

    struct RecipeDatabase {
        delegate: BytesDatabase,
    }

    impl RecipeDatabase {
        pub fn new(base_dir: &Path) -> Result<RecipeDatabase> {
            Ok(RecipeDatabase {
                delegate: BytesDatabase::new(base_dir)?,
            })
        }

        pub fn add(&mut self, recipe: &Recipe) -> Result<()> {
            let recipe_bytes = serialize(recipe).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Failed to serialize Recipe")
            })?;

            self.delegate.add(recipe.id, recipe_bytes.as_slice())?;

            Ok(())
        }

        pub fn get(&self, recipe_id: u64) -> Option<Recipe> {
            self.delegate.get(recipe_id).map(|data| {
                deserialize(data).expect("Should be able to always deserialize written data")
            })
        }
    }

    fn open_empty() -> RecipeDatabase {
        let tmpdir = tempfile::TempDir::new().unwrap();
        RecipeDatabase::new(&tmpdir.path()).unwrap()
    }

    #[test]
    fn can_open_empty_db() {
        open_empty();
    }

    #[test]
    fn get_on_empty_works() {
        assert_eq!(None, open_empty().get(10));
    }

    fn create_recipe<'a>(id: u64) -> Recipe<'a> {
        Recipe {
            id: id,
            name: "recipe",
        }
    }

    #[test]
    fn can_add_and_get() {
        let mut db = open_empty();

        db.add(&create_recipe(1)).unwrap();
        db.add(&create_recipe(2)).unwrap();
        db.add(&create_recipe(3)).unwrap();

        assert_eq!(create_recipe(1), db.get(1).unwrap());
        assert_eq!(create_recipe(3), db.get(3).unwrap());
        assert_eq!(create_recipe(2), db.get(2).unwrap());
    }

    #[test]
    fn can_load_existing_database() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let db_path = tmpdir.path();
        let mut db = RecipeDatabase::new(db_path).unwrap();

        {
            db.add(&create_recipe(1)).unwrap();
            db.add(&create_recipe(2)).unwrap();
        }

        let existing_db = RecipeDatabase::new(db_path).unwrap();
        assert_eq!(create_recipe(1), existing_db.get(1).unwrap());
        assert_eq!(create_recipe(2), existing_db.get(2).unwrap());
    }
}
