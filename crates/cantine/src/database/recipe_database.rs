use std::collections::HashMap;
use std::io;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use super::mapped_file::AppendOnlyMappedFile;

#[allow(dead_code, unused_imports)]
use super::recipe_generated::{get_root_as_recipe, Recipe, RecipeArgs};

type Result<T> = super::Result<T>;

pub struct RecipeDatabase {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, usize>,
}

impl RecipeDatabase {
    pub fn new(base_dir: &Path) -> Result<RecipeDatabase> {
        let mut db = RecipeDatabase {
            index: HashMap::new(),
            log: AppendOnlyMappedFile::new(&base_dir.join("log.bin"))?,
            data: AppendOnlyMappedFile::new(&base_dir.join("data.bin"))?,
        };

        db.process_log()?;

        Ok(db)
    }

    fn process_log(&mut self) -> Result<()> {
        let index = &mut self.index;
        self.log.each_chunk(16, |chunk| {
            let mut cursor = io::Cursor::new(&chunk);
            let id = cursor.read_u64::<LittleEndian>()?;
            let offset = cursor.read_u64::<LittleEndian>()?;

            // So, when a id is already known it gets replaced
            // TODO decide wether to clean the db somehow (externally?)
            index.insert(id, offset as usize);
            Ok(())
        })?;

        Ok(())
    }

    pub fn add(&mut self, recipe: &Recipe) -> Result<()> {
        let id = recipe.id();
        let cur_offset = self.data.len();

        self.data.append(recipe._tab.buf)?;

        // XXX I'm sure this can be better
        let mut buf = Vec::with_capacity(16);
        buf.write_u64::<LittleEndian>(id)?;
        buf.write_u64::<LittleEndian>(cur_offset as u64)?;

        self.log.append(buf.as_mut_slice())?;

        self.index.insert(id, cur_offset);

        Ok(())
    }

    pub fn get(&self, recipe_id: u64) -> Option<Recipe> {
        self.index.get(&recipe_id).map_or(None, |offset| {
            // Care about read errors?
            self.data
                .from_offset(*offset)
                .ok()
                .map(|data| get_root_as_recipe(&data))
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use flatbuffers::FlatBufferBuilder;

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

    fn create_recipe<'a>(fbb: &'a mut FlatBufferBuilder, id: u64) -> Recipe<'a> {
        fbb.reset();
        let offset = Recipe::create(
            fbb,
            &RecipeArgs {
                id: id,
                ..Default::default()
            },
        );
        fbb.finish(offset, None);
        get_root_as_recipe(fbb.finished_data())
    }

    #[test]
    fn can_add_and_get() {
        let mut db = open_empty();
        let mut fbb = FlatBufferBuilder::new();

        db.add(&create_recipe(&mut fbb, 1)).unwrap();
        db.add(&create_recipe(&mut fbb, 2)).unwrap();
        db.add(&create_recipe(&mut fbb, 3)).unwrap();

        assert_eq!(1, db.get(1).unwrap().id());
        assert_eq!(3, db.get(3).unwrap().id());
        assert_eq!(2, db.get(2).unwrap().id());
    }

    #[test]
    fn can_load_existing_database() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let db_path = tmpdir.path();

        {
            let mut db = RecipeDatabase::new(db_path).unwrap();
            let mut fbb = FlatBufferBuilder::new();

            db.add(&create_recipe(&mut fbb, 1)).unwrap();
            db.add(&create_recipe(&mut fbb, 2)).unwrap();
        }

        let existing_db = RecipeDatabase::new(db_path).unwrap();
        assert_eq!(1, existing_db.get(1).unwrap().id());
        assert_eq!(2, existing_db.get(2).unwrap().id());
    }
}
