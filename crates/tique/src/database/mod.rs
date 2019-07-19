use std::collections::HashMap;
use std::io;
use std::path::Path;

use byteorder::{LittleEndian, WriteBytesExt};

mod mapped_file;
use mapped_file::AppendOnlyMappedFile;

#[allow(dead_code, unused_imports)]
mod recipe_generated;
use recipe_generated::Recipe;

pub type Result<T> = io::Result<T>;

struct RecipeDatabase {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, usize>,
}

impl RecipeDatabase {
    pub fn new(base_dir: &Path) -> Result<RecipeDatabase> {
        Ok(RecipeDatabase {
            index: HashMap::new(),
            log: AppendOnlyMappedFile::new(&base_dir.join("log.bin"))?,
            data: AppendOnlyMappedFile::new(&base_dir.join("data.bin"))?,
        })
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
                .map(|data| recipe_generated::get_root_as_recipe(&data))
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use flatbuffers::FlatBufferBuilder;
    use recipe_generated::RecipeArgs;

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
        let offset = Recipe::create(fbb, &RecipeArgs { id: id, name: None });
        fbb.finish(offset, None);
        recipe_generated::get_root_as_recipe(fbb.finished_data())
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
}
