use std::collections::HashMap;
use std::io;
use std::path::Path;

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use bincode::{deserialize, serialize};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::mapped_file::AppendOnlyMappedFile;

type Result<T> = super::Result<T>;

pub struct BytesDatabase {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, usize>,
}

pub trait Database<T> {
    fn add(&mut self, id: u64, obj: &T) -> Result<()>;
    fn get(&self, id: u64) -> Result<Option<T>>;
}

pub struct BincodeDatabase {
    delegate: BytesDatabase,
}

impl BincodeDatabase {
    pub fn new<T: Serialize + DeserializeOwned>(p: &Path) -> Result<Box<impl Database<T>>> {
        Ok(Box::new(BincodeDatabase {
            delegate: BytesDatabase::new(p)?,
        }))
    }
}

impl<T> Database<T> for BincodeDatabase
where
    T: Serialize + DeserializeOwned,
{
    fn add(&mut self, id: u64, obj: &T) -> Result<()> {
        let buf = serialize(obj).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Failed to serialize Recipe")
        })?;

        self.delegate.add(id, buf.as_slice())?;

        Ok(())
    }
    fn get(&self, id: u64) -> Result<Option<T>> {
        self.delegate.get(id)?.map_or(Ok(None), |data| {
            Ok(Some(deserialize::<T>(data).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "deserialize")
            })?))
        })
    }
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

    pub fn get(&self, id: u64) -> Result<Option<&[u8]>> {
        self.index.get(&id).map_or(Ok(None), |offset| {
            self.data.from_offset(*offset).map(|slice| Some(slice))
        })
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    pub struct Recipe {
        pub id: u64,
        name: String,
    }

    impl Recipe {
        fn new(id: u64) -> Recipe {
            Recipe {
                id: id,
                name: "hue".to_owned(),
            }
        }
    }

    fn open_empty<'a>() -> Box<impl Database<Recipe>> {
        let tmpdir = tempfile::TempDir::new().unwrap();
        BincodeDatabase::new::<Recipe>(&tmpdir.path()).unwrap()
    }

    #[test]
    fn can_open_empty_db() {
        open_empty();
    }

    #[test]
    fn get_on_empty_works() {
        assert_eq!(None, open_empty().get(10).unwrap());
    }

    #[test]
    fn can_add_and_get() -> Result<()> {
        let mut db = open_empty();

        let one = Recipe::new(1);
        let two = Recipe::new(2);
        let three = Recipe::new(3);

        db.add(1, &one)?;
        db.add(2, &two)?;
        db.add(3, &three)?;

        assert_eq!(Some(one), db.get(1)?);
        assert_eq!(Some(three), db.get(3)?);
        assert_eq!(Some(two), db.get(2)?);

        Ok(())
    }

    #[test]
    fn can_load_existing_database() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let db_path = tmpdir.path();

        let mut db = BincodeDatabase::new::<Recipe>(&db_path).unwrap();

        {
            db.add(1, &Recipe::new(1)).unwrap();
            db.add(2, &Recipe::new(2)).unwrap();
        }

        let existing_db = BincodeDatabase::new::<Recipe>(&db_path).unwrap();
        assert_eq!(Some(Recipe::new(1)), existing_db.get(1).unwrap());
        assert_eq!(Some(Recipe::new(2)), existing_db.get(2).unwrap());
    }
}
