use std::collections::HashMap;
use std::io;
use std::path::Path;

use bincode;
use byteorder::LittleEndian;
use serde::{de::DeserializeOwned, Serialize};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, U64};

use super::mapped_file::AppendOnlyMappedFile;

type Result<T> = super::Result<T>;

#[derive(FromBytes, AsBytes)]
#[repr(C)]
struct LogEntry {
    id: U64<LittleEndian>,
    offset: U64<LittleEndian>,
}

struct LogEntrySlice<'a> {
    entry: LayoutVerified<&'a [u8], LogEntry>,
    #[allow(dead_code)]
    body: &'a [u8],
}

pub trait Database<T> {
    fn add(&mut self, id: u64, obj: &T) -> Result<()>;
    fn get(&self, id: u64) -> Result<Option<T>>;
}

pub struct BincodeDatabase {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, usize>,
}

impl BincodeDatabase {
    pub fn new<T: Serialize + DeserializeOwned>(base_dir: &Path) -> Result<Box<impl Database<T>>> {
        let mut index = HashMap::new();
        let mut max_offset = 0;

        // TODO flock() {log,data}.bin

        let log = AppendOnlyMappedFile::new(&base_dir.join("log.bin"))?;
        log.each_chunk(16, |chunk| {
            let (entry, body) =
                LayoutVerified::new_from_prefix(chunk).expect("Failure reading log. Corrupted?");

            let slice = LogEntrySlice { entry, body };

            // No removals, the offsets are always increasing
            max_offset = slice.entry.offset.get();
            // Updates are simply same id, larger offset
            index.insert(slice.entry.id.get(), max_offset as usize);
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
            Ok(Box::new(BincodeDatabase {
                index: index,
                log: log,
                data: data,
            }))
        }
    }
}

impl<T> Database<T> for BincodeDatabase
where
    T: Serialize + DeserializeOwned,
{
    fn add(&mut self, id: u64, obj: &T) -> Result<()> {
        let data = bincode::serialize(obj)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to serialize"))?;

        let cur_offset = self.data.len();
        self.data.append(data.as_slice())?;

        let entry = LogEntry {
            id: U64::new(id),
            offset: U64::new(cur_offset as u64),
        };
        self.log.append(&entry.as_bytes())?;

        self.index.insert(id, cur_offset);
        Ok(())
    }

    fn get(&self, id: u64) -> Result<Option<T>> {
        match self.index.get(&id) {
            None => Ok(None),

            Some(&offset) => {
                let found = self.data.from_offset(offset)?;
                Ok(Some(bincode::deserialize(found).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Failed to deserialize")
                })?))
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use serde::Deserialize;

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

    fn open_empty<'a>() -> Result<Box<impl Database<Recipe>>> {
        let tmpdir = tempfile::TempDir::new().unwrap();
        BincodeDatabase::new::<Recipe>(&tmpdir.path())
    }

    #[test]
    fn can_open_empty_db() {
        open_empty().unwrap();
    }

    #[test]
    fn get_on_empty_works() -> Result<()> {
        assert_eq!(None, open_empty()?.get(10)?);
        Ok(())
    }

    #[test]
    fn can_add_and_get() -> Result<()> {
        let mut db = open_empty()?;

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
    fn can_load_existing_database() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let db_path = tmpdir.path();

        let mut db = BincodeDatabase::new::<Recipe>(&db_path)?;

        {
            db.add(1, &Recipe::new(1))?;
            db.add(2, &Recipe::new(2))?;
        }

        let existing_db = BincodeDatabase::new::<Recipe>(&db_path)?;
        assert_eq!(Some(Recipe::new(1)), existing_db.get(1)?);
        assert_eq!(Some(Recipe::new(2)), existing_db.get(2)?);

        Ok(())
    }
}
