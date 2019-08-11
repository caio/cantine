use std::{collections::HashMap, io, marker::PhantomData, path::Path};

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

pub struct BincodeDatabase<T> {
    log: AppendOnlyMappedFile,
    data: AppendOnlyMappedFile,
    index: HashMap<u64, usize>,
    _marker: PhantomData<T>,
}

const CHUNK_SIZE: usize = 16;

impl<T> BincodeDatabase<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn new(base_dir: &Path) -> Result<Self> {
        let mut index = HashMap::new();
        let mut max_offset = 0;

        // TODO flock() {log,data}.bin

        let log = AppendOnlyMappedFile::new(&base_dir.join("log.bin"))?;
        let mut data_read = 0;
        log.each_chunk(CHUNK_SIZE, |chunk| {
            if let Some((entry, body)) = LayoutVerified::new_from_prefix(chunk) {
                let slice = LogEntrySlice { entry, body };
                // No removals, the offsets are always increasing
                max_offset = slice.entry.offset.get();
                // Updates are simply same id, larger offset
                index.insert(slice.entry.id.get(), max_offset as usize);

                data_read += CHUNK_SIZE;
                Ok(true)
            } else {
                Ok(false)
            }
        })?;

        if data_read != log.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Log corrupted! Aborted at offset {}", data_read),
            ));
        }

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
            Ok(BincodeDatabase {
                index: index,
                log: log,
                data: data,
                _marker: PhantomData,
            })
        }
    }

    pub fn add(&mut self, id: u64, obj: &T) -> Result<()> {
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

    pub fn get(&self, id: u64) -> Result<Option<T>> {
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

    fn open_empty() -> Result<BincodeDatabase<Recipe>> {
        let tmpdir = tempfile::TempDir::new().unwrap();
        BincodeDatabase::new(&tmpdir.path())
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

        let mut db = BincodeDatabase::new(&db_path)?;

        {
            db.add(1, &Recipe::new(1))?;
            db.add(2, &Recipe::new(2))?;
        }

        let existing_db = BincodeDatabase::new(&db_path)?;
        assert_eq!(Some(Recipe::new(1)), existing_db.get(1)?);
        assert_eq!(Some(Recipe::new(2)), existing_db.get(2)?);

        Ok(())
    }
}
