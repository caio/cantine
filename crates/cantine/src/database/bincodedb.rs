use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, prelude::*, BufReader, Result},
    marker::PhantomData,
    path::Path,
};

use bincode::{deserialize, serialize, serialized_size};
use byteorder::LittleEndian;
use serde::{de::DeserializeOwned, Serialize};
use zerocopy::{AsBytes, ByteSlice, FromBytes, LayoutVerified, U64};

use super::mapped_file::MappedFile;

pub struct BincodeDatabase<T> {
    log: File,
    data: MappedFile,
    index: HashMap<u64, usize>,
    _marker: PhantomData<T>,
}

const LOG_ENTRY_LEN: usize = 16;
const LOG_FILE: &str = "log.bin";
const DATA_FILE: &str = "data.bin";

impl<T> BincodeDatabase<T>
where
    T: Serialize + DeserializeOwned,
{
    pub fn create(base_dir: &Path, initial_size: u64) -> Result<Self> {
        let log_path = base_dir.join(LOG_FILE);
        let data_path = base_dir.join(DATA_FILE);

        if log_path.exists() || data_path.exists() {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "database files already exist",
            ))
        } else if initial_size == 0 {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "initial_size can't be zero",
            ))
        } else {
            OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(log_path)?;

            let data = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(data_path)?;
            data.set_len(initial_size)?;

            BincodeDatabase::open(base_dir)
        }
    }

    pub fn open(base_dir: &Path) -> Result<Self> {
        let mut index = HashMap::new();
        let mut max_offset = 0;

        let log = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(base_dir.join(LOG_FILE))?;

        let mut log_reader = BufReader::new(&log);
        loop {
            let buf = log_reader.fill_buf()?;

            if buf.is_empty() {
                break;
            }

            let mut num_bytes_read = 0;
            for chunk in buf.chunks_exact(LOG_ENTRY_LEN) {
                num_bytes_read += LOG_ENTRY_LEN;

                if let Some(entry) = LayoutVerified::new(chunk) {
                    let slice = LogEntrySlice(entry);
                    // No removals, the offsets are always increasing
                    max_offset = slice.0.offset.get() as usize;
                    // Updates are simply same id, larger offset
                    index.insert(slice.0.id.get(), max_offset as usize);
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Log corrupted!"));
                }
            }

            log_reader.consume(num_bytes_read);
        }

        let datafile = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(base_dir.join(DATA_FILE))?;
        let mut data = MappedFile::open(datafile)?;

        if max_offset >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index points at unreachable offset",
            ));
        }

        // The data file size might be larger than required
        // so we need to figure out where to start writing from
        // XXX Should be able to make this less awkward
        if max_offset > 0 {
            let last_item: T = deserialize(&data[max_offset..])
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Failed to deserialize"))?;
            let item_size =
                serialized_size(&last_item).expect("size after deserialize doesn't fail") as usize;

            data.set_append_offset(max_offset + item_size)?;
        } else {
            data.set_append_offset(0)?;
        }

        Ok(BincodeDatabase {
            index,
            log,
            data,
            _marker: PhantomData,
        })
    }

    pub fn add(&mut self, id: u64, obj: &T) -> Result<()> {
        let data = serialize(obj).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Failed to serialize data being added",
            )
        })?;

        let read_offset = self.data.append(data.as_slice())?;
        let entry = LogEntry {
            id: U64::new(id),
            offset: U64::new(read_offset as u64),
        };
        self.log.write_all(&entry.as_bytes())?;

        self.index.insert(id, read_offset);
        Ok(())
    }

    pub fn get(&self, id: u64) -> Result<Option<T>> {
        match self.index.get(&id) {
            None => Ok(None),

            Some(&offset) => Ok(Some(deserialize(&self.data[offset..]).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Failed to deserialize stored data",
                )
            })?)),
        }
    }
}

#[derive(FromBytes, AsBytes)]
#[repr(C)]
struct LogEntry {
    id: U64<LittleEndian>,
    offset: U64<LittleEndian>,
}

struct LogEntrySlice<B: ByteSlice>(LayoutVerified<B, LogEntry>);

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Item(u64);

    fn open_empty() -> Result<BincodeDatabase<Item>> {
        let tmpdir = tempfile::TempDir::new().unwrap();
        BincodeDatabase::create(&tmpdir.path(), 10)
    }

    #[test]
    fn can_open_empty_db() {
        open_empty().unwrap();
    }

    #[test]
    fn get_on_empty_works() -> Result<()> {
        assert_eq!(None, open_empty()?.get(1)?);
        Ok(())
    }

    #[test]
    fn can_add_and_get() -> Result<()> {
        let mut db = open_empty()?;

        let one = Item(1);
        let two = Item(2);
        let three = Item(3);

        db.add(1, &one)?;
        db.add(2, &two)?;
        db.add(3, &three)?;

        assert_eq!(Some(one), db.get(1)?);
        assert_eq!(Some(three), db.get(3)?);
        assert_eq!(Some(two), db.get(2)?);

        Ok(())
    }

    #[test]
    fn cannot_overwrite_database() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let db_path = tmpdir.path();

        BincodeDatabase::<Item>::create(&db_path, 1)?;
        let overwrite_result = BincodeDatabase::<Item>::create(&db_path, 1);
        assert!(overwrite_result.is_err());

        Ok(())
    }

    #[test]
    fn can_load_existing_database() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let db_path = tmpdir.path();

        const DB_SIZE: u64 = 1_000;

        {
            let mut db = BincodeDatabase::create(&db_path, DB_SIZE)?;

            db.add(1, &Item(1))?;
            db.add(2, &Item(2))?;
        }

        {
            let mut db = BincodeDatabase::open(&db_path)?;
            db.add(3, &Item(3))?;
        }

        let existing_db = BincodeDatabase::open(&db_path)?;
        assert_eq!(Some(Item(1)), existing_db.get(1)?);
        assert_eq!(Some(Item(2)), existing_db.get(2)?);
        assert_eq!(Some(Item(3)), existing_db.get(3)?);

        let data_file = OpenOptions::new()
            .read(true)
            .open(db_path.join(DATA_FILE))?;
        assert_eq!(DB_SIZE, data_file.metadata()?.len());

        Ok(())
    }
}
