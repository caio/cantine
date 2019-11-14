use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, prelude::*, BufReader, Result, SeekFrom},
    marker::PhantomData,
    mem::size_of,
    path::Path,
};

use bincode::{deserialize, serialize};
use byteorder::NativeEndian;
use serde::{de::DeserializeOwned, Serialize};
use uuid::{self, Uuid};
use zerocopy::{AsBytes, ByteSlice, FromBytes, LayoutVerified, U64};

use super::mapped_file::MappedFile;

pub struct BincodeDatabase<T> {
    log: File,
    data: MappedFile,

    uuid_index: HashMap<Uuid, usize>,
    id_index: HashMap<u64, usize>,

    _marker: PhantomData<T>,
}

const LOG_FILE: &str = "log.bin";
const DATA_FILE: &str = "data.bin";

pub trait DatabaseRecord {
    fn get_id(&self) -> u64;
    fn get_uuid(&self) -> &Uuid;
}

impl<T> BincodeDatabase<T>
where
    T: Serialize + DeserializeOwned + DatabaseRecord,
{
    pub fn create<P: AsRef<Path>>(base_dir: P, initial_size: u64) -> Result<Self> {
        let log_path = base_dir.as_ref().join(LOG_FILE);
        let data_path = base_dir.as_ref().join(DATA_FILE);

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
            let mut log = File::create(log_path)?;
            log.write_all(LogHeader::empty().as_bytes())?;

            let data = File::create(data_path)?;
            data.set_len(initial_size)?;

            BincodeDatabase::open(base_dir)
        }
    }

    pub fn open<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let mut log = OpenOptions::new()
            .read(true)
            .write(true)
            .open(base_dir.as_ref().join(LOG_FILE))?;

        let mut header_buf = vec![0u8; LOG_HEADER_LEN];
        log.read_exact(&mut header_buf)?;

        let header = if let Some(verified) = LayoutVerified::new(header_buf.as_slice()) {
            LogHeaderSlice(verified).0
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "No header found in log",
            ));
        };

        let expected_index_size = header.unique_items.get() as usize;

        let mut id_index = HashMap::with_capacity(expected_index_size);
        let mut uuid_index = HashMap::with_capacity(expected_index_size);

        let mut log_reader = BufReader::with_capacity(500 * LOG_ENTRY_LEN, &log);
        loop {
            let buf = log_reader.fill_buf()?;

            if buf.is_empty() {
                break;
            }

            let mut bytes_consumed = 0;
            for chunk in buf.chunks_exact(LOG_ENTRY_LEN) {
                bytes_consumed += LOG_ENTRY_LEN;

                if let Some(verified) = LayoutVerified::new(chunk) {
                    let entry = LogEntrySlice(verified).0;
                    // No removals, the offsets are always increasing
                    let read_offset = entry.offset.get() as usize;
                    // Updates are simply same id, larger offset
                    uuid_index.insert(Uuid::from_bytes(entry.uuid), read_offset);
                    id_index.insert(entry.id.get(), read_offset);
                } else {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Log corrupted!"));
                }
            }
            assert!(bytes_consumed == buf.len());

            log_reader.consume(bytes_consumed);
        }

        let datafile = OpenOptions::new()
            .read(true)
            .append(true)
            .open(base_dir.as_ref().join(DATA_FILE))?;
        let mut data = MappedFile::open(datafile)?;

        if id_index.len() != expected_index_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Log claims {} unique items, but found {}",
                    expected_index_size,
                    id_index.len()
                ),
            ));
        }

        data.set_append_offset(header.data_offset.get() as usize)?;

        Ok(BincodeDatabase {
            log,
            data,
            uuid_index,
            id_index,
            _marker: PhantomData,
        })
    }

    pub fn add(&mut self, obj: &T) -> Result<()> {
        let data = serialize(obj).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Failed to serialize data being added",
            )
        })?;

        let data_len = data.len();
        let read_offset = self.data.append(data.as_slice())?;

        let id = obj.get_id();
        let uuid = obj.get_uuid();

        let entry = LogEntry::new(uuid, id, read_offset);
        self.log.write_all(entry.as_bytes())?;

        self.id_index.insert(id, read_offset);
        self.uuid_index.insert(*uuid, read_offset);

        self.update_header(read_offset + data_len)?;

        Ok(())
    }

    fn update_header(&mut self, append_offset: usize) -> Result<()> {
        let new_header = LogHeader::new(self.id_index.len(), append_offset);

        // XXX unix lets me write at offset without fidling with seek
        self.log.seek(SeekFrom::Start(0))?;
        self.log.write_all(new_header.as_bytes())?;
        self.log.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn deserialize_at(&self, offset: usize) -> Result<Option<T>> {
        Ok(Some(deserialize(&self.data[offset..]).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to deserialize data at offset {}", offset),
            )
        })?))
    }

    pub fn get_by_id(&self, id: u64) -> Result<Option<T>> {
        match self.id_index.get(&id) {
            Some(&offset) => self.deserialize_at(offset),
            None => Ok(None),
        }
    }

    pub fn get_by_uuid(&self, uuid: &Uuid) -> Result<Option<T>> {
        match self.uuid_index.get(uuid) {
            Some(&offset) => self.deserialize_at(offset),
            None => Ok(None),
        }
    }
}

const LOG_HEADER_LEN: usize = size_of::<LogHeader>();
const LOG_ENTRY_LEN: usize = size_of::<LogEntry>();

#[derive(FromBytes, AsBytes, Debug)]
#[repr(C)]
struct LogHeader {
    unique_items: U64<NativeEndian>,
    data_offset: U64<NativeEndian>,
}

impl LogHeader {
    fn empty() -> Self {
        Self::new(0, 0)
    }

    fn new(num_items: usize, offset: usize) -> Self {
        Self {
            unique_items: U64::new(num_items as u64),
            data_offset: U64::new(offset as u64),
        }
    }
}

struct LogHeaderSlice<B: ByteSlice>(LayoutVerified<B, LogHeader>);

#[derive(FromBytes, AsBytes)]
#[repr(C)]
struct LogEntry {
    uuid: uuid::Bytes,
    id: U64<NativeEndian>,
    offset: U64<NativeEndian>,
}

impl LogEntry {
    fn new(uuid: &Uuid, id: u64, offset: usize) -> Self {
        Self {
            id: U64::new(id),
            uuid: *uuid.as_bytes(),
            offset: U64::new(offset as u64),
        }
    }
}

struct LogEntrySlice<B: ByteSlice>(LayoutVerified<B, LogEntry>);

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Copy)]
    struct Item(u64, Uuid);

    impl Item {
        fn new(id: u64) -> Self {
            Self(id, Uuid::new_v4())
        }
    }

    impl DatabaseRecord for Item {
        fn get_id(&self) -> u64 {
            self.0
        }
        fn get_uuid(&self) -> &Uuid {
            &self.1
        }
    }

    fn open_empty() -> Result<BincodeDatabase<Item>> {
        BincodeDatabase::create(tempfile::TempDir::new()?, 10)
    }

    #[test]
    fn can_open_empty_db() {
        open_empty().unwrap();
    }

    #[test]
    fn get_on_empty_works() -> Result<()> {
        assert_eq!(None, open_empty()?.get_by_uuid(&Uuid::new_v4())?);
        assert_eq!(None, open_empty()?.get_by_id(42)?);
        Ok(())
    }

    #[test]
    fn can_add_and_get() -> Result<()> {
        let mut db = open_empty()?;

        let one = Item::new(1);
        let two = Item::new(2);
        let three = Item::new(3);

        db.add(&one)?;
        db.add(&two)?;
        db.add(&three)?;

        assert_eq!(Some(one), db.get_by_id(1)?);
        assert_eq!(Some(three), db.get_by_id(3)?);
        assert_eq!(Some(two), db.get_by_id(2)?);

        Ok(())
    }

    #[test]
    fn add_updates_both_indices_correctly() -> Result<()> {
        let mut db = open_empty()?;

        let item = Item::new(42);
        db.add(&item)?;

        assert_eq!(
            db.get_by_id(item.get_id())?,
            db.get_by_uuid(item.get_uuid())?
        );
        Ok(())
    }

    #[test]
    fn cannot_overwrite_database() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;

        BincodeDatabase::<Item>::create(&tmpdir, 1)?;
        let overwrite_result = BincodeDatabase::<Item>::create(tmpdir, 1);
        assert!(overwrite_result.is_err());

        Ok(())
    }

    #[test]
    fn can_load_existing_database() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;

        const DB_SIZE: u64 = 1_000;

        let one = Item::new(1);
        let two = Item::new(2);
        let three = Item::new(3);

        {
            let mut db = BincodeDatabase::create(&tmpdir, DB_SIZE)?;

            db.add(&one)?;
            db.add(&two)?;
        }

        {
            let mut db = BincodeDatabase::open(&tmpdir)?;
            db.add(&three)?;
        }

        let existing_db = BincodeDatabase::open(&tmpdir)?;
        assert_eq!(Some(one), existing_db.get_by_uuid(one.get_uuid())?);
        assert_eq!(Some(two), existing_db.get_by_uuid(two.get_uuid())?);
        assert_eq!(Some(three), existing_db.get_by_uuid(three.get_uuid())?);

        // Shouldn't have grown from DB_SIZE
        let data_file = OpenOptions::new()
            .read(true)
            .open(tmpdir.path().join(DATA_FILE))?;
        assert_eq!(DB_SIZE, data_file.metadata()?.len());

        Ok(())
    }
}
