use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Result, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
};

use byteorder::NativeEndian;
use memmap::Mmap;
use serde::{de::Deserialize, Serialize};
use uuid::{self, Uuid};
use zerocopy::{AsBytes, FromBytes, U64};

use super::structuredlog::StructuredLog;

pub trait DatabaseRecord {
    fn get_id(&self) -> u64;
    fn get_uuid(&self) -> uuid::Bytes;
}

pub struct DatabaseReader<T> {
    uuid_index: HashMap<Uuid, u64>,
    id_index: HashMap<u64, usize>,
    data: Mmap,
    _marker: PhantomData<T>,
}

impl<'a, T: Deserialize<'a>> DatabaseReader<T> {
    pub fn open<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        let log = StructuredLog::new(base_dir.as_ref().join(OFFSETS_FILE))?;
        let num_items = log.len()?;

        let mut id_index = HashMap::with_capacity(num_items);
        let mut uuid_index = HashMap::with_capacity(num_items);

        log.for_each_entry(|entry: &LogEntry| {
            let offset = entry.offset.get() as usize;
            let id = entry.id.get();
            id_index.insert(id, offset);
            uuid_index.insert(Uuid::from_bytes(entry.uuid), id);
        })?;

        let datafile = OpenOptions::new()
            .read(true)
            .write(true)
            .open(base_dir.as_ref().join(DATA_FILE))?;

        Ok(Self {
            id_index,
            uuid_index,
            data: unsafe { Mmap::map(&datafile)? },
            _marker: PhantomData,
        })
    }

    pub fn find_by_id(&'a self, id: u64) -> Option<Result<T>> {
        if let Some(&offset) = self.id_index.get(&id) {
            Some(bincode::deserialize(&self.data[offset..]).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Failure decoding at offset")
            }))
        } else {
            None
        }
    }

    pub fn find_by_uuid(&'a self, uuid: &Uuid) -> Option<Result<T>> {
        if let Some(&id) = self.uuid_index.get(uuid) {
            self.find_by_id(id)
        } else {
            None
        }
    }

    pub fn id_for_uuid(&self, uuid: &Uuid) -> Option<&u64> {
        self.uuid_index.get(uuid)
    }
}

pub struct DatabaseWriter<T> {
    log: StructuredLog<LogEntry>,
    writer: BufWriter<File>,
    _marker: PhantomData<T>,
}

impl<T> DatabaseWriter<T>
where
    T: DatabaseRecord + Serialize,
{
    pub fn new<P: AsRef<Path>>(base_dir: P) -> Result<Self> {
        Ok(Self {
            writer: BufWriter::new(File::create(base_dir.as_ref().join(DATA_FILE))?),
            log: StructuredLog::new(base_dir.as_ref().join(OFFSETS_FILE))?,
            _marker: PhantomData,
        })
    }

    pub fn append(&mut self, item: &T) -> Result<()> {
        let encoded = bincode::serialize(item)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Failure encoding input"))?;
        let offset = self.writer.seek(SeekFrom::Current(0))?;
        self.writer.write_all(&encoded)?;

        let entry = LogEntry::new(item.get_id(), item.get_uuid(), offset);
        self.log.append(&entry)?;
        Ok(())
    }
}

const OFFSETS_FILE: &str = "offsets.bin";
const DATA_FILE: &str = "data.bin";

#[derive(FromBytes, AsBytes)]
#[repr(C)]
struct LogEntry {
    uuid: uuid::Bytes,
    id: U64<NativeEndian>,
    offset: U64<NativeEndian>,
}

impl LogEntry {
    fn new(id: u64, uuid: uuid::Bytes, offset: u64) -> Self {
        Self {
            uuid,
            id: U64::new(id),
            offset: U64::new(offset),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct Named<'a>(u64, Uuid, &'a str);

    impl<'a> DatabaseRecord for Named<'a> {
        fn get_id(&self) -> u64 {
            self.0
        }

        fn get_uuid(&self) -> uuid::Bytes {
            *self.1.as_bytes()
        }
    }

    #[test]
    fn usage() -> Result<()> {
        let basedir = tempfile::tempdir()?;

        let mut db_writer = DatabaseWriter::new(basedir.path())?;

        let entries = vec![
            Named(0, Uuid::new_v4(), "a"),
            Named(1, Uuid::new_v4(), "b"),
            Named(2, Uuid::new_v4(), "c"),
            Named(3, Uuid::new_v4(), "d"),
        ];

        for entry in entries.iter() {
            db_writer.append(entry)?;
        }

        // So it flushes
        drop(db_writer);

        let db_reader = DatabaseReader::open(basedir)?;

        for entry in entries.into_iter() {
            let id = entry.get_id();
            let uuid = Uuid::from_bytes(entry.get_uuid());
            let entry = Some(entry);

            assert_eq!(entry, db_reader.find_by_id(id).transpose().ok().flatten());

            assert_eq!(
                entry,
                db_reader.find_by_uuid(&uuid).transpose().ok().flatten()
            );
        }

        Ok(())
    }
}
