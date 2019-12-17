use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Result, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::Path,
};

use byteorder::NativeEndian;
use memmap::Mmap;
use serde::Serialize;
use uuid::{self, Uuid};
use zerocopy::{AsBytes, FromBytes, U64};

use super::{
    config::{Decoder, Encoder},
    structuredlog::StructuredLog,
};

pub trait DatabaseRecord {
    fn get_id(&self) -> u64;
    fn get_uuid(&self) -> uuid::Bytes;
}

pub struct DatabaseReader<'a, T, TDecoder: Decoder<'a, Item = T>> {
    data: TypedMmap<'a, T, TDecoder>,
    uuid_index: HashMap<Uuid, u64>,
    id_index: HashMap<u64, usize>,
}

impl<'a, T, TDecoder: Decoder<'a, Item = T>> DatabaseReader<'a, T, TDecoder> {
    pub fn open<P: AsRef<Path>>(base_dir: P, config: TDecoder) -> Result<Self> {
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

        Ok(Self {
            id_index,
            uuid_index,
            data: TypedMmap::with_config(base_dir.as_ref().join(DATA_FILE), config)?,
        })
    }

    pub fn find_by_id(&self, id: u64) -> Result<Option<TDecoder::Item>> {
        if let Some(&offset) = self.id_index.get(&id) {
            Ok(Some(self.data.get(offset)?))
        } else {
            Ok(None)
        }
    }

    pub fn find_by_uuid(&self, uuid: &Uuid) -> Result<Option<TDecoder::Item>> {
        if let Some(&id) = self.uuid_index.get(uuid) {
            self.find_by_id(id)
        } else {
            Ok(None)
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

    pub fn append<'a, TEncoder: Encoder<'a, Item = T>>(
        &mut self,
        item: &'a TEncoder::Item,
    ) -> Result<()> {
        if let Some(encoded) = TEncoder::to_bytes(item) {
            let offset = self.writer.seek(SeekFrom::Current(0))?;
            self.writer.write_all(&encoded)?;

            let entry = LogEntry::new(item.get_id(), item.get_uuid(), offset);
            self.log.append(&entry)?;
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Failure encoding input",
            ))
        }
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

struct TypedMmap<'a, T, TDecoder>
where
    T: 'a,
    TDecoder: Decoder<'a, Item = T>,
{
    data: Mmap,
    _file: File,
    _config: TDecoder,
    _marker: PhantomData<&'a T>,
}

impl<'a, T: 'a, TDecoder> TypedMmap<'a, T, TDecoder>
where
    TDecoder: Decoder<'a, Item = T>,
{
    pub fn with_config<P: AsRef<Path>>(path: P, _config: TDecoder) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        Ok(Self {
            data: unsafe { Mmap::map(&file)? },
            _file: file,
            _config,
            _marker: PhantomData,
        })
    }

    pub fn get(&self, offset: usize) -> Result<TDecoder::Item> {
        if offset > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Offset too large",
            ));
        }

        let data = self.data[offset..].as_ptr();
        let len = self.data.len() - offset;
        if let Some(decoded) =
            TDecoder::from_bytes(unsafe { std::slice::from_raw_parts(data, len) })
        {
            Ok(decoded)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failure decoding bytes at offset {}", offset),
            ))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use crate::database::BincodeConfig;
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
            db_writer.append::<BincodeConfig<Named>>(entry)?;
        }

        // So it flushes
        drop(db_writer);

        let db_reader = DatabaseReader::open(basedir, BincodeConfig::<Named>::new())?;

        for entry in entries.into_iter() {
            let id = entry.get_id();
            let uuid = Uuid::from_bytes(entry.get_uuid());
            let entry = Some(entry);

            assert_eq!(entry, db_reader.find_by_id(id)?);
            assert_eq!(entry, db_reader.find_by_uuid(&uuid)?);
        }

        Ok(())
    }
}
