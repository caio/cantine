use std::{
    borrow::Cow,
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Result, Seek, SeekFrom, Write},
    marker::PhantomData,
    mem::size_of,
    path::Path,
};

use bincode::{deserialize, serialize};
use byteorder::NativeEndian;
use memmap::Mmap;
use serde::{Deserialize, Serialize};
use uuid::{self, Uuid};
use zerocopy::{AsBytes, FromBytes, LayoutVerified, U64};

const OFFSETS_FILE: &str = "offsets.bin";
const DATA_FILE: &str = "data.bin";
const DATA_HEADER_SIZE: usize = size_of::<u64>();

pub trait DatabaseRecord {
    fn get_id(&self) -> u64;
    fn get_uuid(&self) -> uuid::Bytes;
}

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

struct StructuredLog<T> {
    file: File,
    _header: PhantomData<T>,
}

impl<T> StructuredLog<T>
where
    T: FromBytes + AsBytes,
{
    fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        if !path.as_ref().exists() {
            File::create(&path)?;
        }

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&path.as_ref())?;

        let entry_len = size_of::<T>();

        let file_size = file.metadata()?.len() as usize;
        if file_size % entry_len != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Expected file to size to be a multiple of {}. Got {}",
                    entry_len, file_size
                ),
            ));
        }

        Ok(Self {
            file,
            _header: PhantomData,
        })
    }

    fn len(&self) -> Result<usize> {
        Ok(self.file.metadata()?.len() as usize / size_of::<T>())
    }

    fn for_each_entry<F>(&self, mut each_entry: F) -> std::io::Result<()>
    where
        F: FnMut(&T),
    {
        let entry_len = size_of::<T>();
        let mut log_reader = BufReader::with_capacity((8192 / entry_len) * entry_len, &self.file);

        loop {
            let buf = log_reader.fill_buf()?;

            if buf.is_empty() {
                break;
            }

            let mut bytes_consumed = 0;
            if let Some(slice) = LayoutVerified::new_slice(buf) {
                let entries: &[T] = slice.into_slice();
                for entry in entries {
                    (each_entry)(entry);
                    bytes_consumed += entry_len;
                }
            } else {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Log corrupted!"));
            }

            log_reader.consume(bytes_consumed);
        }

        Ok(())
    }

    fn append(&mut self, item: &T) -> Result<()> {
        self.file.write_all(item.as_bytes())
    }
}

pub trait Encoder<'a> {
    type Item: 'a;
    fn to_bytes(item: &'a Self::Item) -> Option<Cow<'a, [u8]>>;
}

pub trait Decoder<'a> {
    type Item: 'a;
    fn from_bytes(src: &'a [u8]) -> Option<Self::Item>;
}

pub struct BincodeConfig<T>(PhantomData<T>);

impl<T> BincodeConfig<T> {
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<'a, T: 'a> Encoder<'a> for BincodeConfig<T>
where
    T: Serialize,
{
    type Item = T;

    fn to_bytes(item: &'a T) -> Option<Cow<[u8]>> {
        serialize(item).map(Cow::Owned).ok()
    }
}

impl<'a, T: 'a> Decoder<'a> for BincodeConfig<T>
where
    T: Deserialize<'a> + Clone,
{
    type Item = T;

    fn from_bytes(src: &'a [u8]) -> Option<T> {
        deserialize(src).ok()
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

pub struct DatabaseReader<'a, T, TDecoder: Decoder<'a, Item = T>> {
    data: TypedMmap<'a, T, TDecoder>,
    uuid_index: HashMap<Uuid, usize>,
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
            uuid_index.insert(Uuid::from_bytes(entry.uuid), offset);
            id_index.insert(entry.id.get(), offset);
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
        if let Some(&offset) = self.uuid_index.get(&uuid) {
            Ok(Some(self.data.get(offset)?))
        } else {
            Ok(None)
        }
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

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

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
    fn less_awkward_api() -> Result<()> {
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
