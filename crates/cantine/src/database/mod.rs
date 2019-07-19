mod mapped_file;

#[allow(dead_code, unused_imports)]
mod recipe_generated;

use memmap::{Mmap, MmapOptions};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::prelude::Seek;
use std::io::BufWriter;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use tempfile::TempDir;

use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io;

use recipe_generated::{
    finish_size_prefixed_recipe_buffer, get_size_prefixed_root_as_recipe,
    Recipe as GeneratedRecipe, RecipeArgs,
};

use flatbuffers::FlatBufferBuilder;

pub type Error = io::Error;
pub type Result<T> = io::Result<T>;

pub struct Database {
    log: File,
    data: File,

    mapped: Option<MappedData>,

    index: HashMap<u64, u64>,
}

struct MappedData {
    log: Mmap,
    data: Mmap,
}

trait BytesDatabase {
    fn size(&self) -> usize;
    fn add(&mut self, id: u64, payload: &mut [u8]) -> Result<()>;
    fn get(&self, id: u64) -> Result<()>;
}

impl BytesDatabase for Database {
    fn size(&self) -> usize {
        self.index.len()
    }

    fn add(&mut self, id: u64, payload: &mut [u8]) -> Result<()> {
        self.data.write_all(payload)?;
        let offset = self.data.seek(SeekFrom::Current(0))?;

        self.log.write_u64::<LittleEndian>(id)?;
        self.log.write_u64::<LittleEndian>(offset)?;

        Ok(())
    }

    fn get(&self, id: u64) -> Result<()> {
        Ok(())
    }
}

impl Database {
    pub fn reload(&mut self) -> Result<()> {
        let log_len = self.log.metadata()?.len();
        println!("Log has size={}", log_len);

        // Empty database, nothing to do
        if log_len == 0 {
            return Ok(());
        }

        if let Some(current_mapping) = &self.mapped {
            // No changes, nothing to do
            if (current_mapping.log.len() as u64) == log_len {
                return Ok(());
            }
        }

        let position = self.log.seek(SeekFrom::Current(0))?;

        if position >= log_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Didn't this grow?",
            ));
        }

        let new_data_len = log_len - position;

        if new_data_len % 16 != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Log should only have <u64><u64> pairs",
            ));
        }

        self.mapped.replace(MappedData {
            log: unsafe { Mmap::map(&self.log)? },
            data: unsafe { Mmap::map(&self.data)? },
        });

        let mut new_entries = new_data_len / 16;
        println!("New entries found: {}", new_entries);

        let m = &self.mapped.as_ref().expect("Never happens").log;
        let mut cursor = io::Cursor::new(m);

        while new_entries > 0 {
            let id = cursor.read_u64::<LittleEndian>()?;
            let offset = cursor.read_u64::<LittleEndian>()?;

            println!("Entry[ id: {}, offset: {} ]", id, offset);

            self.index.insert(id, offset);
            new_entries -= 1;
        }

        Ok(())
    }

    pub fn open(base_dir: &Path) -> Result<Database> {
        if !base_dir.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Not a directory",
            ));
        }

        let log = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(base_dir.join("log.bin"))?;

        let data = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(base_dir.join("data.bin"))?;

        let mut db = Database {
            log: log,
            data: data,
            index: HashMap::new(),
            mapped: None,
        };

        db.reload()?;

        Ok(db)
    }
}
