use std::{
    fs::File,
    io::{Error, ErrorKind, Result},
    ops::Deref,
};

use memmap::{MmapMut, MmapOptions};

pub(super) struct AppendOnlyMappedFile {
    file: File,
    mmap: MmapMut,
    write_from: usize,
}

impl AppendOnlyMappedFile {
    pub fn open(file: File) -> Result<AppendOnlyMappedFile> {
        let write_from = file.metadata()?.len() as usize;
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        Ok(AppendOnlyMappedFile {
            file,
            mmap,
            write_from,
        })
    }

    pub fn set_write_from(&mut self, write_from: usize) -> Result<()> {
        if write_from <= self.len() {
            self.write_from = write_from;
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                "write_from must be <= len()",
            ))
        }
    }

    pub fn len(&self) -> usize {
        self.mmap.len()
    }

    pub fn append(&mut self, data: &[u8]) -> Result<usize> {
        let read_from = self.write_from;
        let final_size = read_from + data.len();

        if final_size > self.mmap.len() {
            self.file.set_len(final_size as u64)?;
            self.mmap = unsafe { MmapOptions::new().map_mut(&self.file)? };
        }

        self.mmap[read_from..final_size].copy_from_slice(data);
        self.write_from = final_size;
        Ok(read_from)
    }
}

impl Deref for AppendOnlyMappedFile {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        &self.mmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile;

    fn open_empty() -> Result<AppendOnlyMappedFile> {
        let file = tempfile::tempfile()?;
        file.set_len(10)?;
        let db = AppendOnlyMappedFile::open(file)?;
        Ok(db)
    }

    #[test]
    fn open_starts_at_end() -> Result<()> {
        let db = open_empty()?;
        assert_eq!(db.len(), db.write_from);
        Ok(())
    }

    #[test]
    fn cannot_set_offset_beyond_len() -> Result<()> {
        let mut db = open_empty()?;
        assert!(db.set_write_from(db.len() + 1).is_err());
        Ok(())
    }

    #[test]
    fn can_write_and_read() -> Result<()> {
        let mut db = open_empty()?;
        db.set_write_from(0)?;

        let data = [1, 2, 3, 4, 5];
        let read_from = db.append(&data)?;

        assert_eq!(data, db[read_from..data.len()]);
        Ok(())
    }

    #[test]
    fn len_does_not_grow_if_not_needed() -> Result<()> {
        let mut db = open_empty()?;
        let initial_len = db.len();
        db.set_write_from(0)?;
        db.append(&[1, 2, 3])?;
        assert_eq!(initial_len, db.len());
        Ok(())
    }

    #[test]
    fn len_grows_when_appending() -> Result<()> {
        let mut db = open_empty()?;
        let initial_len = db.len();
        db.append(&[1, 2, 3])?;
        assert_eq!(initial_len + 3, db.len());
        Ok(())
    }

    #[test]
    fn len_grows_correctly_at_boundary() -> Result<()> {
        let mut db = open_empty()?;
        let initial_len = db.len();
        let data = [1u8, 2, 3, 4, 5];

        let write_from = initial_len - (data.len() - 2);
        db.set_write_from(write_from)?;
        db.append(&data)?;

        assert_eq!(initial_len + 2, db.len());
        assert_eq!(data, db[write_from..]);
        Ok(())
    }
}
