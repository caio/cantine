use memmap::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;

type Result<T> = super::Result<T>;

pub struct AppendOnlyMappedFile {
    file: File,
    mmap: Option<MmapMut>,
}

impl AppendOnlyMappedFile {
    pub fn new(path: &Path) -> Result<AppendOnlyMappedFile> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;

        let mut db = AppendOnlyMappedFile { file, mmap: None };

        db.remap()?;

        Ok(db)
    }

    pub fn len(&self) -> usize {
        self.mmap.as_ref().map_or(0, |mmap| mmap.len())
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        let current_len = self.len();
        let target_size = current_len + data.len();

        self.file.set_len(target_size as u64)?;
        self.remap()?;

        let mmap = self.mmap.as_mut().expect("Impossible?");
        mmap[current_len..].copy_from_slice(data);
        Ok(())
    }

    fn remap(&mut self) -> Result<()> {
        if self.file.metadata()?.len() > 0 {
            self.mmap
                .replace(unsafe { MmapOptions::new().map_mut(&self.file)? });
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn flush(&self) -> Result<()> {
        self.mmap.as_ref().map_or(Ok(()), |mmap| mmap.flush())
    }

    pub fn at_offset(&self, offset: usize) -> Result<&[u8]> {
        let mmap = self
            .mmap
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "No mmap found. File empty?"))?;

        if offset > mmap.len() {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Can't read beyond the mmap",
            ))
        } else {
            Ok(&mmap[offset..])
        }
    }

    pub fn each_chunk<F>(&self, chunk_size: usize, mut mapper: F) -> Result<()>
    where
        F: FnMut(&[u8]) -> Result<(bool)>,
    {
        match self.mmap.as_ref().map(|m| m.chunks(chunk_size)) {
            None => Ok(()),
            Some(iter) => {
                for chunk in iter {
                    if !mapper(chunk)? {
                        break;
                    };
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    use byteorder::ReadBytesExt;

    fn open_empty() -> Result<AppendOnlyMappedFile> {
        let tmpdir = tempfile::TempDir::new()?;
        AppendOnlyMappedFile::new(&tmpdir.path().join("db.bin"))
    }

    #[test]
    fn can_open_empty() {
        assert!(open_empty().is_ok());
    }

    #[test]
    fn can_flush_empty_db() -> Result<()> {
        open_empty()?.flush()
    }

    #[test]
    fn can_remap_empty_db() -> Result<()> {
        open_empty()?.remap()
    }

    #[test]
    fn cannot_read_empty_db() -> Result<()> {
        assert!(open_empty()?.at_offset(0).is_err());
        Ok(())
    }

    #[test]
    fn can_append_and_read_on_empty() -> Result<()> {
        let mut db = open_empty()?;
        let data: &[u8] = &[1, 2, 3, 4, 5];

        db.append(data)?;

        assert_eq!(data, db.at_offset(0)?);
        Ok(())
    }

    #[test]
    fn length_grows_along_with_append_size() -> Result<()> {
        let mut db = open_empty()?;

        assert_eq!(0, db.len());

        db.append(&[1])?;
        assert_eq!(1, db.len());

        db.append(&[2])?;
        assert_eq!(2, db.len());

        db.append(&[3, 4, 5])?;
        assert_eq!(5, db.len());

        Ok(())
    }

    #[test]
    fn cannot_read_beyond_map() -> Result<()> {
        let mut db = open_empty()?;

        db.append(&[1, 2, 3, 4, 5])?;
        assert!(db.at_offset(6).is_err());
        Ok(())
    }

    #[test]
    fn can_operate_existing_db() -> Result<()> {
        let tmpdir = tempfile::TempDir::new()?;
        let db_path = tmpdir.path().join("db.bin");

        {
            let mut db = AppendOnlyMappedFile::new(&db_path)?;
            db.append(&[1, 2, 3])?;
            db.flush()?;
        } // db goes out of scope, all should be synced

        let mut db = AppendOnlyMappedFile::new(&db_path)?;
        db.append(&[4, 5])?;
        assert_eq!(&[1, 2, 3, 4, 5], db.at_offset(0)?);

        Ok(())
    }

    #[test]
    fn can_use_chunks_on_empty() -> Result<()> {
        let mut tick = 0;

        open_empty()?.each_chunk(1, |_| {
            tick += 1;
            Ok(true)
        })?;

        assert_eq!(0, tick);
        Ok(())
    }

    #[test]
    fn chunking_works() -> Result<()> {
        let mut db = open_empty()?;
        db.append(&[1, 2, 3, 4, 5, 6])?;

        db.each_chunk(2, |chunk| {
            let mut cursor = io::Cursor::new(&chunk);
            assert_eq!(cursor.read_u8()? + 1, cursor.read_u8()?);
            Ok(true)
        })?;

        Ok(())
    }

    #[test]
    fn can_stop_chunking() -> Result<()> {
        let mut db = open_empty()?;
        db.append(&[1, 2, 3, 4, 5, 6])?;

        let mut tick = 0;
        db.each_chunk(1, |chunk| {
            tick += 1;
            Ok(chunk[0] < 5)
        })?;

        assert_eq!(5, tick);
        Ok(())
    }
}
