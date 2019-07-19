use memmap::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
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

        let mut db = AppendOnlyMappedFile {
            file: file,
            mmap: None,
        };

        db.remap()?;

        Ok(db)
    }

    pub fn len(&self) -> usize {
        self.mmap.as_ref().map_or(0, |mmap| mmap.len())
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        // XXX would it be better to grow+remap+write instead?
        self.file.write_all(data)?;
        self.remap()?;
        Ok(())
    }

    fn remap(&mut self) -> Result<()> {
        if self.file.metadata()?.len() > 0 {
            self.mmap
                .replace(unsafe { MmapOptions::new().map_mut(&self.file)? });
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        self.mmap.as_ref().map_or(Ok(()), |mmap| mmap.flush())
    }

    fn read(&self, offset: usize, length: usize) -> Result<&[u8]> {
        let mmap = self.mmap.as_ref().ok_or(io::Error::new(
            io::ErrorKind::Other,
            "No mmap found. File empty?",
        ))?;

        if (offset + length) > mmap.len() {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Can't read beyond the mmap",
            ))
        } else {
            Ok(&mmap[offset..(offset + length)])
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use tempfile;

    fn open_empty() -> AppendOnlyMappedFile {
        let tmpdir = tempfile::TempDir::new().unwrap();
        AppendOnlyMappedFile::new(&tmpdir.path().join("db.bin")).unwrap()
    }

    #[test]
    fn can_open_empty() {
        open_empty();
    }

    #[test]
    fn can_flush_empty_db() {
        open_empty().flush().unwrap();
    }

    #[test]
    fn can_remap_empty_db() {
        open_empty().remap().unwrap();
    }

    #[test]
    #[should_panic]
    fn cannot_read_empty_db() {
        open_empty().read(0, 1).unwrap();
    }

    #[test]
    fn can_append_and_read_on_empty() {
        let mut db = open_empty();
        let data: &[u8] = &[1, 2, 3, 4, 5];

        db.append(data).unwrap();

        assert_eq!(data, db.read(0, data.len()).unwrap());
    }

    #[test]
    fn length_grows_along_with_append_size() {
        let mut db = open_empty();

        assert_eq!(0, db.len());

        db.append(&[1]).unwrap();
        assert_eq!(1, db.len());

        db.append(&[2]).unwrap();
        assert_eq!(2, db.len());

        db.append(&[3, 4, 5]).unwrap();
        assert_eq!(5, db.len());
    }

    #[test]
    #[allow(unused_must_use)]
    fn cannot_read_beyond_map() {
        let mut db = open_empty();

        db.append(&[1, 2, 3, 4, 5]).unwrap();

        db.read(0, 6).map(|_| panic!("length > db.len()"));
        db.read(6, 2).map(|_| panic!("offset > db.len()"));
        db.read(5, 3).map(|_| panic!("offset + length > db.len()"));
    }

    #[test]
    fn can_operate_existing_db() {
        let tmpdir = tempfile::TempDir::new().unwrap();
        let db_path = tmpdir.path().join("db.bin");

        {
            let mut db = AppendOnlyMappedFile::new(&db_path).unwrap();
            db.append(&[1, 2, 3]).unwrap();
            db.flush().unwrap();
        } // db goes out of scope, all should be synced

        let mut db = AppendOnlyMappedFile::new(&db_path).unwrap();
        db.append(&[4, 5]).unwrap();
        assert_eq!(&[1, 2, 3, 4, 5], db.read(0, 5).unwrap())
    }
}
