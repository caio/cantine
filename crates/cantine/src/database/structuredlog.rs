use std::{
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, Result, Write},
    marker::PhantomData,
    mem::size_of,
    path::Path,
};

use zerocopy::{AsBytes, FromBytes, LayoutVerified};

pub(crate) struct StructuredLog<T> {
    file: File,
    _header: PhantomData<T>,
}

impl<T> StructuredLog<T>
where
    T: FromBytes + AsBytes,
{
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
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

    pub fn len(&self) -> Result<usize> {
        Ok(self.file.metadata()?.len() as usize / size_of::<T>())
    }

    pub fn for_each_entry<F>(&self, mut each_entry: F) -> std::io::Result<()>
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

    pub fn append(&mut self, item: &T) -> Result<()> {
        self.file.write_all(item.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    use byteorder::NativeEndian;
    use zerocopy::U64;

    #[test]
    fn usage() -> Result<()> {
        let tmpdir = tempfile::tempdir()?;
        let log_path = tmpdir.path().join("testlog");

        {
            let mut log = StructuredLog::new(&log_path)?;

            assert_eq!(0, log.len()?);

            for i in 0..100 {
                log.append(&U64::<NativeEndian>::new(i))?;
            }
        }

        let log = StructuredLog::new(&log_path)?;

        assert_eq!(100, log.len()?);

        let mut wanted: u64 = 0;
        log.for_each_entry(|e: &U64<NativeEndian>| {
            assert_eq!(wanted, e.get());
            wanted += 1;
        })?;

        Ok(())
    }
}
