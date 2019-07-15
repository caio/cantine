mod recipe_generated;

struct Database {}

#[cfg(test)]
mod tests {

    use memmap::Mmap;
    use std::fs::OpenOptions;
    use tempfile::TempDir;

    #[test]
    fn test_runme() {
        let tmpdir = TempDir::new().expect("Unable to create tempdir");

        let mut db_file = tmpdir.into_path();
        db_file.push("test.mmap");

        println!("{:?}", db_file);

        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(db_file.into_os_string())
            .expect("Unable to open file");

        // TODO write flatbuffers

        let mmap = unsafe { Mmap::map(&f).expect("Failed to mmap()") };

        let rw_mmap = mmap.make_mut().expect("Failed to set +w on mmap()ed file");
    }
}
