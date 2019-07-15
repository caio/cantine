use memmap::Mmap;
use std::fs::OpenOptions;
use std::io::prelude::Seek;
use std::io::Read;
use std::io::SeekFrom;
use std::io::Write;
use tempfile::TempDir;

#[allow(dead_code, unused_imports)]
mod recipe_generated;

use recipe_generated::{Recipe, RecipeArgs};

#[allow(dead_code)]
pub fn run() -> std::io::Result<()> {
    let tmpdir = TempDir::new().expect("Unable to create tempdir");

    let mut db_file = tmpdir.into_path();
    db_file.push("test.mmap");

    println!("{:?}", db_file);

    let mut file = OpenOptions::new()
        .read(true)
        .append(true)
        .create(true)
        .open(db_file.into_os_string())?;

    let mut fb_builder = flatbuffers::FlatBufferBuilder::new();

    let name = fb_builder.create_string("Caio");
    let recipe = Recipe::create(
        &mut fb_builder,
        &RecipeArgs {
            id: 1,
            name: Some(name),
        },
    );

    fb_builder.finish_size_prefixed(recipe, None);

    let ser_data = fb_builder.finished_data();

    file.write_all(ser_data)?;
    file.sync_all()?;

    file.seek(SeekFrom::Start(0))?;

    let mut buf: Vec<u8> = Vec::new();

    file.read_to_end(buf.as_mut())?;

    let r = recipe_generated::get_size_prefixed_root_as_recipe(buf.as_slice());

    println!("Id: {} Name: {}", r.id(), r.name().expect("fml"));

    let mmap = unsafe { Mmap::map(&file).expect("Failed to mmap()") };

    let rw_mmap = mmap.make_mut().expect("Failed to set +w on mmap()ed file");

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_runme() {
        run().unwrap();
    }
}
