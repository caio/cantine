use flatc_rust;
use std::path::Path;

fn main() {
    println!("cargo:rerun-if-changed=src/recipe.fbs");
    println!("cargo:rerun-if-changed=src/main.rs");
    flatc_rust::run(flatc_rust::Args {
        inputs: &[Path::new("src/recipe.fbs")],
        out_dir: Path::new("src/database/"),
        ..Default::default()
    })
    .expect("flatc");
}
