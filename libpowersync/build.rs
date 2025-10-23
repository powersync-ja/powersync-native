use std::env;

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_namespace("powersync::internal")
        .generate()
        .expect("Unable to generate bindings")
        .write_to_file("src/bindings.h");

    cc::Build::new()
        .cpp(true)
        .std("c++17")
        .include("include/")
        .include("src")
        .file("src/powersync.cpp")
        .link_lib_modifier("+whole-archive")
        .compile("powersync_bridge");
    println!("cargo::rerun-if-changed=src/powersync.cpp");
}
