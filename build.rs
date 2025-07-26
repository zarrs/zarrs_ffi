use std::{env, path::PathBuf};

fn main() {
    let crate_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    #[cfg(feature = "cbindgen")]
    {
        let config = cbindgen::Config::from_file("cbindgen.toml").unwrap();
        cbindgen::generate_with_config(&crate_dir, config)
            .unwrap()
            .write_to_file("zarrs.h");
    }

    let mut shared_object_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    shared_object_dir.push("target");
    shared_object_dir.push(env::var("PROFILE").unwrap());
    let shared_object_dir = shared_object_dir.as_path().to_string_lossy();

    let include_dir = PathBuf::from(&crate_dir);
    let include_dir = include_dir.as_path().to_string_lossy();

    println!(
        "cargo:rustc-env=INLINE_C_RS_CFLAGS=-I{include_dir} -L{shared_object_dir} -D_DEBUG -D_CRT_SECURE_NO_WARNINGS",
    );

    println!(
        "cargo:rustc-env=INLINE_C_RS_LDFLAGS={shared_object_dir}/{lib}",
        shared_object_dir = shared_object_dir,
        lib = if cfg!(target_os = "windows") {
            "zarrs_ffi.dll".to_string()
        } else if cfg!(target_os = "macos") {
            "libzarrs_ffi.dylib".to_string()
        } else {
            "libzarrs_ffi.so".to_string()
        },
    );
}
