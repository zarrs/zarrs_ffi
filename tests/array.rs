use inline_c::Assert;

pub fn assert_cxx_str(input_as_string: &str) -> Assert {
    inline_c::run(inline_c::Language::Cxx, input_as_string)
        .map_err(|e| panic!("{}", e))
        .unwrap()
}

#[test]
fn ffi_array_write_rust_read_c() {
    use std::sync::Arc;
    use zarrs::array::{DataType, FillValue};
    use zarrs::filesystem::FilesystemStore;

    let tmp_path = tempfile::tempdir().unwrap();
    std::env::set_var(
        "INLINE_C_RS_TMP_PATH_WRITE_RUST_READ_C",
        tmp_path.path().to_string_lossy().to_string(),
    );

    let store = Arc::new(FilesystemStore::new(tmp_path.path()).unwrap());

    // Create an array
    let array_path = "/array";
    let array = zarrs::array::ArrayBuilder::new(
        vec![8, 8], // array shape
        vec![4, 4], // regular chunk shape
        DataType::Float32,
        FillValue::from(f32::NAN),
    )
    .dimension_names(["y", "x"].into())
    .storage_transformers(vec![].into())
    .build(store.clone(), array_path)
    .unwrap();

    array.store_metadata().unwrap();

    array
        .store_chunk_elements::<f32>(&[0, 0], &(0..16).map(|f| f as f32).collect::<Vec<_>>())
        .unwrap();

    assert_cxx_str(include_str!("array_read.cpp"))
        .success()
        .stdout("64\n0 1 2 3 4 -1 -2 7 8 -3 -4 11 12 13 14 15\n");
}

#[test]
fn ffi_array_write_c_read_c() {
    let tmp_path = tempfile::tempdir().unwrap();
    std::env::set_var(
        "INLINE_C_RS_TMP_PATH_WRITE_C_READ_C",
        tmp_path.path().to_string_lossy().to_string(),
    );

    assert_cxx_str(include_str!("../examples/array_write_read.cpp"))
        .success()
        .stdout("nan nan nan nan nan -1 -2 nan nan -3 -4 nan nan nan nan nan\n");
}
