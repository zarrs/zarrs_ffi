#![doc = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))]
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::{
    ffi::{c_char, CString},
    sync::Mutex,
};

use once_cell::sync::Lazy;

extern crate zarrs;

mod array;
mod group;
mod storage;
mod version;

pub use array::{
    array_read::*, array_read_write::*, array_sharded::*, array_write::*, data_type::*, *,
};
pub use group::{group_write::*, *};
pub use storage::*;
pub use version::*;

#[non_exhaustive]
#[repr(i32)]
pub enum ZarrsResult {
    ZARRS_SUCCESS = 0,
    ZARRS_ERROR_NULL_PTR = -1,
    ZARRS_ERROR_STORAGE = -2,
    ZARRS_ERROR_ARRAY = -3,
    ZARRS_ERROR_BUFFER_LENGTH = -4,
    ZARRS_ERROR_INVALID_INDICES = -5,
    ZARRS_ERROR_NODE_PATH = -6,
    ZARRS_ERROR_STORE_PREFIX = -7,
    ZARRS_ERROR_INVALID_METADATA = -8,
    ZARRS_ERROR_STORAGE_CAPABILITY = -9,
    ZARRS_ERROR_UNKNOWN_CHUNK_GRID_SHAPE = -10,
    ZARRS_ERROR_UNKNOWN_INTERSECTING_CHUNKS = -11,
    ZARRS_ERROR_UNSUPPORTED_DATA_TYPE = -12,
    ZARRS_ERROR_GROUP = -13,
}

static LAST_ERROR: Lazy<Mutex<String>> = Lazy::new(|| Mutex::new("".to_string()));

/// Get the last error string.
///
/// The string must be freed with `zarrsFreeString`.
#[no_mangle]
pub extern "C" fn zarrsLastError() -> *mut c_char {
    let c_str = CString::new(LAST_ERROR.lock().unwrap().as_str()).unwrap();
    c_str.into_raw()
}

/// Free a string created by zarrs.
///
/// # Safety
/// `array` must be a valid string created by zarrs.
#[no_mangle]
pub unsafe extern "C" fn zarrsFreeString(string: *mut c_char) -> ZarrsResult {
    if string.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    unsafe {
        let _ = CString::from_raw(string);
    }
    ZarrsResult::ZARRS_SUCCESS
}
