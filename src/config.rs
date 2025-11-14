use std::ffi::{c_char, CString};

use ffi_support::FfiStr;

use crate::{ZarrsResult, LAST_ERROR};

/// Get the global zarrs configuration as a JSON string.
///
/// # Safety
/// The caller must free the returned string with `zarrsStringFree`.
///
/// # Errors
/// Returns `ZARRS_ERROR_CONFIG` if serialization fails.
#[no_mangle]
pub unsafe extern "C" fn zarrsGetGlobalConfig(pJsonString: *mut *mut c_char) -> ZarrsResult {
    if pJsonString.is_null() {
        *LAST_ERROR.lock().unwrap() = "pJsonString is null".to_string();
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }

    let config = zarrs::config::global_config();

    match serde_json::to_string_pretty(&*config) {
        Ok(json_str) => match CString::new(json_str) {
            Ok(cstring) => {
                *pJsonString = cstring.into_raw();
                ZarrsResult::ZARRS_SUCCESS
            }
            Err(e) => {
                *LAST_ERROR.lock().unwrap() = format!("Failed to create C string: {e}");
                ZarrsResult::ZARRS_ERROR_CONFIG
            }
        },
        Err(e) => {
            *LAST_ERROR.lock().unwrap() = format!("Failed to serialize config: {e}");
            ZarrsResult::ZARRS_ERROR_CONFIG
        }
    }
}

/// Set the global zarrs configuration from a JSON string.
///
/// # Safety
/// The caller must provide a valid JSON string representing a zarrs Config.
///
/// # Errors
/// Returns `ZARRS_ERROR_CONFIG` if deserialization fails or the JSON is invalid.
#[no_mangle]
pub unsafe extern "C" fn zarrsSetGlobalConfig(jsonString: FfiStr) -> ZarrsResult {
    let json_str = jsonString.as_str();

    match serde_json::from_str::<zarrs::config::Config>(json_str) {
        Ok(config) => {
            *zarrs::config::global_config_mut() = config;
            ZarrsResult::ZARRS_SUCCESS
        }
        Err(e) => {
            *LAST_ERROR.lock().unwrap() = format!("Failed to deserialize config: {e}");
            ZarrsResult::ZARRS_ERROR_CONFIG
        }
    }
}
