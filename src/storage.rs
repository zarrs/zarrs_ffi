use ffi_support::FfiStr;
use std::sync::Arc;

use crate::{LAST_ERROR, ZarrsResult};

#[doc(hidden)]
#[allow(clippy::upper_case_acronyms)]
pub enum ZarrsStorageEnum {
    R(Arc<dyn zarrs::storage::ReadableStorageTraits>),
    W(Arc<dyn zarrs::storage::WritableStorageTraits>),
    L(Arc<dyn zarrs::storage::ListableStorageTraits>),
    RL(Arc<dyn zarrs::storage::ReadableListableStorageTraits>),
    RW(Arc<dyn zarrs::storage::ReadableWritableStorageTraits>),
    RWL(Arc<dyn zarrs::storage::ReadableWritableListableStorageTraits>),
}

#[doc(hidden)]
pub struct ZarrsStorage_T(pub ZarrsStorageEnum);

impl std::ops::Deref for ZarrsStorage_T {
    type Target = ZarrsStorageEnum;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An opaque handle to a zarr store or storage transformer.
pub type ZarrsStorage = *mut ZarrsStorage_T;

/// Create a storage handle to a filesystem store.
///
/// `pStorage` is a pointer to a handle in which the created `ZarrsStorage` is returned.
///
/// # Safety
/// `pStorage` must be a valid pointer to a `ZarrsStorage` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsCreateStorageFilesystem(
    path: FfiStr,
    pStorage: *mut ZarrsStorage,
) -> ZarrsResult {
    let path = std::path::Path::new(path.as_str());
    match zarrs::filesystem::FilesystemStore::new(path) {
        Ok(store) => {
            // SAFETY: pStorage is a valid pointer per the function's safety contract.
            unsafe {
                *pStorage = Box::into_raw(Box::new(ZarrsStorage_T(ZarrsStorageEnum::RW(
                    Arc::new(store),
                ))));
            }
            ZarrsResult::ZARRS_SUCCESS
        }
        Err(err) => {
            *LAST_ERROR.lock().unwrap() = err.to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE
        }
    }
}

/// Destroy storage.
///
/// # Errors
/// Returns `ZarrsResult::ZARRS_ERROR_NULL_PTR` if `storage` is a null pointer.
///
/// # Safety
/// If not null, `storage` must be a valid storage device created with a `zarrsStorage` function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsDestroyStorage(storage: ZarrsStorage) -> ZarrsResult {
    if storage.is_null() {
        ZarrsResult::ZARRS_ERROR_NULL_PTR
    } else {
        // SAFETY: storage is not null, and the caller guarantees it is a valid ZarrsStorage handle.
        unsafe { storage.to_owned().drop_in_place() };
        ZarrsResult::ZARRS_SUCCESS
    }
}
