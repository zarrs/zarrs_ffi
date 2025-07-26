use zarrs::{group::Group, storage::WritableStorageTraits};

use crate::{
    group::{ZarrsGroup, ZarrsGroupEnum},
    ZarrsResult, LAST_ERROR,
};

// use super::group_fn;

fn zarrsGroupStoreMetadataImpl<T: WritableStorageTraits + ?Sized + 'static>(
    group: &Group<T>,
) -> ZarrsResult {
    match group.store_metadata() {
        Ok(()) => ZarrsResult::ZARRS_SUCCESS,
        Err(err) => {
            *LAST_ERROR.lock().unwrap() = err.to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE
        }
    }
}

/// Store group metadata.
///
/// # Errors
/// Returns an error if the group does not have write capability.
///
/// # Safety
/// `group` must be a valid `ZarrsGroup` handle.
#[no_mangle]
pub unsafe extern "C" fn zarrsGroupStoreMetadata(group: ZarrsGroup) -> ZarrsResult {
    if group.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let group = &**group;
    match group {
        ZarrsGroupEnum::W(group) => zarrsGroupStoreMetadataImpl(group),
        ZarrsGroupEnum::RW(group) => zarrsGroupStoreMetadataImpl(group),
        ZarrsGroupEnum::RWL(group) => zarrsGroupStoreMetadataImpl(group),
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have write capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}
