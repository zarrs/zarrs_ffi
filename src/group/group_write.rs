use zarrs::{group::Group, storage::WritableStorageTraits};

use crate::{
    LAST_ERROR, ZarrsResult,
    group::{ZarrsGroup, ZarrsGroupEnum},
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsGroupStoreMetadata(group: ZarrsGroup) -> ZarrsResult {
    if group.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    // SAFETY: group is not null, and the caller guarantees it is a valid ZarrsGroup handle.
    let group = unsafe { &**group };
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
