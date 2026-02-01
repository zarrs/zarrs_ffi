use zarrs::{
    array::{Array, ArrayBytes, ArraySubset},
    storage::ReadableWritableStorageTraits,
};

use crate::{LAST_ERROR, ZarrsResult};

use super::{ZarrsArray, ZarrsArrayEnum};

fn zarrsArrayStoreSubsetImpl<T: ReadableWritableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
    array_subset: &ArraySubset,
    subset_bytes: &[u8],
) -> ZarrsResult {
    let array_bytes: ArrayBytes<'static> = ArrayBytes::new_flen(subset_bytes.to_vec());
    if let Err(err) = array.store_array_subset(array_subset, array_bytes) {
        *LAST_ERROR.lock().unwrap() = err.to_string();
        ZarrsResult::ZARRS_ERROR_ARRAY
    } else {
        ZarrsResult::ZARRS_SUCCESS
    }
}

/// Store an array subset.
///
/// `pSubsetStart` and `pSubsetShape` are pointers to arrays of length `dimensionality` holding the chunk start and shape respectively.
/// `pSubsetBytes` is a pointer to an array of bytes of length `subsetBytesCount` that must match the expected size of the subset as returned by `zarrsArrayGetSubsetSize()`.
///
/// # Errors
/// Returns an error if the array does not have read/write capability.
///
/// # Safety
/// `array`  must be a valid `ZarrsArray` handle.
/// `dimensionality` must match the dimensionality of the array and the length of the arrays pointed to by `pSubsetStart` and `pSubsetShape`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsArrayStoreSubset(
    array: ZarrsArray,
    dimensionality: usize,
    pSubsetStart: *const u64,
    pSubsetShape: *const u64,
    subsetBytesCount: usize,
    pSubsetBytes: *const u8,
) -> ZarrsResult {
    // Validation
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    // SAFETY: array is not null, and the caller guarantees it is a valid ZarrsArray handle.
    let array = unsafe { &**array };
    // SAFETY: pSubsetStart and pSubsetShape point to arrays of length dimensionality per the function's safety contract.
    let subset_start = unsafe { std::slice::from_raw_parts(pSubsetStart, dimensionality) };
    let subset_shape = unsafe { std::slice::from_raw_parts(pSubsetShape, dimensionality) };
    // SAFETY: pSubsetBytes points to an array of length subsetBytesCount per the function's safety contract.
    let subset_bytes = unsafe { std::slice::from_raw_parts(pSubsetBytes, subsetBytesCount) };
    let array_subset = ArraySubset::from(
        std::iter::zip(subset_start, subset_shape).map(|(&start, &shape)| start..start + shape),
    );

    // Store the subset bytes
    match array {
        ZarrsArrayEnum::RW(array) => zarrsArrayStoreSubsetImpl(array, &array_subset, subset_bytes),
        ZarrsArrayEnum::RWL(array) => zarrsArrayStoreSubsetImpl(array, &array_subset, subset_bytes),
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have read/write capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}
