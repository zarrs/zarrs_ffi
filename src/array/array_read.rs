use zarrs::{
    array::{Array, ArrayBytes, ArraySubset},
    storage::ReadableStorageTraits,
};

use crate::{LAST_ERROR, ZarrsResult};

use super::{ZarrsArray, ZarrsArrayEnum};

fn zarrsArrayRetrieveChunkImpl<T: ReadableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
    chunk_indices: &[u64],
    chunk_bytes_length: usize,
    chunk_bytes: *mut u8,
) -> ZarrsResult {
    match array.retrieve_chunk::<ArrayBytes>(chunk_indices) {
        Ok(bytes) => {
            let Ok(bytes) = bytes.into_fixed() else {
                *LAST_ERROR.lock().unwrap() =
                    "variable size data types are not supported".to_string();
                return ZarrsResult::ZARRS_ERROR_UNSUPPORTED_DATA_TYPE;
            };
            if bytes.len() != chunk_bytes_length {
                *LAST_ERROR.lock().unwrap() = format!(
                    "chunk_bytes_length {chunk_bytes_length} does not match decoded chunk size {}",
                    bytes.len()
                );
                ZarrsResult::ZARRS_ERROR_BUFFER_LENGTH
            } else {
                unsafe { std::ptr::copy(bytes.as_ptr(), chunk_bytes, chunk_bytes_length) };
                ZarrsResult::ZARRS_SUCCESS
            }
        }
        Err(err) => {
            *LAST_ERROR.lock().unwrap() = err.to_string();
            ZarrsResult::ZARRS_ERROR_ARRAY
        }
    }
}

/// Retrieve a chunk from an array.
///
/// `pChunkIndices` is a pointer to an array of length `dimensionality` holding the chunk indices.
/// `pChunkBytes` is a pointer to an array of bytes of length `chunkBytesCount` that must match the expected size of the chunk as returned by `zarrsArrayGetChunkSize()`.
///
/// # Errors
/// Returns an error if the array does not have read capability.
///
/// # Safety
/// `array` must be a valid `ZarrsArray` handle.
/// `dimensionality` must match the dimensionality of the array and the length of the array pointed to by `pChunkIndices`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsArrayRetrieveChunk(
    array: ZarrsArray,
    dimensionality: usize,
    pChunkIndices: *const u64,
    chunkBytesCount: usize,
    pChunkBytes: *mut u8,
) -> ZarrsResult {
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    // SAFETY: array is not null, and the caller guarantees it is a valid ZarrsArray handle.
    let array = unsafe { &**array };
    // SAFETY: pChunkIndices points to an array of length dimensionality per the function's safety contract.
    let chunk_indices = unsafe { std::slice::from_raw_parts(pChunkIndices, dimensionality) };

    // Get the chunk bytes
    match array {
        ZarrsArrayEnum::R(array) => {
            zarrsArrayRetrieveChunkImpl(array, chunk_indices, chunkBytesCount, pChunkBytes)
        }
        ZarrsArrayEnum::RL(array) => {
            zarrsArrayRetrieveChunkImpl(array, chunk_indices, chunkBytesCount, pChunkBytes)
        }
        ZarrsArrayEnum::RW(array) => {
            zarrsArrayRetrieveChunkImpl(array, chunk_indices, chunkBytesCount, pChunkBytes)
        }
        ZarrsArrayEnum::RWL(array) => {
            zarrsArrayRetrieveChunkImpl(array, chunk_indices, chunkBytesCount, pChunkBytes)
        }
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have read capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}

fn zarrsArrayRetrieveSubsetImpl<T: ReadableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
    array_subset: &ArraySubset,
    subset_bytes_length: usize,
    subset_bytes: *mut u8,
) -> ZarrsResult {
    match array.retrieve_array_subset::<ArrayBytes>(array_subset) {
        Ok(bytes) => {
            let Ok(bytes) = bytes.into_fixed() else {
                *LAST_ERROR.lock().unwrap() =
                    "variable size data types are not supported".to_string();
                return ZarrsResult::ZARRS_ERROR_UNSUPPORTED_DATA_TYPE;
            };
            if bytes.len() != subset_bytes_length {
                *LAST_ERROR.lock().unwrap() = format!(
                    "subset_bytes_length {subset_bytes_length} does not match decoded subset size {}",
                    bytes.len()
                );
                ZarrsResult::ZARRS_ERROR_BUFFER_LENGTH
            } else {
                unsafe { std::ptr::copy(bytes.as_ptr(), subset_bytes, subset_bytes_length) };
                ZarrsResult::ZARRS_SUCCESS
            }
        }
        Err(err) => {
            *LAST_ERROR.lock().unwrap() = err.to_string();
            ZarrsResult::ZARRS_ERROR_ARRAY
        }
    }
}

/// Retrieve a subset from an array.
///
/// `pSubsetStart` and `pSubsetShape` are pointers to arrays of length `dimensionality` holding the chunk start and shape respectively.
/// `pSubsetBytes` is a pointer to an array of bytes of length `subsetBytesCount` that must match the expected size of the subset as returned by `zarrsArrayGetSubsetSize()`.
///
/// # Errors
/// Returns an error if the array does not have read capability.
///
/// # Safety
/// `array` must be a valid `ZarrsArray` handle.
/// `dimensionality` must match the dimensionality of the array and the length of the arrays pointed to by `pSubsetStart` and `pSubsetShape`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsArrayRetrieveSubset(
    array: ZarrsArray,
    dimensionality: usize,
    pSubsetStart: *const u64,
    pSubsetShape: *const u64,
    subsetBytesCount: usize,
    pSubsetBytes: *mut u8,
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
    let array_subset = ArraySubset::from(
        std::iter::zip(subset_start, subset_shape).map(|(&start, &shape)| start..start + shape),
    );

    // Get the subset bytes
    match array {
        ZarrsArrayEnum::R(array) => {
            zarrsArrayRetrieveSubsetImpl(array, &array_subset, subsetBytesCount, pSubsetBytes)
        }
        ZarrsArrayEnum::RL(array) => {
            zarrsArrayRetrieveSubsetImpl(array, &array_subset, subsetBytesCount, pSubsetBytes)
        }
        ZarrsArrayEnum::RW(array) => {
            zarrsArrayRetrieveSubsetImpl(array, &array_subset, subsetBytesCount, pSubsetBytes)
        }
        ZarrsArrayEnum::RWL(array) => {
            zarrsArrayRetrieveSubsetImpl(array, &array_subset, subsetBytesCount, pSubsetBytes)
        }
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have read capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}
