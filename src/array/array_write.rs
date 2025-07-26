use zarrs::{array::Array, storage::WritableStorageTraits};

use crate::{
    array::{ZarrsArray, ZarrsArrayEnum},
    ZarrsResult, LAST_ERROR,
};

use super::array_fn;

fn zarrsArrayStoreMetadataImpl<T: WritableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
) -> ZarrsResult {
    match array.store_metadata() {
        Ok(()) => ZarrsResult::ZARRS_SUCCESS,
        Err(err) => {
            *LAST_ERROR.lock().unwrap() = err.to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE
        }
    }
}

/// Store array metadata.
///
/// # Errors
/// Returns an error if the array does not have write capability.
///
/// # Safety
/// `array` must be a valid `ZarrsArray` handle.
#[no_mangle]
pub unsafe extern "C" fn zarrsArrayStoreMetadata(array: ZarrsArray) -> ZarrsResult {
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;
    match array {
        ZarrsArrayEnum::W(array) => zarrsArrayStoreMetadataImpl(array),
        ZarrsArrayEnum::RW(array) => zarrsArrayStoreMetadataImpl(array),
        ZarrsArrayEnum::RWL(array) => zarrsArrayStoreMetadataImpl(array),
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have write capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}

fn zarrsArrayStoreChunkImpl<T: WritableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
    chunk_indices: &[u64],
    chunk_bytes: &[u8],
) -> ZarrsResult {
    if let Err(err) = array.store_chunk(chunk_indices, chunk_bytes) {
        *LAST_ERROR.lock().unwrap() = err.to_string();
        ZarrsResult::ZARRS_ERROR_ARRAY
    } else {
        ZarrsResult::ZARRS_SUCCESS
    }
}

/// Store a chunk.
///
/// `pChunkIndices` is a pointer to an array of length `dimensionality` holding the chunk indices.
/// `pChunkBytes` is a pointer to an array of bytes of length `chunkBytesCount` that must match the expected size of the chunk as returned by `zarrsArrayGetChunkSize()`.
///
/// # Errors
/// Returns an error if the array does not have write capability.
///
/// # Safety
/// `array`  must be a valid `ZarrsArray` handle.
/// `dimensionality` must match the dimensionality of the array and the length of the array pointed to by `pChunkIndices`.
#[no_mangle]
pub unsafe extern "C" fn zarrsArrayStoreChunk(
    array: ZarrsArray,
    dimensionality: usize,
    pChunkIndices: *const u64,
    chunkBytesCount: usize,
    pChunkBytes: *const u8,
) -> ZarrsResult {
    // Validation
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;
    let chunk_indices = std::slice::from_raw_parts(pChunkIndices, dimensionality);
    let chunk_bytes = std::slice::from_raw_parts(pChunkBytes, dimensionality);

    let chunk_representation = array_fn!(array, chunk_array_representation, chunk_indices);
    let Ok(chunk_representation) = chunk_representation else {
        unsafe {
            *LAST_ERROR.lock().unwrap() = chunk_representation.unwrap_err_unchecked().to_string()
        };
        return ZarrsResult::ZARRS_ERROR_INVALID_INDICES;
    };
    let Some(chunk_size) = chunk_representation.fixed_size() else {
        *LAST_ERROR.lock().unwrap() = "variable size data types are not supported".to_string();
        return ZarrsResult::ZARRS_ERROR_UNSUPPORTED_DATA_TYPE;
    };
    if chunkBytesCount != chunk_size {
        *LAST_ERROR.lock().unwrap() =
                        format!("zarrsArrayRetrieveChunk chunk_bytes_length {chunkBytesCount} does not match expected length {chunk_size}");
        return ZarrsResult::ZARRS_ERROR_BUFFER_LENGTH;
    }

    // Store the chunk bytes
    match array {
        ZarrsArrayEnum::W(array) => zarrsArrayStoreChunkImpl(array, chunk_indices, chunk_bytes),
        ZarrsArrayEnum::RW(array) => zarrsArrayStoreChunkImpl(array, chunk_indices, chunk_bytes),
        ZarrsArrayEnum::RWL(array) => zarrsArrayStoreChunkImpl(array, chunk_indices, chunk_bytes),
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have write capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}
