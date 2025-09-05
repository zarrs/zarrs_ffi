use zarrs::{
    array::{
        chunk_shape_to_array_shape, codec::CodecOptions, Array, ArrayShardedExt,
        ArrayShardedReadableExt, ArrayShardedReadableExtCache,
    },
    array_subset::ArraySubset,
    storage::ReadableStorageTraits,
};

use crate::{ZarrsResult, LAST_ERROR};

use super::{array_fn, ZarrsArray, ZarrsArrayEnum};

#[doc(hidden)]
pub struct ZarrsShardIndexCache_T(pub ArrayShardedReadableExtCache);

impl std::ops::Deref for ZarrsShardIndexCache_T {
    type Target = ArrayShardedReadableExtCache;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// An opaque handle to a zarrs [`ArrayShardedReadableExtCache`].
pub type ZarrsShardIndexCache = *mut ZarrsShardIndexCache_T;

/// Get the shape of the inner chunk grid of a sharded array.
///
/// If the array is not sharded, the contents of `pInnerChunkGridShape` will equal the standard chunk grid shape.
///
/// # Errors
/// - Returns `ZarrsResult::ZARRS_ERROR_NULL_PTR` if `array` is a null pointer.
/// 
/// # Safety
/// `array` must be a valid `ZarrsArray` handle.
/// `dimensionality` must match the dimensionality of the array and the length of the array pointed to by `pInnerChunkGridShape`.
#[no_mangle]
pub unsafe extern "C" fn zarrsArrayGetInnerChunkGridShape(
    array: ZarrsArray,
    dimensionality: usize,
    pInnerChunkGridShape: *mut u64,
) -> ZarrsResult {
    // Validation
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;

    // Get the inner chunk grid shape
    let inner_chunk_grid_shape = array_fn!(array, inner_chunk_grid_shape);
    let pInnerChunkShape =
        unsafe { std::slice::from_raw_parts_mut(pInnerChunkGridShape, dimensionality) };
    pInnerChunkShape.copy_from_slice(&inner_chunk_grid_shape);
    ZarrsResult::ZARRS_SUCCESS
}

/// Get the inner chunk shape for a sharded array.
///
/// `pIsSharded` is set to true if the array is sharded, otherwise false.
/// If the array is not sharded, the contents of `pInnerChunkShape` will be undefined.
///
/// # Safety
/// `array` must be a valid `ZarrsArray` handle.
/// `dimensionality` must match the dimensionality of the array and the length of the array pointed to by `pChunkShape`.
#[no_mangle]
pub unsafe extern "C" fn zarrsArrayGetInnerChunkShape(
    array: ZarrsArray,
    dimensionality: usize,
    pIsSharded: *mut bool,
    pInnerChunkShape: *mut u64,
) -> ZarrsResult {
    // Validation
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;

    // Get the inner chunk shape
    let inner_chunk_shape = array_fn!(array, inner_chunk_shape);
    match inner_chunk_shape {
        Some(inner_chunk_shape) => {
            let pInnerChunkShape =
                unsafe { std::slice::from_raw_parts_mut(pInnerChunkShape, dimensionality) };
            pInnerChunkShape.copy_from_slice(&chunk_shape_to_array_shape(&inner_chunk_shape));
            *pIsSharded = true;
        }
        None => {
            *pIsSharded = false;
        }
    }
    ZarrsResult::ZARRS_SUCCESS
}

/// Create a handle to a new shard index cache.
///
/// # Errors
/// Returns an error if the array does not have read capability.
///
/// # Safety
/// `array` must be a valid `ZarrsArray` handle.
#[no_mangle]
pub unsafe extern "C" fn zarrsCreateShardIndexCache(
    array: ZarrsArray,
    pShardIndexCache: *mut ZarrsShardIndexCache,
) -> ZarrsResult {
    // Validation
    if array.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;

    match array {
        ZarrsArrayEnum::R(array) => {
            *pShardIndexCache = Box::into_raw(Box::new(ZarrsShardIndexCache_T(
                ArrayShardedReadableExtCache::new(array),
            )));
        }
        ZarrsArrayEnum::RW(array) => {
            *pShardIndexCache = Box::into_raw(Box::new(ZarrsShardIndexCache_T(
                ArrayShardedReadableExtCache::new(array),
            )));
        }
        ZarrsArrayEnum::RWL(array) => {
            *pShardIndexCache = Box::into_raw(Box::new(ZarrsShardIndexCache_T(
                ArrayShardedReadableExtCache::new(array),
            )));
        }
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have read capability".to_string();
            return ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY;
        }
    }

    ZarrsResult::ZARRS_SUCCESS
}

/// Destroy a shard index cache.
///
/// # Errors
/// Returns `ZarrsResult::ZARRS_ERROR_NULL_PTR` if `shardIndexCache` is a null pointer.
///
/// # Safety
/// If not null, `shardIndexCache` must be a valid `ZarrsShardIndexCache` handle.
#[no_mangle]
pub unsafe extern "C" fn zarrsDestroyShardIndexCache(
    shardIndexCache: ZarrsShardIndexCache,
) -> ZarrsResult {
    if shardIndexCache.is_null() {
        ZarrsResult::ZARRS_ERROR_NULL_PTR
    } else {
        unsafe { shardIndexCache.to_owned().drop_in_place() };
        ZarrsResult::ZARRS_SUCCESS
    }
}

fn zarrsArrayRetrieveInnerChunkImpl<T: ReadableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
    cache: &ArrayShardedReadableExtCache,
    chunk_indices: &[u64],
    chunk_bytes_length: usize,
    chunk_bytes: *mut u8,
) -> ZarrsResult {
    match array.retrieve_inner_chunk_opt(cache, chunk_indices, &CodecOptions::default()) {
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

/// Retrieve an inner chunk from a sharded array (or outer chunk for an unsharded array).
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
#[no_mangle]
pub unsafe extern "C" fn zarrsArrayRetrieveInnerChunk(
    array: ZarrsArray,
    cache: ZarrsShardIndexCache,
    dimensionality: usize,
    pChunkIndices: *const u64,
    chunkBytesCount: usize,
    pChunkBytes: *mut u8,
) -> ZarrsResult {
    if array.is_null() || cache.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;
    let cache = &**cache;
    let chunk_indices = std::slice::from_raw_parts(pChunkIndices, dimensionality);

    // Get the chunk bytes
    match array {
        ZarrsArrayEnum::R(array) => zarrsArrayRetrieveInnerChunkImpl(
            array,
            cache,
            chunk_indices,
            chunkBytesCount,
            pChunkBytes,
        ),
        ZarrsArrayEnum::RL(array) => zarrsArrayRetrieveInnerChunkImpl(
            array,
            cache,
            chunk_indices,
            chunkBytesCount,
            pChunkBytes,
        ),
        ZarrsArrayEnum::RW(array) => zarrsArrayRetrieveInnerChunkImpl(
            array,
            cache,
            chunk_indices,
            chunkBytesCount,
            pChunkBytes,
        ),
        ZarrsArrayEnum::RWL(array) => zarrsArrayRetrieveInnerChunkImpl(
            array,
            cache,
            chunk_indices,
            chunkBytesCount,
            pChunkBytes,
        ),
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have read capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}

fn zarrsArrayRetrieveSubsetShardedImpl<T: ReadableStorageTraits + ?Sized + 'static>(
    array: &Array<T>,
    cache: &ArrayShardedReadableExtCache,
    array_subset: &ArraySubset,
    subset_bytes_length: usize,
    subset_bytes: *mut u8,
) -> ZarrsResult {
    match array.retrieve_array_subset_sharded_opt(cache, array_subset, &CodecOptions::default()) {
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

// TODO: Retrieve inner chunks

/// Retrieve a subset from an array (with a shard index cache).
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
#[no_mangle]
pub unsafe extern "C" fn zarrsArrayRetrieveSubsetSharded(
    array: ZarrsArray,
    cache: ZarrsShardIndexCache,
    dimensionality: usize,
    pSubsetStart: *const u64,
    pSubsetShape: *const u64,
    subsetBytesCount: usize,
    pSubsetBytes: *mut u8,
) -> ZarrsResult {
    // Validation
    if array.is_null() || cache.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    let array = &**array;
    let cache = &**cache;
    let subset_start = std::slice::from_raw_parts(pSubsetStart, dimensionality);
    let subset_shape = std::slice::from_raw_parts(pSubsetShape, dimensionality);
    let array_subset = ArraySubset::from(
        std::iter::zip(subset_start, subset_shape).map(|(&start, &shape)| start..start + shape),
    );

    // Get the subset bytes
    match array {
        ZarrsArrayEnum::R(array) => zarrsArrayRetrieveSubsetShardedImpl(
            array,
            cache,
            &array_subset,
            subsetBytesCount,
            pSubsetBytes,
        ),
        ZarrsArrayEnum::RL(array) => zarrsArrayRetrieveSubsetShardedImpl(
            array,
            cache,
            &array_subset,
            subsetBytesCount,
            pSubsetBytes,
        ),
        ZarrsArrayEnum::RW(array) => zarrsArrayRetrieveSubsetShardedImpl(
            array,
            cache,
            &array_subset,
            subsetBytesCount,
            pSubsetBytes,
        ),
        ZarrsArrayEnum::RWL(array) => zarrsArrayRetrieveSubsetShardedImpl(
            array,
            cache,
            &array_subset,
            subsetBytesCount,
            pSubsetBytes,
        ),
        _ => {
            *LAST_ERROR.lock().unwrap() = "storage does not have read capability".to_string();
            ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
        }
    }
}
