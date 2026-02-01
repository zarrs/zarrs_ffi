pub mod group_write;

use std::ffi::{CString, c_char};

use ffi_support::FfiStr;
use zarrs::group::{Group, GroupMetadata};

use crate::{
    LAST_ERROR, ZarrsResult,
    storage::{ZarrsStorage, ZarrsStorageEnum},
};

#[doc(hidden)]
#[allow(clippy::upper_case_acronyms)]
pub enum ZarrsGroupEnum {
    #[allow(dead_code)]
    R(Group<dyn zarrs::storage::ReadableStorageTraits>),
    #[allow(dead_code)]
    W(Group<dyn zarrs::storage::WritableStorageTraits>),
    #[allow(dead_code)]
    L(Group<dyn zarrs::storage::ListableStorageTraits>),
    #[allow(dead_code)]
    RL(Group<dyn zarrs::storage::ReadableListableStorageTraits>),
    RW(Group<dyn zarrs::storage::ReadableWritableStorageTraits>),
    #[allow(dead_code)]
    RWL(Group<dyn zarrs::storage::ReadableWritableListableStorageTraits>),
}

macro_rules! _group_fn {
    ($group:expr, $fn:ident ) => {
        match $group {
            crate::group::ZarrsGroupEnum::R(group) => group.$fn(),
            crate::group::ZarrsGroupEnum::W(group) => group.$fn(),
            crate::group::ZarrsGroupEnum::L(group) => group.$fn(),
            crate::group::ZarrsGroupEnum::RL(group) => group.$fn(),
            crate::group::ZarrsGroupEnum::RW(group) => group.$fn(),
            crate::group::ZarrsGroupEnum::RWL(group) => group.$fn(),
        }
    };
    ($group:expr, $fn:ident, $( $args:expr ),* ) => {
        match $group {
            crate::group::ZarrsGroupEnum::R(group) => group.$fn($( $args ),*),
            crate::group::ZarrsGroupEnum::W(group) => group.$fn($( $args ),*),
            crate::group::ZarrsGroupEnum::L(group) => group.$fn($( $args ),*),
            crate::group::ZarrsGroupEnum::RL(group) => group.$fn($( $args ),*),
            crate::group::ZarrsGroupEnum::RW(group) => group.$fn($( $args ),*),
            crate::group::ZarrsGroupEnum::RWL(group) => group.$fn($( $args ),*),
        }
    };
}

pub(crate) use _group_fn as group_fn;

#[doc(hidden)]
pub struct ZarrsGroup_T(pub ZarrsGroupEnum);

impl std::ops::Deref for ZarrsGroup_T {
    type Target = ZarrsGroupEnum;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ZarrsGroup_T {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// An opaque handle to a zarr group.
pub type ZarrsGroup = *mut ZarrsGroup_T;

/// Create a handle to an existing group (read/write capability).
///
/// `pGroup` is a pointer to a handle in which the created `ZarrsGroup` is returned.
///
/// # Safety
/// `pGroup` must be a valid pointer to a `ZarrsGroup` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsOpenGroupRW(
    storage: ZarrsStorage,
    path: FfiStr,
    pGroup: *mut ZarrsGroup,
) -> ZarrsResult {
    if storage.is_null() {
        *LAST_ERROR.lock().unwrap() = "storage is null".to_string();
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }

    // SAFETY: storage is not null, and the caller guarantees it is a valid ZarrsStorage handle.
    let storage = unsafe { &**storage };

    if let ZarrsStorageEnum::RW(storage) = storage {
        match Group::open(storage.clone(), path.into()) {
            Ok(group) => {
                // SAFETY: pGroup is a valid pointer per the function's safety contract.
                unsafe {
                    *pGroup = Box::into_raw(Box::new(ZarrsGroup_T(ZarrsGroupEnum::RW(group))));
                }
                ZarrsResult::ZARRS_SUCCESS
            }
            Err(err) => {
                *LAST_ERROR.lock().unwrap() = err.to_string();
                ZarrsResult::ZARRS_ERROR_GROUP
            }
        }
    } else {
        *LAST_ERROR.lock().unwrap() = "storage does not support read and write".to_string();
        ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
    }
}

/// Create a handle to a new group (read/write capability).
///
/// `metadata` is expected to be a JSON string representing a zarr V3 group `zarr.json`.
/// `pGroup` is a pointer to a handle in which the created `ZarrsGroup` is returned.
///
/// # Safety
/// `pGroup` must be a valid pointer to a `ZarrsGroup` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsCreateGroupRW(
    storage: ZarrsStorage,
    path: FfiStr,
    metadata: FfiStr,
    pGroup: *mut ZarrsGroup,
) -> ZarrsResult {
    if storage.is_null() {
        *LAST_ERROR.lock().unwrap() = "storage is null".to_string();
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }

    // SAFETY: storage is not null, and the caller guarantees it is a valid ZarrsStorage handle.
    let storage = unsafe { &**storage };

    let metadata = match GroupMetadata::try_from(metadata.as_str()) {
        Ok(metadata) => metadata,
        Err(err) => {
            *LAST_ERROR.lock().unwrap() = err.to_string();
            return ZarrsResult::ZARRS_ERROR_INVALID_METADATA;
        }
    };

    if let ZarrsStorageEnum::RW(storage) = storage {
        match Group::new_with_metadata(storage.clone(), path.into(), metadata) {
            Ok(group) => {
                // SAFETY: pGroup is a valid pointer per the function's safety contract.
                unsafe {
                    *pGroup = Box::into_raw(Box::new(ZarrsGroup_T(ZarrsGroupEnum::RW(group))));
                }
                ZarrsResult::ZARRS_SUCCESS
            }
            Err(err) => {
                *LAST_ERROR.lock().unwrap() = err.to_string();
                ZarrsResult::ZARRS_ERROR_GROUP
            }
        }
    } else {
        *LAST_ERROR.lock().unwrap() = "storage does not support read and write".to_string();
        ZarrsResult::ZARRS_ERROR_STORAGE_CAPABILITY
    }
}

/// Destroy group.
///
/// # Errors
/// Returns `ZarrsResult::ZARRS_ERROR_NULL_PTR` if `group` is a null pointer.
///
/// # Safety
/// If not null, `group` must be a valid `ZarrsGroup` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsDestroyGroup(group: ZarrsGroup) -> ZarrsResult {
    if group.is_null() {
        ZarrsResult::ZARRS_ERROR_NULL_PTR
    } else {
        // SAFETY: group is not null, and the caller guarantees it is a valid ZarrsGroup handle.
        unsafe { group.to_owned().drop_in_place() };
        ZarrsResult::ZARRS_SUCCESS
    }
}

/// Get the group attributes as a JSON string.
///
/// The string must be freed with `zarrsFreeString`.
///
/// # Safety
/// `group` must be a valid `ZarrsGroup` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsGroupGetAttributes(
    group: ZarrsGroup,
    pretty: bool,
    pAttributesString: *mut *mut c_char,
) -> ZarrsResult {
    // Validation
    if group.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    // SAFETY: group is not null, and the caller guarantees it is a valid ZarrsGroup handle.
    let group = unsafe { &**group };

    let attributes = group_fn!(group, attributes);
    let attributes_str = if pretty {
        serde_json::to_string_pretty(&attributes)
    } else {
        serde_json::to_string(&attributes)
    };
    if let Ok(attributes_str) = attributes_str
        && let Ok(cstring) = CString::new(attributes_str)
    {
        // SAFETY: pAttributesString is a valid pointer per the function's safety contract.
        unsafe { *pAttributesString = cstring.into_raw() };
        return ZarrsResult::ZARRS_SUCCESS;
    }

    *LAST_ERROR.lock().unwrap() = "error converting attributes to a json string".to_string();
    ZarrsResult::ZARRS_ERROR_INVALID_METADATA
}

/// Set the group attributes from a JSON string.
///
/// # Errors
/// Returns `ZarrsResult::ZARRS_ERROR_INVALID_METADATA` if attributes is not a valid JSON object (map).
///
/// # Safety
/// `group` must be a valid `ZarrsGroup` handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn zarrsGroupSetAttributes(
    group: ZarrsGroup,
    attributes: FfiStr,
) -> ZarrsResult {
    // Validation
    if group.is_null() {
        return ZarrsResult::ZARRS_ERROR_NULL_PTR;
    }
    // SAFETY: group is not null, and the caller guarantees it is a valid ZarrsGroup handle.
    let group = unsafe { &mut **group };

    // Deserialise the attributes
    let Ok(serde_json::Value::Object(mut attributes)) =
        serde_json::from_str::<serde_json::Value>(attributes.into())
    else {
        *LAST_ERROR.lock().unwrap() = "error interpreting attributes to a json map".to_string();
        return ZarrsResult::ZARRS_ERROR_INVALID_METADATA;
    };

    // Set the group attributes
    let group_attributes = group_fn!(group, attributes_mut);
    group_attributes.clear();
    group_attributes.append(&mut attributes);

    ZarrsResult::ZARRS_SUCCESS
}
