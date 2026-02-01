include(FetchContent)

# Example Usage
# find_package(zarrs 0.10 REQUIRED COMPONENTS zarrs/bz2)
# Components are zarrs optional codecs, such as:
#   zarrs/bitround zarrs/zfp zarrs/bz2 zarrs/pcodec

# Corrosion: integrate rust into a cmake project
FetchContent_Declare(Corrosion
    GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
    GIT_TAG v0.5
)
FetchContent_MakeAvailable(Corrosion)

# zarrs_ffi: C/C++ bindings to zarrs
FetchContent_Declare(zarrs_ffi_source
    GIT_REPOSITORY https://github.com/LDeakin/zarrs_ffi.git
    GIT_TAG v${zarrs_FIND_VERSION}
)
FetchContent_MakeAvailable(zarrs_ffi_source)

# Fetch the dependencies at configure time using cargo fetch
get_property(CARGO_EXECUTABLE TARGET Rust::Cargo PROPERTY IMPORTED_LOCATION)
execute_process(COMMAND ${CARGO_EXECUTABLE} fetch --locked WORKING_DIRECTORY ${zarrs_ffi_source_SOURCE_DIR})

# Build zarrs_ffi, creates a zarrs_ffi target aliased to zarrs::zarrs
corrosion_import_crate(MANIFEST_PATH ${zarrs_ffi_source_SOURCE_DIR}/Cargo.toml FEATURES ${zarrs_FIND_COMPONENTS} FLAGS --frozen)
# corrosion_experimental_cbindgen(TARGET zarrs_ffi HEADER_NAME "zarrs.h") # not working

# add zarrs.h to include directories
target_include_directories(zarrs_ffi INTERFACE ${zarrs_ffi_source_SOURCE_DIR})
target_include_directories(zarrs_ffi-static INTERFACE ${zarrs_ffi_source_SOURCE_DIR})

# namespaced library aliases
add_library(zarrs::zarrs ALIAS zarrs_ffi)
add_library(zarrs::zarrs-static ALIAS zarrs_ffi-static)

set(zarrs_INCLUDE_DIR ${zarrs_ffi_source_SOURCE_DIR})
set(zarrs_VERSION_STRING ${zarrs_FIND_VERSION})

mark_as_advanced(zarrs_INCLUDE_DIR ${zarrs_ffi_source_SOURCE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(zarrs
    REQUIRED_VARS
        zarrs_INCLUDE_DIR
    VERSION_VAR
        zarrs_FIND_VERSION
)
