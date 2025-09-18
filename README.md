# zarrs_ffi

[![Latest Version](https://img.shields.io/crates/v/zarrs_ffi.svg)](https://crates.io/crates/zarrs_ffi)
[![zarrs_ffi documentation](https://img.shields.io/badge/docs-Doxygen-green)](https://zarrs.dev/zarrs_ffi/)
[![zarrs documentation](https://img.shields.io/badge/docs-docs.rs-green)](https://docs.rs/zarrs_ffi)
![msrv](https://img.shields.io/crates/msrv/zarrs_ffi)
[![build](https://github.com/LDeakin/zarrs_ffi/actions/workflows/ci.yml/badge.svg)](https://github.com/LDeakin/zarrs_ffi/actions/workflows/ci.yml)

C/C++ bindings for the [`zarrs`] crate, a Rust library for the [Zarr](https://zarr.dev) storage format for multidimensional arrays and metadata.

`zarrs_ffi` is a single header library: `zarrs.h` [(docs)](https://zarrs.dev/zarrs_ffi/zarrs_8h.html).

Currently `zarrs_ffi` only supports a small subset of the [`zarrs`] API.

A changelog can be found [here](https://github.com/LDeakin/zarrs_ffi/blob/main/CHANGELOG.md).

## Example
```C++
#include "zarrs.h"

void main() {
  // Open a filesystem store pointing to a zarr hierarchy
  ZarrsStorage storage = nullptr;
  zarrs_assert(zarrsCreateStorageFilesystem("/path/to/hierarchy.zarr", &storage));

  // Open an array in the hierarchy
  ZarrsArray array = nullptr;
  zarrs_assert(zarrsOpenArrayRW(storage, "/array", &array));

  // Get the array dimensionality
  size_t dimensionality;
  zarrs_assert(zarrsArrayGetDimensionality(array, &dimensionality));
  assert(dimensionality == 2);

  // Retrieve the decoded bytes of the chunk at [0, 0]
  size_t indices[] = {0, 0};
  size_t chunk_size;
  zarrs_assert(zarrsArrayGetChunkSize(array, 2, indices, &chunk_size));
  std::unique_ptr<uint8_t[]> chunk_bytes(new uint8_t[chunk_size]);
  zarrs_assert(zarrsArrayRetrieveChunk(array, 2, indices, chunk_size, chunk_bytes.get()));
}
```

See a more comprehensive example in the [examples](https://github.com/LDeakin/zarrs_ffi/tree/main/examples) directory.

## CMake Quickstart
1. Install the Rust compiler (and cargo).
2. Put [Findzarrs.cmake](https://github.com/LDeakin/zarrs_ffi/blob/main/examples/Findzarrs.cmake) in your `CMAKE_MODULE_PATH`
3. `find_package(zarrs <version> REQUIRED COMPONENTS zarrs/bz2)`
   - Replace `<version>` with the latest release: [![Latest Version](https://img.shields.io/crates/v/zarrs_ffi.svg)](https://crates.io/crates/zarrs_ffi) (e.g., `0.9` or `0.9.4`)
   - [`zarrs`] is retrieved from `GitHub` using [FetchContent](https://cmake.org/cmake/help/latest/module/FetchContent.html) and built using [corrosion](https://github.com/corrosion-rs/corrosion)
   - Components are optional [`zarrs`] codecs
4. the `zarrs_ffi` library is available as the `zarrs::zarrs` or  `zarrs::zarrs-static` target

A complete `CMake` example can be found in [examples/](https://github.com/LDeakin/zarrs_ffi/tree/main/examples/).

## Manual Build

#### Basic Build
Building generates a header, and a platform-dependent static and dynamic library.
```bash
cargo build --release --features cbindgen # -> zarrs.h and target/release/[lib]zarrs_ffi{.a,.so,.dll,.dylib}
```
`zarrs.h` is only re-generated if the `cbindgen` feature is enabled.

#### Enabling SIMD intrinsics
Encoding and decoding performance may be improved with `avx2`/`sse2` enabled (if supported).
Compile with either of:
 - `RUSTFLAGS="-C target-cpu=native"`
 - `RUSTFLAGS="-C target-feature=+avx2,+sse2"`

#### Enabling non-default zarrs codecs
Non-default `zarrs` codecs (see [`zarrs` crate features](https://docs.rs/zarrs/latest/zarrs/#crate-features)) can be enabled with the `all_codecs` feature.
Alternatively, individual codecs can be enabled by passing them as feature flags.
For example:
```bash
cargo build --release --features cbindgen --features zarrs/zstd,zarrs/bitround,zarrs/zfp,zarrs/bz2,zarrs/pcodec,zarrs/gdeflate
```

## Licence
`zarrs_ffi` is licensed under either of
 - the Apache License, Version 2.0 [LICENSE-APACHE](./LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
 - the MIT license [LICENSE-MIT](./LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.

[`zarrs`]: https://github.com/LDeakin/zarrs
