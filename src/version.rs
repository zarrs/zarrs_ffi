/// Get the zarrs major version.
#[no_mangle]
pub extern "C" fn zarrsVersionMajor() -> u32 {
    zarrs::version::version_major()
}

/// Get the zarrs minor version.
#[no_mangle]
pub extern "C" fn zarrsVersionMinor() -> u32 {
    zarrs::version::version_minor()
}

/// Get the zarrs patch version.
#[no_mangle]
pub extern "C" fn zarrsVersionPatch() -> u32 {
    zarrs::version::version_patch()
}

/// Get the zarrs version.
///
/// A u32 representation of the version encoded as `(zarrsVersionMajor() << 22) | (zarrsVersionMinor() << 12) | zarrsVersionPatch()`.
#[no_mangle]
pub extern "C" fn zarrsVersion() -> u32 {
    zarrs::version::version()
}

#[cfg(test)]
mod tests {
    use inline_c::assert_cxx;

    #[test]
    fn ffi_version() {
        (assert_cxx! {
            #include "zarrs.h"
            #include <cassert>

            int main() {
                assert(zarrsVersionMajor() == 0);
                assert(zarrsVersionMinor() == 22);
            }
        })
        .success();
    }
}
