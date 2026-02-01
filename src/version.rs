/// Get the zarrs major version.
#[unsafe(no_mangle)]
pub extern "C" fn zarrsVersionMajor() -> u32 {
    zarrs::version::version_major()
}

/// Get the zarrs minor version.
#[unsafe(no_mangle)]
pub extern "C" fn zarrsVersionMinor() -> u32 {
    zarrs::version::version_minor()
}

/// Get the zarrs patch version.
#[unsafe(no_mangle)]
pub extern "C" fn zarrsVersionPatch() -> u32 {
    zarrs::version::version_patch()
}

/// Get the zarrs version.
///
/// A u32 representation of the version encoded as `(zarrsVersionMajor() << 22) | (zarrsVersionMinor() << 12) | zarrsVersionPatch()`.
#[unsafe(no_mangle)]
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
                assert(zarrsVersionMinor() == 23);
            }
        })
        .success();
    }
}
