//! Helper functions that wrap or re-implement FFmpeg's common utility routines. Keeping these
//! thin veneers in one place makes it easy to compare our Rust code with the original C
//! implementations from libavutil/libavformat when debugging.

use ffmpeg_sys_next::{
    av_dict_free, av_dict_get, av_dict_iterate, av_dict_set, av_strerror, AVDictionary, AVRational,
    AV_DICT_MATCH_CASE, AV_ERROR_MAX_STRING_SIZE,
};
use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};

/// Owns an `*mut AVDictionary` and frees it on drop.
///
/// FFmpeg consumer APIs (avformat_open_input, avcodec_open2, av_opt_set_dict2)
/// take `&mut *mut AVDictionary` and leave the UNRECOGNIZED entries behind in
/// a reallocated dictionary — which leaks on every early return unless
/// something owns it. Construct the guard immediately after building the
/// dict, hand `as_double_ptr()` to the consumer, and let drop clean up
/// whatever remains on every path.
pub(crate) struct DictGuard {
    dict: *mut AVDictionary,
}

impl DictGuard {
    pub(crate) fn new(dict: *mut AVDictionary) -> Self {
        Self { dict }
    }

    /// Pointer for FFmpeg consumer APIs; the guard keeps owning the result.
    #[inline]
    pub(crate) fn as_double_ptr(&mut self) -> *mut *mut AVDictionary {
        &mut self.dict
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const AVDictionary {
        self.dict as *const _
    }

    /// Iterate leftover keys (the entries no consumer recognized).
    pub(crate) fn leftover_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        let mut entry = std::ptr::null();
        unsafe {
            loop {
                entry = av_dict_iterate(self.dict, entry);
                if entry.is_null() {
                    break;
                }
                keys.push(CStr::from_ptr((*entry).key).to_string_lossy().into_owned());
            }
        }
        keys
    }

    /// Remove one key (e.g. an internally injected option that must not be
    /// reported as "unrecognized user option").
    pub(crate) fn remove(&mut self, key: &CStr) {
        unsafe {
            if !av_dict_get(
                self.dict,
                key.as_ptr(),
                std::ptr::null(),
                AV_DICT_MATCH_CASE,
            )
            .is_null()
            {
                // Setting a key to NULL deletes it (libavutil/dict.c).
                av_dict_set(&mut self.dict, key.as_ptr(), std::ptr::null(), 0);
            }
        }
    }
}

impl Drop for DictGuard {
    fn drop(&mut self) {
        unsafe {
            av_dict_free(&mut self.dict);
        }
    }
}

/// Convert an optional `HashMap<CString, CString>` into an `AVDictionary` by invoking
/// `av_dict_set()` for each entry.
///
/// FFmpeg reference: `av_dict_set()` in `libavutil/dict.c` uses the same ownership rules; the
/// caller is still responsible for freeing the resulting dictionary via `av_dict_free()`.
pub(crate) fn hashmap_to_avdictionary(
    opts: &Option<HashMap<CString, CString>>,
) -> *mut AVDictionary {
    let mut av_dict: *mut AVDictionary = std::ptr::null_mut();

    if let Some(map) = opts {
        for (key, value) in map {
            unsafe {
                av_dict_set(&mut av_dict, key.as_ptr(), value.as_ptr(), 0);
            }
        }
    }

    av_dict
}

/// Convert Rust String to C-compatible CString, handling null bytes
///
/// This is a utility function for safe String → CString conversion with proper error handling.
/// FFmpeg's C API requires null-terminated strings, but Rust Strings can contain null bytes.
///
/// # Arguments
/// * `s` - Rust string slice to convert
///
/// # Returns
/// * `Ok(CString)` - Successfully converted string
/// * `Err(String)` - String contains embedded null bytes (invalid for C strings)
///
/// # Note
/// Currently unused but kept as a utility function for future features that need
/// safe String → CString conversion with validation. Used internally by
/// hashmap_to_avdictionary_string().
#[allow(dead_code)]
pub(crate) fn string_to_cstring(s: &str) -> Result<CString, String> {
    CString::new(s).map_err(|e| format!("String contains null byte: {}", e))
}

/// Convert HashMap<String, String> to FFmpeg's AVDictionary
///
/// This function provides a type-safe way to convert Rust HashMap to FFmpeg's C dictionary.
/// Unlike hashmap_to_avdictionary() which takes CString values, this accepts String values
/// and performs validation to ensure they don't contain null bytes.
///
/// # Arguments
/// * `opts` - Optional HashMap with String keys and values
///
/// # Returns
/// * `Ok(*mut AVDictionary)` - Pointer to created dictionary (caller must free)
/// * `Err(String)` - Key or value contains embedded null bytes
///
/// # Memory Management
/// Returned AVDictionary must be freed by calling av_dict_free() or similar FFmpeg function.
///
/// # Note
/// Currently unused but kept as a utility function for future user-facing API that accepts
/// String-based options. The metadata implementation uses direct CString conversion instead.
#[allow(dead_code)]
/// Convert a `HashMap<String, String>` into an `AVDictionary`, validating that every key/value is
/// a valid C string before calling into FFmpeg.
///
/// FFmpeg reference: this mirrors the CLI path in `fftools/cmdutils.c` where user-facing strings
/// are validated and then passed to `av_dict_set()`.
pub(crate) fn hashmap_to_avdictionary_string(
    opts: &Option<HashMap<String, String>>,
) -> Result<*mut AVDictionary, String> {
    let mut av_dict: *mut AVDictionary = std::ptr::null_mut();

    if let Some(map) = opts {
        for (key, value) in map {
            let c_key = string_to_cstring(key)?;
            let c_value = string_to_cstring(value)?;
            unsafe {
                av_dict_set(&mut av_dict, c_key.as_ptr(), c_value.as_ptr(), 0);
            }
        }
    }

    Ok(av_dict)
}

/// Safe wrapper around `av_strerror()` that returns a Rust `String` instead of writing into a
/// caller-provided buffer.
///
/// FFmpeg reference: `av_strerror()` in `libavutil/error.c` uses a fixed-size buffer (defined by
/// `AV_ERROR_MAX_STRING_SIZE`), which we allocate on the stack and convert into UTF-8.
pub fn av_err2str(err: i32) -> String {
    unsafe {
        let mut buffer = [0 as c_char; AV_ERROR_MAX_STRING_SIZE];
        av_strerror(err, buffer.as_mut_ptr(), AV_ERROR_MAX_STRING_SIZE);
        let c_str = CStr::from_ptr(buffer.as_ptr());
        match c_str.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => format!("Unknown error: {}", err),
        }
    }
}

/// Whether a frame's data buffers are exclusively owned, i.e. safe to mutate
/// in place.
///
/// Frames handed to a [`FrameFilter`]'s `filter_frame` usually wrap
/// REFCOUNTED buffers shared with the decoder's frame pool or other
/// consumers; writing into a shared buffer corrupts data someone else is
/// still reading. Probe with this, or skip straight to
/// [`make_frame_writable`], before any in-place edit.
///
/// A null or empty frame reports `false`.
///
/// [`FrameFilter`]: crate::filter::frame_filter::FrameFilter
pub fn frame_is_writable(frame: &ffmpeg_next::Frame) -> bool {
    // SAFETY: null-checked; av_frame_is_writable only reads buffer refcounts.
    unsafe {
        !frame.as_ptr().is_null()
            && ffmpeg_sys_next::av_frame_is_writable(frame.as_ptr() as *mut _) > 0
    }
}

/// Ensures exclusive ownership of a frame's data buffers, copying them first
/// if they are shared (FFmpeg's `av_frame_make_writable`).
///
/// Call this before mutating frame data in place inside a
/// [`FrameFilter`](crate::filter::frame_filter::FrameFilter): if the buffers
/// are already exclusive it is a cheap refcount check, otherwise it clones
/// the data so the shared original stays untouched. Props-only frames (no data
/// planes) and the null EOF shell pass through unchanged — there is nothing to
/// copy. A non-refcounted software frame (`buf[0] == null` but real data in
/// `data[0]`) is NOT skipped: `av_frame_make_writable` allocates owned buffers
/// and copies its data, so an in-place edit cannot corrupt the caller's memory.
pub fn make_frame_writable(frame: &mut ffmpeg_next::Frame) -> Result<(), String> {
    // A null shell or props-only frame has nothing to copy; everything else,
    // non-refcounted frames included, is handed to av_frame_make_writable.
    if frame_is_eof_marker(frame) {
        return Ok(());
    }
    // SAFETY: not a props-only frame, so it owns a live AVFrame with data.
    unsafe {
        let ret = ffmpeg_sys_next::av_frame_make_writable(frame.as_mut_ptr());
        if ret < 0 {
            return Err(format!(
                "av_frame_make_writable failed: {}",
                av_err2str(ret)
            ));
        }
    }
    Ok(())
}

/// Returns `true` if `frame` is an end-of-stream flush marker rather than a
/// frame carrying pixel/sample data: either a null frame shell or a props-only
/// frame that owns no buffers AND has no data planes set.
///
/// The test requires BOTH `buf[0] == null` AND every `data[]` pointer null,
/// because neither alone is sufficient:
/// - a hardware frame (e.g. VAAPI) keeps its surface handle in a later `data[]`
///   slot with `data[0] == null`, so a `data[0]`-only check (`Frame::is_empty`)
///   would misclassify it as a marker;
/// - a non-refcounted software frame has `buf[0] == null` but real pixel data in
///   `data[0]`, so a `buf[0]`-only check would misclassify IT as a marker.
///
/// Requiring no buffers and no data leaves only the genuine props-only marker
/// (and the null shell). A
/// [`FrameFilter`](crate::filter::frame_filter::FrameFilter) that passes flush
/// markers straight through should probe its cue with this instead of an
/// open-coded `unsafe` read.
pub fn frame_is_eof_marker(frame: &ffmpeg_next::Frame) -> bool {
    // SAFETY: null-checked; only reads buf[0] and the data plane pointers.
    unsafe {
        let p = frame.as_ptr();
        p.is_null() || ((*p).buf[0].is_null() && (*p).data.iter().all(|d| d.is_null()))
    }
}

/// Rust implementation of FFmpeg's `av_rescale_q_rnd`.
///
/// This function is reimplemented in Rust rather than using FFmpeg's C version because:
/// - In C, `enum AVRounding` values can be freely combined with bitwise OR (e.g., `AV_ROUND_NEAR_INF | AV_ROUND_PASS_MINMAX`)
/// - In Rust, `enum AVRounding` is a strict type that cannot hold combined bit values without `unsafe transmute`
/// - By accepting `u32` as the rounding parameter, we avoid type safety issues while maintaining full compatibility
///
/// Rescales a 64-bit integer by 2 rational numbers with specified rounding.
///
/// The operation is mathematically equivalent to `a * bq / cq`.
///
/// # Arguments
/// * `a` - The value to rescale
/// * `bq` - Source rational (time base)
/// * `cq` - Destination rational (time base)
/// * `rnd` - Rounding mode as `u32`, supports bitwise OR combinations like `AV_ROUND_NEAR_INF as u32 | AV_ROUND_PASS_MINMAX as u32`
///
/// # Returns
/// Rescaled value
///
/// # Reference
/// FFmpeg `libavutil/mathematics.c:134-140`
pub(crate) fn av_rescale_q_rnd(a: i64, bq: AVRational, cq: AVRational, rnd: u32) -> i64 {
    let b = bq.num as i64 * cq.den as i64;
    let c = cq.num as i64 * bq.den as i64;
    av_rescale_rnd(a, b, c, rnd)
}

/// Rust implementation of FFmpeg's `av_rescale_rnd`.
///
/// This is a direct port of FFmpeg's rescaling algorithm to avoid `unsafe transmute` when
/// combining `AVRounding` enum flags. C allows `enum` values to be freely OR-ed together,
/// but Rust's type system requires enum values to be valid variants. By using `u32` for
/// the rounding parameter, we maintain type safety while preserving FFmpeg's behavior.
///
/// Rescales a 64-bit integer with rounding to nearest.
///
/// The operation is mathematically equivalent to `a * b / c`, but writing that
/// directly can overflow.
///
/// # Arguments
/// * `a` - The value to rescale
/// * `b` - Numerator
/// * `c` - Denominator
/// * `rnd` - Rounding mode as `u32`. The lower bits (0-5) specify the base rounding mode,
///          and bit 8192 (`AV_ROUND_PASS_MINMAX`) passes `INT64_MIN/MAX` through unchanged.
///
/// # Returns
/// Rescaled value, or `i64::MIN` on error
///
/// # Reference
/// FFmpeg `libavutil/mathematics.c:58-127`
fn av_rescale_rnd(a: i64, b: i64, c: i64, mut rnd: u32) -> i64 {
    const AV_ROUND_PASS_MINMAX: u32 = ffmpeg_sys_next::AVRounding::AV_ROUND_PASS_MINMAX as u32;
    const INT_MAX: i64 = i32::MAX as i64;

    // av_assert2(c > 0);
    // av_assert2(b >= 0);
    // av_assert2((unsigned)(rnd&~AV_ROUND_PASS_MINMAX)<=5 && (rnd&~AV_ROUND_PASS_MINMAX)!=4);
    if c <= 0
        || b < 0
        || !((rnd & !AV_ROUND_PASS_MINMAX) <= 5 && (rnd & !AV_ROUND_PASS_MINMAX) != 4)
    {
        return i64::MIN;
    }

    // if (rnd & AV_ROUND_PASS_MINMAX) {
    //     if (a == INT64_MIN || a == INT64_MAX)
    //         return a;
    //     rnd -= AV_ROUND_PASS_MINMAX;
    // }
    if (rnd & AV_ROUND_PASS_MINMAX) != 0 {
        if a == i64::MIN || a == i64::MAX {
            return a;
        }
        rnd -= AV_ROUND_PASS_MINMAX;
    }

    // if (a < 0)
    //     return -(uint64_t)av_rescale_rnd(-FFMAX(a, -INT64_MAX), b, c, rnd ^ ((rnd >> 1) & 1));
    if a < 0 {
        // -FFMAX(a, -INT64_MAX) equivalent to -a.max(-INT64_MAX)
        let neg_a = -a.max(-i64::MAX);
        let neg_result = av_rescale_rnd(neg_a, b, c, rnd ^ ((rnd >> 1) & 1));
        return -((neg_result as u64) as i64);
    }

    // if (rnd == AV_ROUND_NEAR_INF)
    //     r = c / 2;
    // else if (rnd & 1)
    //     r = c - 1;
    let r = if rnd == ffmpeg_sys_next::AVRounding::AV_ROUND_NEAR_INF as u32 {
        c / 2
    } else if (rnd & 1) != 0 {
        c - 1
    } else {
        0
    };

    // Fast path: if (b <= INT_MAX && c <= INT_MAX)
    if b <= INT_MAX && c <= INT_MAX {
        // if (a <= INT_MAX)
        //     return (a * b + r) / c;
        if a <= INT_MAX {
            return (a * b + r) / c;
        } else {
            // int64_t ad = a / c;
            // int64_t a2 = (a % c * b + r) / c;
            // if (ad >= INT32_MAX && b && ad > (INT64_MAX - a2) / b)
            //     return INT64_MIN;
            // return ad * b + a2;
            let ad = a / c;
            let a2 = (a % c * b + r) / c;
            if ad >= INT_MAX && b != 0 && ad > (i64::MAX - a2) / b {
                return i64::MIN;
            }
            return ad * b + a2;
        }
    }

    // Large value path: 128-bit precision
    rescale_large(a, b, c, r)
}

/// 128-bit precision rescaling for large values.
///
/// Rust uses native u128 instead of manual 128-bit simulation because:
/// - Rust has built-in u128 support (unlike C99/C11)
/// - LLVM optimizes u128 operations efficiently on modern hardware
/// - Simpler, more maintainable code with equivalent performance
///
/// # Reference
/// FFmpeg `libavutil/mathematics.c:93-117` (manual 128-bit simulation)
fn rescale_large(a: i64, b: i64, c: i64, r: i64) -> i64 {
    let a = a as u128;
    let b = b as u128;
    let c = c as u128;
    let r = r as u128;

    let result = (a * b + r) / c;

    if result > i64::MAX as u128 {
        i64::MIN
    } else {
        result as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_av_rescale_q_rnd_basic() {
        // Test basic rescaling: 1000 from 1/1000 to 1/90000
        // Expected: 1000 * (1 * 90000) / (1000 * 1) = 90000
        let bq = AVRational { num: 1, den: 1000 };
        let cq = AVRational { num: 1, den: 90000 };

        let result = av_rescale_q_rnd(1000, bq, cq, 5); // AV_ROUND_NEAR_INF = 5
        assert_eq!(result, 90000);
    }

    #[test]
    fn test_av_rescale_q_rnd_with_pass_minmax_flag() {
        // Test AV_ROUND_NEAR_INF | AV_ROUND_PASS_MINMAX = 5 | 8192 = 8197
        let bq = AVRational { num: 1, den: 1000 };
        let cq = AVRational { num: 1, den: 90000 };

        // INT64_MIN should pass through unchanged
        let result = av_rescale_q_rnd(i64::MIN, bq, cq, 8197);
        assert_eq!(result, i64::MIN);

        // INT64_MAX should pass through unchanged
        let result = av_rescale_q_rnd(i64::MAX, bq, cq, 8197);
        assert_eq!(result, i64::MAX);
    }

    #[test]
    fn test_av_rescale_q_rnd_normal_value_with_pass_minmax() {
        // Test normal value with AV_ROUND_PASS_MINMAX flag
        // Flag should be ignored for normal values
        let bq = AVRational { num: 1, den: 1000 };
        let cq = AVRational { num: 1, den: 90000 };

        let result = av_rescale_q_rnd(1000, bq, cq, 8197); // 5 | 8192
        assert_eq!(result, 90000);
    }

    #[test]
    fn test_av_rescale_rnd_negative_value() {
        // Test negative value rescaling
        let result = av_rescale_rnd(-1000, 90000, 1000, 5);
        assert_eq!(result, -90000);
    }

    #[test]
    fn test_av_rescale_rnd_zero() {
        // Test zero rescaling
        let result = av_rescale_rnd(0, 90000, 1000, 5);
        assert_eq!(result, 0);
    }

    #[test]
    fn test_av_rescale_rnd_rounding_modes() {
        // Test different rounding modes
        // (7 * 3 + r) / 5 where r depends on rounding mode

        // AV_ROUND_ZERO = 0: r = 0, result = 21 / 5 = 4
        assert_eq!(av_rescale_rnd(7, 3, 5, 0), 4);

        // AV_ROUND_INF = 1: r = c - 1 = 4, result = 25 / 5 = 5
        assert_eq!(av_rescale_rnd(7, 3, 5, 1), 5);

        // AV_ROUND_NEAR_INF = 5: r = c / 2 = 2, result = 23 / 5 = 4
        assert_eq!(av_rescale_rnd(7, 3, 5, 5), 4);
    }

    #[test]
    fn test_av_rescale_rnd_large_values() {
        // Test with large values that trigger the 128-bit path
        let large_a = i32::MAX as i64 + 1000;
        let result = av_rescale_rnd(large_a, 1000, 1, 5);
        assert_eq!(result, large_a * 1000);
    }

    #[test]
    fn test_av_rescale_rnd_invalid_params() {
        // Test invalid c <= 0
        assert_eq!(av_rescale_rnd(100, 100, 0, 5), i64::MIN);

        // Test invalid b < 0
        assert_eq!(av_rescale_rnd(100, -1, 100, 5), i64::MIN);

        // Test invalid rounding mode (4 is invalid)
        assert_eq!(av_rescale_rnd(100, 100, 100, 4), i64::MIN);
    }

    /// frame_is_eof_marker must treat ONLY a null shell or a props-only frame
    /// (no buffers AND no data) as a marker — never a VAAPI-shaped hardware
    /// frame (buf set, data[0] null, surface in data[3]) nor a non-refcounted
    /// software frame (buf null, data[0] set). The last two are the shapes the
    /// old data[0]-only and the interim buf[0]-only tests each got wrong.
    #[test]
    fn frame_is_eof_marker_classifies_every_buffer_shape() {
        use ffmpeg_next::Frame;
        use ffmpeg_sys_next::{av_buffer_alloc, av_frame_alloc, av_frame_get_buffer, AVPixelFormat};
        unsafe {
            // null shell.
            assert!(
                frame_is_eof_marker(&Frame::wrap(std::ptr::null_mut())),
                "null frame is a marker"
            );

            // props-only marker: fresh AVFrame, no buffers, no data.
            let marker = av_frame_alloc();
            assert!(
                frame_is_eof_marker(&Frame::wrap(marker)),
                "props-only frame is a marker"
            );

            // normal refcounted software frame: buf[0] and data[0] set.
            let sw = av_frame_alloc();
            (*sw).format = AVPixelFormat::AV_PIX_FMT_RGBA as i32;
            (*sw).width = 4;
            (*sw).height = 4;
            assert!(av_frame_get_buffer(sw, 0) >= 0);
            assert!(
                !frame_is_eof_marker(&Frame::wrap(sw)),
                "a normal software frame is not a marker"
            );

            // VAAPI-shaped frame: buf[0] set, data[0] null, surface in data[3].
            let hw = av_frame_alloc();
            let hw_buf = av_buffer_alloc(16);
            (*hw).buf[0] = hw_buf;
            (*hw).data[3] = (*hw_buf).data;
            assert!(
                !frame_is_eof_marker(&Frame::wrap(hw)),
                "a hardware frame (data in data[3]) is not a marker"
            );

            // non-refcounted software frame: buf[0] null, data[0] points at
            // caller memory. `pixels` is declared first so it outlives the
            // Frame's drop; av_frame_free leaves non-owned data[] untouched.
            let mut pixels = [0u8; 16];
            let nrc = av_frame_alloc();
            (*nrc).data[0] = pixels.as_mut_ptr();
            let nrc_frame = Frame::wrap(nrc);
            let nrc_is_marker = frame_is_eof_marker(&nrc_frame);
            drop(nrc_frame);
            let _ = &pixels;
            assert!(
                !nrc_is_marker,
                "a non-refcounted software frame (data but no buf) is not a marker"
            );
        }
    }

    /// make_frame_writable must NOT skip a non-refcounted software frame: it
    /// defers to av_frame_make_writable, which allocates owned buffers and
    /// copies, so buf[0] is non-null afterwards and an in-place edit cannot
    /// corrupt the caller's memory. The old buf[0]-null skip wrongly returned
    /// early and left it un-copied.
    #[test]
    fn make_frame_writable_copies_a_non_refcounted_frame() {
        use ffmpeg_next::Frame;
        use ffmpeg_sys_next::{av_frame_alloc, AVPixelFormat};
        unsafe {
            let raw = av_frame_alloc();
            (*raw).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
            (*raw).width = 4;
            (*raw).height = 4;
            (*raw).linesize[0] = 4;
            // Distinctive non-zero payload so the copy can be verified byte for byte.
            let mut pixels: Vec<u8> = (1..=16).collect();
            let src_ptr = pixels.as_mut_ptr();
            (*raw).data[0] = src_ptr;
            assert!((*raw).buf[0].is_null(), "precondition: non-refcounted");
            let mut f = Frame::wrap(raw);
            make_frame_writable(&mut f).expect("make_frame_writable must copy, not skip");

            let out = f.as_ptr();
            assert!(
                !(*out).buf[0].is_null(),
                "a non-refcounted frame must be given owned buffers"
            );
            // The data was copied to a fresh buffer, not aliased to the caller's.
            assert_ne!((*out).data[0], src_ptr, "data must point at a new buffer");
            // ...and the bytes match, honoring the (possibly padded) new linesize.
            let new_ls = (*out).linesize[0] as usize;
            for row in 0..4usize {
                let copied = std::slice::from_raw_parts((*out).data[0].add(row * new_ls), 4);
                assert_eq!(
                    copied,
                    &pixels[row * 4..row * 4 + 4],
                    "row {row} must be copied verbatim"
                );
            }
            // Mutating the copy must leave the caller's memory untouched.
            *(*out).data[0] = 0xAB;
            assert_eq!(pixels[0], 1, "the source must be untouched by an in-place edit");
        }
    }
}
