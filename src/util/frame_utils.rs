//! Frame-level helpers shared by GPU frame filters (OpenGL, wgpu).
#![cfg_attr(not(any(feature = "opengl", feature = "wgpu")), allow(dead_code))]

use ffmpeg_sys_next::{av_pix_fmt_desc_get, AVPixelFormat};

/// Validates that a raw `AVFrame.format` value refers to a software pixel format.
///
/// Frames produced with `hwaccel_output_format` (e.g. `cuda`, `vaapi`) carry GPU
/// memory handles in `data[..]`; reading them as CPU memory is undefined behavior.
/// This must be checked before any conversion/upload touches the data.
pub(crate) fn ensure_software_format(format: i32) -> Result<AVPixelFormat, String> {
    if format < 0 || format >= AVPixelFormat::AV_PIX_FMT_NB as i32 {
        return Err(format!(
            "Received a frame with invalid pixel format value: {format}"
        ));
    }
    // SAFETY: AVPixelFormat is a contiguous #[repr(i32)] enum covering 0..AV_PIX_FMT_NB;
    // the range check above guarantees `format` is a valid discriminant.
    let pix_fmt: AVPixelFormat = unsafe { std::mem::transmute(format) };

    let desc = unsafe { av_pix_fmt_desc_get(pix_fmt) };
    if desc.is_null() {
        return Err(format!(
            "Received a frame with unknown pixel format: {format}"
        ));
    }
    if unsafe { (*desc).flags } & (ffmpeg_sys_next::AV_PIX_FMT_FLAG_HWACCEL as u64) != 0 {
        return Err(format!(
            "Received a hardware frame (pix_fmt: {pix_fmt:?}). GPU-mapped frames \
             cannot be read as CPU memory. Either remove `set_hwaccel_output_format(...)` \
             so decoded frames are downloaded to system memory, or insert `hwdownload` \
             in the filter graph before this pipeline."
        ));
    }
    Ok(pix_fmt)
}

/// Returns whether a raw `AVFrame.format` value refers to a hardware pixel
/// format (frame data lives in GPU memory handles, not CPU planes).
#[cfg_attr(not(feature = "wgpu"), allow(dead_code))]
pub(crate) fn is_hw_format(format: i32) -> bool {
    if format < 0 || format >= AVPixelFormat::AV_PIX_FMT_NB as i32 {
        return false;
    }
    // SAFETY: range-checked discriminant, same as ensure_software_format.
    let pix_fmt: AVPixelFormat = unsafe { std::mem::transmute(format) };
    let desc = unsafe { av_pix_fmt_desc_get(pix_fmt) };
    !desc.is_null()
        && unsafe { (*desc).flags } & (ffmpeg_sys_next::AV_PIX_FMT_FLAG_HWACCEL as u64) != 0
}

/// Copies a plane between buffers with independent row strides.
/// Both buffers must hold at least `(rows - 1) * stride + width_bytes` bytes.
#[cfg_attr(not(feature = "wgpu"), allow(dead_code))]
pub(crate) fn copy_plane(
    src: &[u8],
    src_stride: usize,
    dst: &mut [u8],
    dst_stride: usize,
    width_bytes: usize,
    rows: usize,
) {
    if rows == 0 {
        return;
    }
    if src_stride == dst_stride {
        // Equal strides: one contiguous copy. Row padding travels along, which
        // is harmless and stays within the documented minimum buffer size.
        let len = (rows - 1) * src_stride + width_bytes;
        dst[..len].copy_from_slice(&src[..len]);
        return;
    }
    for row in 0..rows {
        let s = row * src_stride;
        let d = row * dst_stride;
        dst[d..d + width_bytes].copy_from_slice(&src[s..s + width_bytes]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ensure_software_format_shared() {
        assert!(ensure_software_format(AVPixelFormat::AV_PIX_FMT_YUV420P as i32).is_ok());
        assert!(ensure_software_format(AVPixelFormat::AV_PIX_FMT_NV12 as i32).is_ok());
        let err = ensure_software_format(AVPixelFormat::AV_PIX_FMT_VAAPI as i32).unwrap_err();
        assert!(err.contains("hardware frame"), "{err}");
        assert!(ensure_software_format(-1).is_err());
        assert!(ensure_software_format(i32::MAX).is_err());
    }

    #[test]
    fn test_copy_plane_strides() {
        let src = [1u8, 2, 3, 0, 0, 4, 5, 6, 0, 0]; // 2 rows, stride 5, width 3
        let mut dst = [0xFFu8; 8]; // stride 4
        copy_plane(&src, 5, &mut dst, 4, 3, 2);
        assert_eq!(dst, [1, 2, 3, 0xFF, 4, 5, 6, 0xFF]);
    }

    #[test]
    fn test_copy_plane_equal_strides_fast_path() {
        let src = [1u8, 2, 3, 9, 4, 5, 6, 9]; // 2 rows, stride 4, width 3
        let mut dst = [0xFFu8; 8];
        copy_plane(&src, 4, &mut dst, 4, 3, 2);
        // Row 0 padding is copied along; the byte past the last row's width is not.
        assert_eq!(dst, [1, 2, 3, 9, 4, 5, 6, 0xFF]);

        let mut empty = [0xFFu8; 4];
        copy_plane(&[], 4, &mut empty, 4, 3, 0);
        assert_eq!(empty, [0xFF; 4]);
    }
}
