//! Display matrix helpers ported from libavutil/display.c and the fftools
//! `get_rotation` normalization (cmdutils.c:1475-1490).

use ffmpeg_sys_next::AVPacketSideData;
use ffmpeg_sys_next::AVPacketSideDataType::AV_PKT_DATA_DISPLAYMATRIX;
use log::warn;
use std::f64::consts::PI;

/// Rotation in whole degrees from a stream's packed side data, or None when
/// no usable AV_PKT_DATA_DISPLAYMATRIX entry is present. Modern demuxers
/// (mov.c, matroskadec.c) export rotation only through this side data; they
/// never write a "rotate" metadata tag.
pub(crate) fn rotation_from_side_data(side_data: &[AVPacketSideData]) -> Option<i32> {
    let sd = side_data.iter().find(|sd| {
        sd.type_ == AV_PKT_DATA_DISPLAYMATRIX
            && !sd.data.is_null()
            && sd.size >= size_of::<[i32; 9]>()
    })?;

    let mut matrix = [0i32; 9];
    // SAFETY: the size check above guarantees at least 36 readable bytes;
    // the matrix is a plain native-endian i32 array (libavutil/display.h).
    unsafe {
        std::ptr::copy_nonoverlapping(
            sd.data as *const u8,
            matrix.as_mut_ptr() as *mut u8,
            size_of::<[i32; 9]>(),
        );
    }

    let theta = get_rotation(&matrix);
    if !theta.is_finite() {
        // Degenerate matrix (zero scale): same as no display matrix.
        return None;
    }
    Some(theta.round() as i32)
}

/// fftools get_rotation (cmdutils.c:1475-1490): negate the counterclockwise
/// matrix angle and normalize into [0, 360) with a 0.9 degree tolerance.
pub(crate) fn get_rotation(displaymatrix: &[i32; 9]) -> f64 {
    let mut theta = -round(display_rotation_get(displaymatrix));

    theta -= 360.0 * (theta / 360.0 + 0.9 / 360.0).floor();

    if (theta - 90.0 * (theta / 90.0).round()).abs() > 2.0 {
        warn!(
            "Odd rotation angle.\n\
            If you want to help, upload a sample \
            of this file to https://streams.videolan.org/upload/ \
            and contact the ffmpeg-devel mailing list. (ffmpeg-devel@ffmpeg.org)"
        );
    }

    theta
}

/// av_display_rotation_get (libavutil/display.c): counterclockwise rotation
/// angle in degrees, NaN for a degenerate matrix. The fixed-point scale
/// factor cancels out of the atan2 ratio, so any consistent divisor works.
fn display_rotation_get(matrix: &[i32; 9]) -> f64 {
    let mut scale = [0.0; 2];

    scale[0] = hypot(conv_fp(matrix[0]), conv_fp(matrix[3]));
    scale[1] = hypot(conv_fp(matrix[1]), conv_fp(matrix[4]));

    if scale[0] == 0.0 || scale[1] == 0.0 {
        return f64::NAN;
    }

    let rotation =
        (conv_fp(matrix[1]) / scale[1]).atan2(conv_fp(matrix[0]) / scale[0]) * 180.0 / PI;

    -rotation
}

fn hypot(x: f64, y: f64) -> f64 {
    (x.powi(2) + y.powi(2)).sqrt()
}

fn conv_fp(value: i32) -> f64 {
    value as f64 / 1_073_741_824.0 // CONV_FP converts fixed-point values
}

fn round(value: f64) -> f64 {
    value.round()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::AVPacketSideDataType::AV_PKT_DATA_STEREO3D;

    /// av_display_rotation_set(angle) with angle in clockwise degrees:
    /// matrix[0]=cos, matrix[1]=sin, matrix[3]=-sin, matrix[4]=cos of the
    /// negated angle, 16.16 fixed point (libavutil/display.c).
    fn matrix_for_clockwise(angle: f64) -> [i32; 9] {
        let radians = -angle * PI / 180.0;
        let c = radians.cos();
        let s = radians.sin();
        let conv = |v: f64| (v * 65536.0).round() as i32;
        [conv(c), conv(-s), 0, conv(s), conv(c), 0, 0, 0, 1 << 30]
    }

    fn entry(matrix: &[i32; 9]) -> AVPacketSideData {
        AVPacketSideData {
            data: matrix.as_ptr() as *mut u8,
            size: size_of_val(matrix),
            type_: AV_PKT_DATA_DISPLAYMATRIX,
        }
    }

    #[test]
    fn portrait_matrix_reports_90() {
        // iPhone portrait: the matrix says "rotate 90 degrees clockwise to
        // display upright" (av_display_rotation_get returns -90).
        let matrix = matrix_for_clockwise(90.0);
        assert_eq!(rotation_from_side_data(&[entry(&matrix)]), Some(90));
    }

    #[test]
    fn upside_down_matrix_reports_180() {
        let matrix = matrix_for_clockwise(180.0);
        assert_eq!(rotation_from_side_data(&[entry(&matrix)]), Some(180));
    }

    #[test]
    fn counterclockwise_matrix_reports_270() {
        let matrix = matrix_for_clockwise(270.0);
        assert_eq!(rotation_from_side_data(&[entry(&matrix)]), Some(270));
    }

    #[test]
    fn identity_matrix_reports_0() {
        let matrix = matrix_for_clockwise(0.0);
        assert_eq!(rotation_from_side_data(&[entry(&matrix)]), Some(0));
    }

    #[test]
    fn skips_non_matrix_entries() {
        let matrix = matrix_for_clockwise(90.0);
        let mut other = entry(&matrix);
        other.type_ = AV_PKT_DATA_STEREO3D;
        assert_eq!(rotation_from_side_data(&[other, entry(&matrix)]), Some(90));
    }

    #[test]
    fn rejects_truncated_matrix() {
        let matrix = matrix_for_clockwise(90.0);
        let mut short = entry(&matrix);
        short.size = 35;
        assert_eq!(rotation_from_side_data(&[short]), None);
    }

    #[test]
    fn degenerate_matrix_reports_none() {
        let matrix = [0i32; 9];
        assert_eq!(rotation_from_side_data(&[entry(&matrix)]), None);
    }

    #[test]
    fn no_side_data_reports_none() {
        assert_eq!(rotation_from_side_data(&[]), None);
    }
}
