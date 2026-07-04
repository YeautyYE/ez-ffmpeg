//! Pixel-format layout table: where each color component lives in the frame.

use ffmpeg_sys_next::AVPixelFormat;

/// Placement of one color component within the frame's planes.
pub(crate) struct ComponentPlacement {
    /// AVFrame `data`/`linesize` index.
    pub(crate) plane: usize,
    /// Byte offset of this component inside its pixel group.
    pub(crate) offset: usize,
    /// Byte step between horizontally adjacent samples.
    pub(crate) pixel_step: usize,
    /// log2 subsampling relative to the luma grid.
    pub(crate) hsub: u32,
    /// log2 subsampling relative to the luma grid.
    pub(crate) vsub: u32,
}

pub(crate) enum ColorModel {
    /// Components consume the matrix-converted `[Y, U, V]` triple.
    Yuv,
    /// Components consume the overlay's `[R, G, B]` directly.
    Rgb,
}

pub(crate) struct FormatSpec {
    pub(crate) model: ColorModel,
    /// yuvj\* formats are full-range by definition regardless of frame flags.
    pub(crate) force_full_range: bool,
    /// How destination samples are stored (8-bit bytes or little-endian u16
    /// containers for high-depth formats).
    pub(crate) sample: crate::subtitle::blend::SampleFormat,
    /// Component depth + shift: the scale FFmpeg's draw color math targets
    /// (8, 10, 12; 16 for P010 whose 10 bits are MSB-aligned).
    pub(crate) scale_bits: u32,
    /// `(source component index, placement)` — the alpha byte of RGBA-family
    /// formats is deliberately absent (left untouched, like FFmpeg's vf_ass
    /// without `alpha`).
    pub(crate) comps: &'static [(usize, ComponentPlacement)],
}

const fn comp(
    plane: usize,
    offset: usize,
    pixel_step: usize,
    hsub: u32,
    vsub: u32,
) -> ComponentPlacement {
    ComponentPlacement {
        plane,
        offset,
        pixel_step,
        hsub,
        vsub,
    }
}

use crate::subtitle::blend::SampleFormat;

/// (source component, placement) tables. `&[...]` literals promote to
/// `'static` in these static initializers.
const fn yuv(full: bool, comps: &'static [(usize, ComponentPlacement)]) -> FormatSpec {
    FormatSpec {
        model: ColorModel::Yuv,
        force_full_range: full,
        sample: SampleFormat::U8,
        scale_bits: 8,
        comps,
    }
}

const fn yuv_hd(scale_bits: u32, comps: &'static [(usize, ComponentPlacement)]) -> FormatSpec {
    FormatSpec {
        model: ColorModel::Yuv,
        force_full_range: false,
        sample: SampleFormat::U16Le,
        scale_bits,
        comps,
    }
}

const fn rgb(comps: &'static [(usize, ComponentPlacement)]) -> FormatSpec {
    FormatSpec {
        model: ColorModel::Rgb,
        force_full_range: false,
        sample: SampleFormat::U8,
        scale_bits: 8,
        comps,
    }
}

static PLANAR_420: [(usize, ComponentPlacement); 3] = [
    (0, comp(0, 0, 1, 0, 0)),
    (1, comp(1, 0, 1, 1, 1)),
    (2, comp(2, 0, 1, 1, 1)),
];
static PLANAR_422: [(usize, ComponentPlacement); 3] = [
    (0, comp(0, 0, 1, 0, 0)),
    (1, comp(1, 0, 1, 1, 0)),
    (2, comp(2, 0, 1, 1, 0)),
];
static PLANAR_444: [(usize, ComponentPlacement); 3] = [
    (0, comp(0, 0, 1, 0, 0)),
    (1, comp(1, 0, 1, 0, 0)),
    (2, comp(2, 0, 1, 0, 0)),
];

static YUV420P: FormatSpec = yuv(false, &PLANAR_420);
static YUVJ420P: FormatSpec = yuv(true, &PLANAR_420);
static YUV422P: FormatSpec = yuv(false, &PLANAR_422);
static YUVJ422P: FormatSpec = yuv(true, &PLANAR_422);
static YUV444P: FormatSpec = yuv(false, &PLANAR_444);
static YUVJ444P: FormatSpec = yuv(true, &PLANAR_444);

static NV12: FormatSpec = yuv(
    false,
    &[
        (0, comp(0, 0, 1, 0, 0)),
        (1, comp(1, 0, 2, 1, 1)),
        (2, comp(1, 1, 2, 1, 1)),
    ],
);
static NV21: FormatSpec = yuv(
    false,
    &[
        (0, comp(0, 0, 1, 0, 0)),
        (1, comp(1, 1, 2, 1, 1)),
        (2, comp(1, 0, 2, 1, 1)),
    ],
);

static RGB24: FormatSpec = rgb(&[
    (0, comp(0, 0, 3, 0, 0)),
    (1, comp(0, 1, 3, 0, 0)),
    (2, comp(0, 2, 3, 0, 0)),
]);
static BGR24: FormatSpec = rgb(&[
    (0, comp(0, 2, 3, 0, 0)),
    (1, comp(0, 1, 3, 0, 0)),
    (2, comp(0, 0, 3, 0, 0)),
]);
static RGBA: FormatSpec = rgb(&[
    (0, comp(0, 0, 4, 0, 0)),
    (1, comp(0, 1, 4, 0, 0)),
    (2, comp(0, 2, 4, 0, 0)),
]);
static BGRA: FormatSpec = rgb(&[
    (0, comp(0, 2, 4, 0, 0)),
    (1, comp(0, 1, 4, 0, 0)),
    (2, comp(0, 0, 4, 0, 0)),
]);
static ARGB: FormatSpec = rgb(&[
    (0, comp(0, 1, 4, 0, 0)),
    (1, comp(0, 2, 4, 0, 0)),
    (2, comp(0, 3, 4, 0, 0)),
]);
static ABGR: FormatSpec = rgb(&[
    (0, comp(0, 3, 4, 0, 0)),
    (1, comp(0, 2, 4, 0, 0)),
    (2, comp(0, 1, 4, 0, 0)),
]);

static GRAY8: FormatSpec = yuv(false, &[(0, comp(0, 0, 1, 0, 0))]);

// High-depth planar YUV (little-endian u16 samples, 2-byte steps).
static PLANAR_420_HD: [(usize, ComponentPlacement); 3] = [
    (0, comp(0, 0, 2, 0, 0)),
    (1, comp(1, 0, 2, 1, 1)),
    (2, comp(2, 0, 2, 1, 1)),
];
static PLANAR_422_HD: [(usize, ComponentPlacement); 3] = [
    (0, comp(0, 0, 2, 0, 0)),
    (1, comp(1, 0, 2, 1, 0)),
    (2, comp(2, 0, 2, 1, 0)),
];
static PLANAR_444_HD: [(usize, ComponentPlacement); 3] = [
    (0, comp(0, 0, 2, 0, 0)),
    (1, comp(1, 0, 2, 0, 0)),
    (2, comp(2, 0, 2, 0, 0)),
];

static YUV420P10: FormatSpec = yuv_hd(10, &PLANAR_420_HD);
static YUV422P10: FormatSpec = yuv_hd(10, &PLANAR_422_HD);
static YUV444P10: FormatSpec = yuv_hd(10, &PLANAR_444_HD);
static YUV420P12: FormatSpec = yuv_hd(12, &PLANAR_420_HD);

/// P010: 10 bits MSB-aligned in u16 containers (scale = depth+shift = 16,
/// exactly how FFmpeg's draw treats it); semi-planar u16 chroma pairs.
static P010: FormatSpec = yuv_hd(
    16,
    &[
        (0, comp(0, 0, 2, 0, 0)),
        (1, comp(1, 0, 4, 1, 1)),
        (2, comp(1, 2, 4, 1, 1)),
    ],
);

/// Human-readable list for error messages.
pub(crate) const SUPPORTED_LIST: &str = "yuv420p, yuvj420p, yuv422p, yuvj422p, yuv444p, \
     yuvj444p, nv12, nv21, rgb24, bgr24, rgba, bgra, argb, abgr, gray8, \
     yuv420p10le, yuv422p10le, yuv444p10le, yuv420p12le, p010le \
     (big-endian and alpha-plane formats are not supported)";

/// Looks up the layout for a raw `AVFrame.format` id. Takes an `i32` (not
/// `AVPixelFormat`) so a frame carrying an unlisted format id is rejected by
/// value comparison, never by constructing an out-of-range enum (which would
/// be UB).
pub(crate) fn format_spec(format: i32) -> Option<&'static FormatSpec> {
    use AVPixelFormat::*;
    Some(match format {
        f if f == AV_PIX_FMT_YUV420P as i32 => &YUV420P,
        f if f == AV_PIX_FMT_YUVJ420P as i32 => &YUVJ420P,
        f if f == AV_PIX_FMT_YUV422P as i32 => &YUV422P,
        f if f == AV_PIX_FMT_YUVJ422P as i32 => &YUVJ422P,
        f if f == AV_PIX_FMT_YUV444P as i32 => &YUV444P,
        f if f == AV_PIX_FMT_YUVJ444P as i32 => &YUVJ444P,
        f if f == AV_PIX_FMT_NV12 as i32 => &NV12,
        f if f == AV_PIX_FMT_NV21 as i32 => &NV21,
        f if f == AV_PIX_FMT_RGB24 as i32 => &RGB24,
        f if f == AV_PIX_FMT_BGR24 as i32 => &BGR24,
        f if f == AV_PIX_FMT_RGBA as i32 => &RGBA,
        f if f == AV_PIX_FMT_BGRA as i32 => &BGRA,
        f if f == AV_PIX_FMT_ARGB as i32 => &ARGB,
        f if f == AV_PIX_FMT_ABGR as i32 => &ABGR,
        f if f == AV_PIX_FMT_GRAY8 as i32 => &GRAY8,
        f if f == AV_PIX_FMT_YUV420P10LE as i32 => &YUV420P10,
        f if f == AV_PIX_FMT_YUV422P10LE as i32 => &YUV422P10,
        f if f == AV_PIX_FMT_YUV444P10LE as i32 => &YUV444P10,
        f if f == AV_PIX_FMT_YUV420P12LE as i32 => &YUV420P12,
        f if f == AV_PIX_FMT_P010LE as i32 => &P010,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::AVPixelFormat;
    use ffmpeg_sys_next::AVPixelFormat::*;

    /// Test shim: the production `format_spec` takes a raw `i32` format id;
    /// these tests exercise it with typed `AVPixelFormat` variants.
    fn format_spec(format: AVPixelFormat) -> Option<&'static FormatSpec> {
        super::format_spec(format as i32)
    }

    #[test]
    fn rejects_out_of_range_format_ids_without_ub() {
        // A raw format id that is not a valid AVPixelFormat discriminant must
        // be rejected by value, never transmuted into an out-of-range enum.
        assert!(super::format_spec(999_999).is_none());
        assert!(super::format_spec(-7).is_none());
        assert!(super::format_spec(i32::MAX).is_none());
    }

    #[test]
    fn covers_expected_formats_and_rejects_others() {
        for format in [
            AV_PIX_FMT_YUV420P,
            AV_PIX_FMT_YUVJ420P,
            AV_PIX_FMT_YUV422P,
            AV_PIX_FMT_YUV444P,
            AV_PIX_FMT_NV12,
            AV_PIX_FMT_NV21,
            AV_PIX_FMT_RGB24,
            AV_PIX_FMT_BGRA,
            AV_PIX_FMT_GRAY8,
            AV_PIX_FMT_YUV420P10LE,
            AV_PIX_FMT_YUV422P10LE,
            AV_PIX_FMT_YUV444P10LE,
            AV_PIX_FMT_YUV420P12LE,
            AV_PIX_FMT_P010LE,
        ] {
            assert!(format_spec(format).is_some(), "{format:?}");
        }
        assert!(
            format_spec(AV_PIX_FMT_YUV420P10BE).is_none(),
            "big-endian is unsupported"
        );
        assert!(format_spec(AV_PIX_FMT_YUVA420P).is_none());
    }

    #[test]
    fn high_depth_specs_carry_sample_and_scale() {
        use crate::subtitle::blend::SampleFormat;

        let p10 = format_spec(AV_PIX_FMT_YUV420P10LE).unwrap();
        assert_eq!(p10.sample, SampleFormat::U16Le);
        assert_eq!(p10.scale_bits, 10);
        assert_eq!(p10.comps[0].1.pixel_step, 2);

        let p12 = format_spec(AV_PIX_FMT_YUV420P12LE).unwrap();
        assert_eq!(p12.scale_bits, 12);

        // P010: full 16-bit container scale; semi-planar u16 chroma pairs.
        let p010 = format_spec(AV_PIX_FMT_P010LE).unwrap();
        assert_eq!(p010.sample, SampleFormat::U16Le);
        assert_eq!(p010.scale_bits, 16);
        assert_eq!(p010.comps[1].1.plane, 1);
        assert_eq!(p010.comps[1].1.offset, 0);
        assert_eq!(p010.comps[2].1.offset, 2);
        assert_eq!(p010.comps[2].1.pixel_step, 4);

        let p8 = format_spec(AV_PIX_FMT_YUV420P).unwrap();
        assert_eq!(p8.sample, SampleFormat::U8);
        assert_eq!(p8.scale_bits, 8);
    }

    #[test]
    fn nv12_and_nv21_interleave_chroma_correctly() {
        let nv12 = format_spec(AV_PIX_FMT_NV12).unwrap();
        // (source U) at plane 1 offset 0, (source V) at plane 1 offset 1.
        assert_eq!(nv12.comps[1].1.plane, 1);
        assert_eq!(nv12.comps[1].1.offset, 0);
        assert_eq!(nv12.comps[2].1.offset, 1);
        assert_eq!(nv12.comps[2].1.pixel_step, 2);

        let nv21 = format_spec(AV_PIX_FMT_NV21).unwrap();
        assert_eq!(nv21.comps[1].1.offset, 1, "NV21 stores V first");
        assert_eq!(nv21.comps[2].1.offset, 0);
    }

    #[test]
    fn bgr24_reverses_component_offsets() {
        let bgr = format_spec(AV_PIX_FMT_BGR24).unwrap();
        assert_eq!(bgr.comps[0].1.offset, 2, "R lives at byte 2 in BGR24");
        assert_eq!(bgr.comps[2].1.offset, 0, "B lives at byte 0 in BGR24");
    }

    #[test]
    fn yuvj_formats_force_full_range() {
        assert!(format_spec(AV_PIX_FMT_YUVJ420P).unwrap().force_full_range);
        assert!(!format_spec(AV_PIX_FMT_YUV420P).unwrap().force_full_range);
    }
}
