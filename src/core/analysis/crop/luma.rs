//! Read-only luma view over a decoded `AVFrame`.
//!
//! This module is the only unsafe boundary in native crop detection: after
//! pointer, size, stride, format, and overflow checks succeed, the rest of
//! the scanner sees a safe [`LumaAccess`] implementation.

use ffmpeg_next::Frame;
use ffmpeg_sys_next::{av_get_pix_fmt_name, AVPixelFormat, AV_FRAME_FLAG_INTERLACED};
#[cfg(test)]
use std::cell::Cell;
use std::ffi::CStr;
use std::marker::PhantomData;
use std::ptr;

/// Chroma siting used when expanding a crop rectangle so the result can be
/// fed to a typical YUV encoder without cutting content.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChromaGrid {
    /// 4:2:0 (and NV12 / P010): x and y even.
    Yuv420,
    /// 4:2:2: x even.
    Yuv422,
    /// 4:4:4 / gray: no extra alignment.
    None,
}

impl ChromaGrid {
    pub(crate) fn x_step(self) -> i32 {
        match self {
            ChromaGrid::Yuv420 | ChromaGrid::Yuv422 => 2,
            ChromaGrid::None => 1,
        }
    }

    pub(crate) fn y_step(self) -> i32 {
        match self {
            ChromaGrid::Yuv420 => 2,
            ChromaGrid::Yuv422 | ChromaGrid::None => 1,
        }
    }
}

/// Declared digital range of the luma codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalRange {
    Limited,
    Full,
}

/// How a luma sample is packed in memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SamplePacking {
    U8,
    /// 10-bit in the low bits of a 16-bit container (planar YUV / gray10).
    Low10 {
        big_endian: bool,
    },
    /// 10-bit in the high bits of a 16-bit container (P010).
    High10 {
        big_endian: bool,
    },
}

impl SamplePacking {
    pub(crate) fn bytes_per_sample(self) -> usize {
        match self {
            SamplePacking::U8 => 1,
            SamplePacking::Low10 { .. } | SamplePacking::High10 { .. } => 2,
        }
    }

    pub(crate) fn bit_depth(self) -> u8 {
        match self {
            SamplePacking::U8 => 8,
            SamplePacking::Low10 { .. } | SamplePacking::High10 { .. } => 10,
        }
    }
}

/// Why a frame cannot be scanned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CropFrameError {
    Unsupported {
        format_name: String,
        hw: bool,
    },
    InvalidGeometry(&'static str),
    /// Progressive-only scanner: interlaced fields are not modeled.
    Interlaced,
}

impl std::fmt::Display for CropFrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CropFrameError::Unsupported { format_name, hw: true } => write!(
                f,
                "Rust crop detection cannot read hardware frames (format {format_name}, hw_frames_ctx is set). Insert an explicit hwdownload before the frame pipeline."
            ),
            CropFrameError::Unsupported { format_name, hw: false } => write!(
                f,
                "Rust crop detection supports planar YUV 4:2:0/4:2:2/4:4:4, NV12, P010, and gray frames at 8 or 10 bits; got {format_name}. Insert an explicit format conversion before the frame pipeline."
            ),
            CropFrameError::InvalidGeometry(msg) => {
                write!(f, "Rust crop detection rejected a malformed video frame: {msg}")
            }
            CropFrameError::Interlaced => write!(
                f,
                "Rust crop detection scans progressive frames only; this frame is interlaced. Deinterlace before the frame pipeline."
            ),
        }
    }
}

impl std::error::Error for CropFrameError {}

/// Read-only access to unpacked luma codes plus geometry metadata.
pub(crate) trait LumaAccess {
    fn width(&self) -> u32;
    fn height(&self) -> u32;
    fn bit_depth(&self) -> u8;
    fn chroma_grid(&self) -> ChromaGrid;
    fn signal_range(&self) -> SignalRange;
    /// Origin of the visible rectangle in the allocated frame.
    fn origin_x(&self) -> i32 {
        0
    }
    fn origin_y(&self) -> i32 {
        0
    }
    /// Allocated frame size (visible origin + size is inside this).
    fn frame_width(&self) -> u32 {
        self.width()
    }
    fn frame_height(&self) -> u32 {
        self.height()
    }
    fn sample(&self, x: u32, y: u32) -> u16;
    #[cfg(test)]
    fn probe_count(&self) -> u32;
}

/// Checked view of plane 0 of a supported CPU frame.
pub(crate) struct LumaView<'a> {
    base: *const u8,
    linesize: isize,
    width: u32,
    height: u32,
    origin_x: i32,
    origin_y: i32,
    frame_width: u32,
    frame_height: u32,
    packing: SamplePacking,
    chroma: ChromaGrid,
    range: SignalRange,
    #[cfg(test)]
    probes: Cell<u32>,
    _borrow: PhantomData<&'a [u8]>,
}

// SAFETY: `LumaView` only reads pixels. The filter pipeline is `Send` per
// frame; we never share `&LumaView` across threads.
unsafe impl<'a> Send for LumaView<'a> {}

impl<'a> LumaView<'a> {
    /// True for null / props-only flush markers that must not consume
    /// `skip_initial` and must not run format validation.
    ///
    /// Hardware frames with a live surface (`hw_frames_ctx` set and any
    /// plane or buffer present) return false so skip can drop them before
    /// [`try_from_frame`] would error.
    pub(crate) fn is_passthrough_marker(frame: &Frame) -> bool {
        // SAFETY: null-checked before any field access.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return true;
        }
        unsafe {
            if !(*p).hw_frames_ctx.is_null() {
                return (*p).buf[0].is_null() && (*p).data.iter().all(|d| d.is_null());
            }
            (*p).data[0].is_null()
        }
    }

    /// Returns `Ok(None)` for null / props-only frames that the pipeline
    /// should pass through without scanning.
    pub(crate) fn try_from_frame(frame: &'a Frame) -> Result<Option<Self>, CropFrameError> {
        // SAFETY: null-checked before any field access.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return Ok(None);
        }
        unsafe { Self::try_from_avframe_ptr(p) }
    }

    /// # Safety
    /// `p` must be a valid `AVFrame` for the duration of the returned view.
    pub(crate) unsafe fn try_from_avframe_ptr(
        p: *const ffmpeg_sys_next::AVFrame,
    ) -> Result<Option<Self>, CropFrameError> {
        if p.is_null() {
            return Ok(None);
        }
        let width = (*p).width;
        let height = (*p).height;
        let format = (*p).format;
        // Distinguish a props-only flush marker from a real hardware frame.
        // VAAPI (and some other hwaccels) keep the surface in data[3] with
        // data[0] null; treating null luma as flush would skip those frames
        // instead of demanding hwdownload. Match `frame_is_eof_marker`:
        // no buffers and no data planes.
        if !(*p).hw_frames_ctx.is_null() {
            let props_only = (*p).buf[0].is_null() && (*p).data.iter().all(|d| d.is_null());
            if props_only {
                return Ok(None);
            }
            return Err(CropFrameError::Unsupported {
                format_name: pix_fmt_name(format),
                hw: true,
            });
        }
        if (*p).data[0].is_null() {
            return Ok(None);
        }
        if (*p).flags & AV_FRAME_FLAG_INTERLACED != 0 {
            return Err(CropFrameError::Interlaced);
        }
        if width <= 0 || height <= 0 {
            return Err(CropFrameError::InvalidGeometry(
                "width and height must be positive",
            ));
        }
        let Some(desc) = describe_format(format) else {
            return Err(CropFrameError::Unsupported {
                format_name: pix_fmt_name(format),
                hw: false,
            });
        };

        let crop_left = sat_crop((*p).crop_left);
        let crop_right = sat_crop((*p).crop_right);
        let crop_top = sat_crop((*p).crop_top);
        let crop_bottom = sat_crop((*p).crop_bottom);
        let vis_w = (width as u32)
            .checked_sub(crop_left)
            .and_then(|v| v.checked_sub(crop_right))
            .ok_or(CropFrameError::InvalidGeometry(
                "crop_left/crop_right exceed width",
            ))?;
        let vis_h = (height as u32)
            .checked_sub(crop_top)
            .and_then(|v| v.checked_sub(crop_bottom))
            .ok_or(CropFrameError::InvalidGeometry(
                "crop_top/crop_bottom exceed height",
            ))?;
        if vis_w == 0 || vis_h == 0 {
            return Err(CropFrameError::InvalidGeometry(
                "visible crop rectangle is empty",
            ));
        }

        let bps = desc.packing.bytes_per_sample();
        let min_stride =
            (vis_w as usize)
                .checked_mul(bps)
                .ok_or(CropFrameError::InvalidGeometry(
                    "width * bytes_per_sample overflow",
                ))?;
        // Plane 0 is full resolution for every supported format; crop offsets
        // are applied as pointer adjustment so linesize is checked against the
        // allocated width, not the visible width.
        let min_stride_alloc =
            (width as usize)
                .checked_mul(bps)
                .ok_or(CropFrameError::InvalidGeometry(
                    "allocated width * bytes_per_sample overflow",
                ))?;
        let linesize = (*p).linesize[0] as isize;
        let abs_stride = linesize.unsigned_abs();
        if abs_stride < min_stride_alloc {
            return Err(CropFrameError::InvalidGeometry(
                "linesize[0] is smaller than width * bytes_per_sample",
            ));
        }
        let _ = min_stride;

        let bps_i = isize::try_from(bps)
            .map_err(|_| CropFrameError::InvalidGeometry("bytes_per_sample does not fit isize"))?;
        let max_x = isize::try_from(vis_w.saturating_sub(1))
            .map_err(|_| CropFrameError::InvalidGeometry("visible width does not fit isize"))?;
        let max_y = isize::try_from(vis_h.saturating_sub(1))
            .map_err(|_| CropFrameError::InvalidGeometry("visible height does not fit isize"))?;
        let last_row = max_y
            .checked_mul(linesize)
            .ok_or(CropFrameError::InvalidGeometry(
                "row offset overflows isize",
            ))?;
        let last_col = max_x
            .checked_mul(bps_i)
            .ok_or(CropFrameError::InvalidGeometry(
                "sample offset overflows isize",
            ))?;
        let _ = last_row
            .checked_add(last_col)
            .ok_or(CropFrameError::InvalidGeometry(
                "luma sample address overflows isize",
            ))?;

        let origin_x = crop_left as i32;
        let origin_y = crop_top as i32;
        let row0 =
            if linesize >= 0 {
                let row_bytes = (crop_top as usize).checked_mul(abs_stride).ok_or(
                    CropFrameError::InvalidGeometry("crop_top * linesize overflow"),
                )?;
                let col_bytes = (crop_left as usize).checked_mul(bps).ok_or(
                    CropFrameError::InvalidGeometry("crop_left * bytes_per_sample overflow"),
                )?;
                let off =
                    row_bytes
                        .checked_add(col_bytes)
                        .ok_or(CropFrameError::InvalidGeometry(
                            "visible-origin pointer overflow",
                        ))?;
                (*p).data[0].add(off)
            } else {
                // Negative stride: data[0] already points at the first displayed
                // row. Move down `crop_top` displayed rows (which is toward lower
                // addresses) and right `crop_left` samples.
                let crop_top_i = isize::try_from(crop_top)
                    .map_err(|_| CropFrameError::InvalidGeometry("crop_top does not fit isize"))?;
                let row_off =
                    crop_top_i
                        .checked_mul(linesize)
                        .ok_or(CropFrameError::InvalidGeometry(
                            "crop_top * linesize overflow",
                        ))?;
                let col_bytes = (crop_left as usize).checked_mul(bps).ok_or(
                    CropFrameError::InvalidGeometry("crop_left * bytes_per_sample overflow"),
                )?;
                (*p).data[0].offset(row_off).add(col_bytes)
            };

        let range = match desc.forced_range {
            Some(r) => r,
            None => match (*p).color_range {
                ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_JPEG => SignalRange::Full,
                _ => SignalRange::Limited,
            },
        };

        Ok(Some(Self {
            base: row0,
            linesize,
            width: vis_w,
            height: vis_h,
            origin_x,
            origin_y,
            frame_width: width as u32,
            frame_height: height as u32,
            packing: desc.packing,
            chroma: desc.chroma,
            range,
            #[cfg(test)]
            probes: Cell::new(0),
            _borrow: PhantomData,
        }))
    }

    /// Builds a view over a test buffer. `bytes` must outlive the view and
    /// cover every sampled row for the given stride.
    #[cfg(test)]
    pub(crate) fn from_bytes(
        bytes: &'a [u8],
        width: u32,
        height: u32,
        stride: isize,
        packing: SamplePacking,
        chroma: ChromaGrid,
        range: SignalRange,
    ) -> Result<Self, CropFrameError> {
        if width == 0 || height == 0 {
            return Err(CropFrameError::InvalidGeometry(
                "width and height must be positive",
            ));
        }
        let bps = packing.bytes_per_sample();
        let min_row = (width as usize)
            .checked_mul(bps)
            .ok_or(CropFrameError::InvalidGeometry("row size overflow"))?;
        let abs_stride = stride.unsigned_abs();
        if abs_stride < min_row {
            return Err(CropFrameError::InvalidGeometry("stride too small"));
        }
        let needed = (height as usize - 1)
            .checked_mul(abs_stride)
            .and_then(|v| v.checked_add(min_row))
            .ok_or(CropFrameError::InvalidGeometry("buffer size overflow"))?;
        if bytes.len() < needed {
            return Err(CropFrameError::InvalidGeometry(
                "buffer shorter than height * stride",
            ));
        }
        let bps_i = isize::try_from(bps)
            .map_err(|_| CropFrameError::InvalidGeometry("bytes_per_sample does not fit isize"))?;
        let max_x = isize::try_from(width.saturating_sub(1))
            .map_err(|_| CropFrameError::InvalidGeometry("visible width does not fit isize"))?;
        let max_y = isize::try_from(height.saturating_sub(1))
            .map_err(|_| CropFrameError::InvalidGeometry("visible height does not fit isize"))?;
        let last_row = max_y
            .checked_mul(stride)
            .ok_or(CropFrameError::InvalidGeometry(
                "row offset overflows isize",
            ))?;
        let last_col = max_x
            .checked_mul(bps_i)
            .ok_or(CropFrameError::InvalidGeometry(
                "sample offset overflows isize",
            ))?;
        let _ = last_row
            .checked_add(last_col)
            .ok_or(CropFrameError::InvalidGeometry(
                "luma sample address overflows isize",
            ))?;
        let base = if stride >= 0 {
            bytes.as_ptr()
        } else {
            // Display row 0 is the last stored row.
            let off = (height as usize - 1).checked_mul(abs_stride).ok_or(
                CropFrameError::InvalidGeometry("negative-stride origin overflow"),
            )?;
            unsafe { bytes.as_ptr().add(off) }
        };
        Ok(Self {
            base,
            linesize: stride,
            width,
            height,
            origin_x: 0,
            origin_y: 0,
            frame_width: width,
            frame_height: height,
            packing,
            chroma,
            range,
            #[cfg(test)]
            probes: Cell::new(0),
            _borrow: PhantomData,
        })
    }
}

impl LumaAccess for LumaView<'_> {
    fn width(&self) -> u32 {
        self.width
    }
    fn height(&self) -> u32 {
        self.height
    }
    fn bit_depth(&self) -> u8 {
        self.packing.bit_depth()
    }
    fn chroma_grid(&self) -> ChromaGrid {
        self.chroma
    }
    fn signal_range(&self) -> SignalRange {
        self.range
    }
    fn origin_x(&self) -> i32 {
        self.origin_x
    }
    fn origin_y(&self) -> i32 {
        self.origin_y
    }
    fn frame_width(&self) -> u32 {
        self.frame_width
    }
    fn frame_height(&self) -> u32 {
        self.frame_height
    }

    fn sample(&self, x: u32, y: u32) -> u16 {
        debug_assert!(x < self.width && y < self.height);
        #[cfg(test)]
        self.probes.set(self.probes.get().saturating_add(1));
        let bps = self.packing.bytes_per_sample() as isize;
        // SAFETY: constructor rejected any `y * linesize + x * bps` that does
        // not fit `isize`, and verified `x < width` / `y < height`.
        unsafe {
            let ptr = self
                .base
                .offset(y as isize * self.linesize + x as isize * bps);
            read_sample(ptr, self.packing)
        }
    }

    #[cfg(test)]
    fn probe_count(&self) -> u32 {
        self.probes.get()
    }
}

unsafe fn read_sample(ptr: *const u8, packing: SamplePacking) -> u16 {
    match packing {
        SamplePacking::U8 => *ptr as u16,
        SamplePacking::Low10 { big_endian } => {
            let mut buf = [0u8; 2];
            ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 2);
            let packed = if big_endian {
                u16::from_be_bytes(buf)
            } else {
                u16::from_le_bytes(buf)
            };
            packed & 0x03FF
        }
        SamplePacking::High10 { big_endian } => {
            let mut buf = [0u8; 2];
            ptr::copy_nonoverlapping(ptr, buf.as_mut_ptr(), 2);
            let packed = if big_endian {
                u16::from_be_bytes(buf)
            } else {
                u16::from_le_bytes(buf)
            };
            packed >> 6
        }
    }
}

struct FormatDesc {
    packing: SamplePacking,
    chroma: ChromaGrid,
    forced_range: Option<SignalRange>,
}

fn describe_format(format: i32) -> Option<FormatDesc> {
    use AVPixelFormat::*;
    let eq = |pix: AVPixelFormat| format == pix as i32;
    if eq(AV_PIX_FMT_YUV420P) || eq(AV_PIX_FMT_NV12) || eq(AV_PIX_FMT_NV21) {
        Some(FormatDesc {
            packing: SamplePacking::U8,
            chroma: ChromaGrid::Yuv420,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV422P) {
        Some(FormatDesc {
            packing: SamplePacking::U8,
            chroma: ChromaGrid::Yuv422,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV444P) || eq(AV_PIX_FMT_GRAY8) {
        Some(FormatDesc {
            packing: SamplePacking::U8,
            chroma: ChromaGrid::None,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUVJ420P) {
        Some(FormatDesc {
            packing: SamplePacking::U8,
            chroma: ChromaGrid::Yuv420,
            forced_range: Some(SignalRange::Full),
        })
    } else if eq(AV_PIX_FMT_YUVJ422P) {
        Some(FormatDesc {
            packing: SamplePacking::U8,
            chroma: ChromaGrid::Yuv422,
            forced_range: Some(SignalRange::Full),
        })
    } else if eq(AV_PIX_FMT_YUVJ444P) {
        Some(FormatDesc {
            packing: SamplePacking::U8,
            chroma: ChromaGrid::None,
            forced_range: Some(SignalRange::Full),
        })
    } else if eq(AV_PIX_FMT_YUV420P10LE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: false },
            chroma: ChromaGrid::Yuv420,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV420P10BE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: true },
            chroma: ChromaGrid::Yuv420,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV422P10LE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: false },
            chroma: ChromaGrid::Yuv422,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV422P10BE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: true },
            chroma: ChromaGrid::Yuv422,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV444P10LE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: false },
            chroma: ChromaGrid::None,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_YUV444P10BE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: true },
            chroma: ChromaGrid::None,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_P010LE) {
        Some(FormatDesc {
            packing: SamplePacking::High10 { big_endian: false },
            chroma: ChromaGrid::Yuv420,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_P010BE) {
        Some(FormatDesc {
            packing: SamplePacking::High10 { big_endian: true },
            chroma: ChromaGrid::Yuv420,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_GRAY10LE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: false },
            chroma: ChromaGrid::None,
            forced_range: None,
        })
    } else if eq(AV_PIX_FMT_GRAY10BE) {
        Some(FormatDesc {
            packing: SamplePacking::Low10 { big_endian: true },
            chroma: ChromaGrid::None,
            forced_range: None,
        })
    } else {
        None
    }
}

fn pix_fmt_name(format: i32) -> String {
    // Only name values in the AVPixelFormat discriminant range. Transmuting
    // an unlisted i32 is UB even though the C API would return null.
    if (0..AVPixelFormat::AV_PIX_FMT_NB as i32).contains(&format) {
        // SAFETY: `format` is in 0..AV_PIX_FMT_NB.
        let p = unsafe { av_get_pix_fmt_name(std::mem::transmute::<i32, AVPixelFormat>(format)) };
        if !p.is_null() {
            return unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned();
        }
    }
    format!("unknown({format})")
}

fn sat_crop(v: usize) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

/// Procedural luma used by unit tests so 1080p / 4K probe-count cases do not
/// need a multi-megabyte buffer.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct PatternLuma {
    width: u32,
    height: u32,
    bit_depth: u8,
    chroma: ChromaGrid,
    range: SignalRange,
    pub top: u32,
    pub bottom: u32,
    pub left: u32,
    pub right: u32,
    pub black_code: u16,
    pub content_code: u16,
    /// Fraction of bar pixels that are salt (0.0..=1.0).
    pub noise_frac: f32,
    pub noise_seed: u32,
    /// When set, luma ramps from black at the edge to content at the centre.
    pub vignette: bool,
    probes: Cell<u32>,
}

#[cfg(test)]
impl PatternLuma {
    pub(crate) fn letterbox(width: u32, height: u32, bar: u32) -> Self {
        Self {
            width,
            height,
            bit_depth: 8,
            chroma: ChromaGrid::Yuv420,
            range: SignalRange::Limited,
            top: bar,
            bottom: bar,
            left: 0,
            right: 0,
            black_code: 16,
            content_code: 180,
            noise_frac: 0.0,
            noise_seed: 0,
            vignette: false,
            probes: Cell::new(0),
        }
    }

    pub(crate) fn windowbox(width: u32, height: u32, frac: f32) -> Self {
        let left = ((width as f32) * frac).round() as u32;
        let right = left;
        let top = ((height as f32) * frac).round() as u32;
        let bottom = top;
        Self {
            width,
            height,
            bit_depth: 8,
            chroma: ChromaGrid::Yuv420,
            range: SignalRange::Limited,
            top,
            bottom,
            left,
            right,
            black_code: 16,
            content_code: 180,
            noise_frac: 0.0,
            noise_seed: 0,
            vignette: false,
            probes: Cell::new(0),
        }
    }

    pub(crate) fn with_depth(mut self, bit_depth: u8) -> Self {
        let scale = ((1u32 << bit_depth) - 1) as f32 / 255.0;
        self.bit_depth = bit_depth;
        self.black_code = ((self.black_code as f32) * scale).round() as u16;
        self.content_code = ((self.content_code as f32) * scale).round() as u16;
        self
    }

    pub(crate) fn with_chroma(mut self, chroma: ChromaGrid) -> Self {
        self.chroma = chroma;
        self
    }

    pub(crate) fn with_range(mut self, range: SignalRange) -> Self {
        self.range = range;
        self
    }

    pub(crate) fn reset_probes(&self) {
        self.probes.set(0);
    }
}

#[cfg(test)]
impl LumaAccess for PatternLuma {
    fn width(&self) -> u32 {
        self.width
    }
    fn height(&self) -> u32 {
        self.height
    }
    fn bit_depth(&self) -> u8 {
        self.bit_depth
    }
    fn chroma_grid(&self) -> ChromaGrid {
        self.chroma
    }
    fn signal_range(&self) -> SignalRange {
        self.range
    }

    fn sample(&self, x: u32, y: u32) -> u16 {
        self.probes.set(self.probes.get().saturating_add(1));
        if self.vignette {
            let cx = (self.width as f32) / 2.0;
            let cy = (self.height as f32) / 2.0;
            let dx = (x as f32 - cx) / cx.max(1.0);
            let dy = (y as f32 - cy) / cy.max(1.0);
            let r = (dx * dx + dy * dy).sqrt().min(1.0);
            let t = 1.0 - r;
            return (self.black_code as f32
                + t * (self.content_code as f32 - self.black_code as f32))
                .round() as u16;
        }
        let in_bar = y < self.top
            || y >= self.height.saturating_sub(self.bottom)
            || x < self.left
            || x >= self.width.saturating_sub(self.right);
        if !in_bar {
            return self.content_code;
        }
        if self.noise_frac > 0.0 {
            let h = x
                .wrapping_mul(0x9E37_79B9)
                .wrapping_add(y.wrapping_mul(0x85EB_CA6B))
                .wrapping_add(self.noise_seed.wrapping_mul(0xC2B2_AE35));
            if (h >> 16) as f32 / 65536.0 < self.noise_frac {
                return self.content_code;
            }
        }
        self.black_code
    }

    fn probe_count(&self) -> u32 {
        self.probes.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::{av_frame_get_buffer, AVPixelFormat};

    fn alloc_gray8(w: i32, h: i32, fill: u8) -> Frame {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = w;
            (*p).height = h;
            (*p).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            let ls = (*p).linesize[0] as usize;
            let buf = std::slice::from_raw_parts_mut((*p).data[0], ls * h as usize);
            for row in 0..h as usize {
                for col in 0..w as usize {
                    buf[row * ls + col] = fill;
                }
            }
            frame
        }
    }

    #[test]
    fn accepts_gray8_and_counts_reads() {
        let frame = alloc_gray8(16, 8, 40);
        let view = LumaView::try_from_frame(&frame).unwrap().unwrap();
        assert_eq!(view.bit_depth(), 8);
        assert_eq!(view.chroma_grid(), ChromaGrid::None);
        assert_eq!(view.sample(0, 0), 40);
        assert_eq!(view.probe_count(), 1);
    }

    #[test]
    fn rejects_interlaced_progressive_scan() {
        unsafe {
            let mut frame = alloc_gray8(8, 8, 0);
            (*frame.as_mut_ptr()).flags |= AV_FRAME_FLAG_INTERLACED;
            let err = match LumaView::try_from_frame(&frame) {
                Err(e) => e,
                Ok(_) => panic!("expected interlaced frames to fail closed"),
            };
            let msg = err.to_string();
            assert!(msg.contains("interlaced"), "{msg}");
            assert!(msg.contains("Deinterlace"), "{msg}");
        }
    }

    #[test]
    fn interlaced_flush_null_plane_is_passthrough() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 8;
            (*p).height = 8;
            (*p).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
            (*p).flags |= AV_FRAME_FLAG_INTERLACED;
            assert!((*p).data[0].is_null());
            assert!(
                LumaView::try_from_frame(&frame).unwrap().is_none(),
                "interlaced flush frames with a null luma plane must skip, not error"
            );
        }
    }

    #[test]
    fn rejects_rgb() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 8;
            (*p).height = 8;
            (*p).format = AVPixelFormat::AV_PIX_FMT_RGB24 as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            let err = match LumaView::try_from_frame(&frame) {
                Err(e) => e,
                Ok(_) => panic!("expected unsupported RGB"),
            };
            let msg = err.to_string();
            assert!(msg.contains("rgb24"), "{msg}");
            assert!(msg.contains("format conversion"), "{msg}");
        }
    }

    #[test]
    fn rejects_packed_yuv() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 8;
            (*p).height = 8;
            (*p).format = AVPixelFormat::AV_PIX_FMT_YUYV422 as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            let err = match LumaView::try_from_frame(&frame) {
                Err(e) => e,
                Ok(_) => panic!("expected unsupported packed YUV"),
            };
            let msg = err.to_string();
            assert!(msg.contains("yuyv422"), "{msg}");
        }
    }

    #[test]
    fn hardware_frame_names_hwdownload() {
        let mut frame = alloc_gray8(8, 8, 0);
        unsafe {
            (*frame.as_mut_ptr()).hw_frames_ctx = 1 as *mut _;
            let err = match LumaView::try_from_frame(&frame) {
                Err(e) => e,
                Ok(_) => panic!("expected hardware-frame error"),
            };
            (*frame.as_mut_ptr()).hw_frames_ctx = ptr::null_mut();
            let msg = err.to_string();
            assert!(msg.contains("hwdownload"), "{msg}");
        }
    }

    #[test]
    fn hardware_flush_null_plane_is_passthrough() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 8;
            (*p).height = 8;
            (*p).format = AVPixelFormat::AV_PIX_FMT_NV12 as i32;
            (*p).hw_frames_ctx = 1 as *mut _;
            assert!((*p).data[0].is_null());
            assert!(
                LumaView::try_from_frame(&frame).unwrap().is_none(),
                "hw flush frames with a null luma plane must skip, not error"
            );
            (*p).hw_frames_ctx = ptr::null_mut();
        }
    }

    #[test]
    fn hardware_vaapi_shaped_frame_is_unsupported() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 8;
            (*p).height = 8;
            (*p).format = AVPixelFormat::AV_PIX_FMT_VAAPI as i32;
            (*p).hw_frames_ctx = 1 as *mut _;
            // VAAPI stores the surface in data[3]; data[0] stays null.
            (*p).data[3] = 1 as *mut _;
            let err = match LumaView::try_from_frame(&frame) {
                Err(e) => e,
                Ok(_) => panic!("VAAPI-shaped hardware frames must demand hwdownload"),
            };
            (*p).data[3] = ptr::null_mut();
            (*p).hw_frames_ctx = ptr::null_mut();
            let msg = err.to_string();
            assert!(msg.contains("hwdownload"), "{msg}");
        }
    }

    #[test]
    fn rejects_undersized_stride() {
        let mut frame = alloc_gray8(16, 8, 0);
        unsafe {
            (*frame.as_mut_ptr()).linesize[0] = 4;
            let err = match LumaView::try_from_frame(&frame) {
                Err(e) => e,
                Ok(_) => panic!("expected undersized stride"),
            };
            assert!(err.to_string().contains("linesize"));
            (*frame.as_mut_ptr()).linesize[0] = 16;
        }
    }

    #[test]
    fn props_only_is_skip() {
        unsafe {
            let frame = Frame::empty();
            assert!(LumaView::try_from_frame(&frame).unwrap().is_none());
        }
    }

    #[test]
    fn p010_ignores_low_six_bits() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 4;
            (*p).height = 2;
            (*p).format = AVPixelFormat::AV_PIX_FMT_P010LE as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            let ls = (*p).linesize[0] as usize;
            let buf = std::slice::from_raw_parts_mut((*p).data[0], ls * 2);
            // 10-bit code 96 << 6 = 6144, plus low-bit junk 0x3F.
            let packed = (96u16 << 6) | 0x3F;
            for i in 0..4 {
                buf[i * 2..i * 2 + 2].copy_from_slice(&packed.to_le_bytes());
            }
            let view = LumaView::try_from_frame(&frame).unwrap().unwrap();
            assert_eq!(view.sample(0, 0), 96);
            assert_eq!(view.sample(1, 0), 96);
        }
    }

    #[test]
    fn nv12_reads_plane0_only() {
        unsafe {
            let mut frame = Frame::empty();
            let p = frame.as_mut_ptr();
            (*p).width = 8;
            (*p).height = 4;
            (*p).format = AVPixelFormat::AV_PIX_FMT_NV12 as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            let ls = (*p).linesize[0] as usize;
            let y = std::slice::from_raw_parts_mut((*p).data[0], ls * 4);
            for row in 0..4 {
                for col in 0..8 {
                    y[row * ls + col] = 40;
                }
            }
            if !(*p).data[1].is_null() {
                let uv_ls = (*p).linesize[1] as usize;
                let uv = std::slice::from_raw_parts_mut((*p).data[1], uv_ls * 2);
                uv.fill(255);
            }
            let view = LumaView::try_from_frame(&frame).unwrap().unwrap();
            assert_eq!(view.chroma_grid(), ChromaGrid::Yuv420);
            assert_eq!(view.sample(0, 0), 40);
            assert_eq!(view.sample(7, 3), 40);
        }
    }

    #[test]
    fn planar10_le_be_match() {
        fn fill(frame: &mut Frame, code: u16, be: bool) {
            unsafe {
                let p = frame.as_mut_ptr();
                let ls = (*p).linesize[0] as usize;
                let h = (*p).height as usize;
                let w = (*p).width as usize;
                let buf = std::slice::from_raw_parts_mut((*p).data[0], ls * h);
                let bytes = if be {
                    code.to_be_bytes()
                } else {
                    code.to_le_bytes()
                };
                for y in 0..h {
                    for x in 0..w {
                        let o = y * ls + x * 2;
                        buf[o..o + 2].copy_from_slice(&bytes);
                    }
                }
            }
        }
        unsafe {
            let mut le = Frame::empty();
            let p = le.as_mut_ptr();
            (*p).width = 6;
            (*p).height = 4;
            (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P10LE as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            fill(&mut le, 96, false);

            let mut be = Frame::empty();
            let p = be.as_mut_ptr();
            (*p).width = 6;
            (*p).height = 4;
            (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P10BE as i32;
            assert!(av_frame_get_buffer(p, 1) >= 0);
            fill(&mut be, 96, true);

            let a = LumaView::try_from_frame(&le).unwrap().unwrap();
            let b = LumaView::try_from_frame(&be).unwrap().unwrap();
            assert_eq!(a.sample(2, 1), 96);
            assert_eq!(b.sample(2, 1), 96);
        }
    }

    #[test]
    fn negative_stride_reads_display_order() {
        let width = 8u32;
        let height = 4u32;
        let stride = 8isize;
        let mut stored = vec![0u8; (height as usize) * stride as usize];
        for y in 0..height as usize {
            for x in 0..width as usize {
                stored[y * stride as usize + x] = (y * 10 + x) as u8;
            }
        }
        // Reverse row order in memory, then view with negative stride.
        let mut phys = vec![0u8; stored.len()];
        for y in 0..height as usize {
            let src = y * stride as usize;
            let dst = (height as usize - 1 - y) * stride as usize;
            phys[dst..dst + stride as usize].copy_from_slice(&stored[src..src + stride as usize]);
        }
        let view = LumaView::from_bytes(
            &phys,
            width,
            height,
            -stride,
            SamplePacking::U8,
            ChromaGrid::None,
            SignalRange::Full,
        )
        .unwrap();
        assert_eq!(view.sample(3, 0), 3);
        assert_eq!(view.sample(3, 3), 33);
    }

    #[test]
    fn crop_fields_shift_origin() {
        let mut frame = alloc_gray8(16, 8, 7);
        unsafe {
            let p = frame.as_mut_ptr();
            (*p).crop_left = 2;
            (*p).crop_right = 2;
            (*p).crop_top = 1;
            (*p).crop_bottom = 1;
            let ls = (*p).linesize[0] as usize;
            *(*p).data[0].add(ls + 2) = 99;
        }
        let view = LumaView::try_from_frame(&frame).unwrap().unwrap();
        assert_eq!(view.width(), 12);
        assert_eq!(view.height(), 6);
        assert_eq!(view.origin_x(), 2);
        assert_eq!(view.origin_y(), 1);
        assert_eq!(view.sample(0, 0), 99);
    }
}
