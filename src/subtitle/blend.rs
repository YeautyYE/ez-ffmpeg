//! CPU alpha-blend of rendered subtitle overlays onto video frame planes.
//!
//! Entirely safe code: the renderer produces [`OverlayImage`] views over its
//! coverage bitmaps and the filter hands plane slices in as [`PlaneView`]s.
//!
//! The math matches FFmpeg's `ff_blend_mask()`/`blend_pixel()`/
//! `blend_pixel16()` and `ff_draw_color()` (drawutils.c, verified against tag
//! n7.1.3):
//! - 8-bit samples: fixed-point alpha `(0x10307 * opacity + 0x3) >> 8` and
//!   blend `(dst*(0x1010101 - a) + src*a) >> 24`,
//! - high-depth samples (little-endian u16): alpha `(0x101*opacity + 2) >> 8`
//!   and blend `(dst*(0x10001 - a) + src*a) >> 16`,
//! - per output pixel the mask is summed over its `(1<<hsub) x (1<<vsub)`
//!   block and truncated with `>> (hsub+vsub)` — out-of-mask samples count
//!   as zero (FFmpeg semantics, not edge clamping),
//! - colors scale to the target depth as
//!   `normalized * ((1 << scale_bits) - 1) + 0.5` truncated (so e.g.
//!   limited-range white at 10 bit is 943, FFmpeg's stretch — not the
//!   broadcast 940).

#[cfg(target_arch = "x86_64")]
mod avx2;

/// True when the AVX2 kernels are compiled in and the CPU supports them.
fn avx2_available() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        avx2::enabled()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        false
    }
}

/// One rendered coverage bitmap positioned on the frame (the libass
/// `ASS_Image` shape, kept for exact FFmpeg blend parity). Alpha 0 in
/// `color` = opaque.
pub(crate) struct OverlayImage<'a> {
    pub(crate) w: usize,
    pub(crate) h: usize,
    pub(crate) stride: usize,
    /// 1bpp coverage buffer. Length must be at least `stride*(h-1) + w`; the
    /// last row may be shorter than `stride` (inherited libass-layout
    /// contract), which this module never reads past.
    pub(crate) bitmap: &'a [u8],
    /// Bitmap color as 0xRRGGBBAA (ASS convention: alpha 0 = opaque).
    pub(crate) color: u32,
    pub(crate) dst_x: i32,
    pub(crate) dst_y: i32,
}

impl OverlayImage<'_> {
    /// RGB part of the color.
    pub(crate) fn rgb(&self) -> [u8; 3] {
        [
            (self.color >> 24) as u8,
            (self.color >> 16) as u8,
            (self.color >> 8) as u8,
        ]
    }

    /// Opacity in 0..=255 (255 = opaque); libass stores transparency.
    pub(crate) fn opacity(&self) -> u8 {
        255 - (self.color & 0xFF) as u8
    }
}

/// A writable view over one destination plane (or one component of an
/// interleaved plane: pass `data` offset to the component and the interleave
/// factor as `pixel_step`). All quantities are in BYTES.
pub(crate) struct PlaneView<'a> {
    pub(crate) data: &'a mut [u8],
    pub(crate) linesize: usize,
    /// Byte step between horizontally adjacent samples of this component
    /// (1 = 8-bit planar, 2 = 16-bit planar or NV12 chroma, 3/4 = packed
    /// RGB, 4 = P010 chroma).
    pub(crate) pixel_step: usize,
}

/// Color matrix used to convert subtitle RGB into the frame's YUV.
/// Variants mirror the entries of FFmpeg's `av_csp_luma_coeffs_from_avcsp`
/// table that `ff_draw_init2` accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColorMatrix {
    Bt601,
    Bt709,
    Bt2020,
    Fcc,
    Smpte240m,
    /// FFmpeg quirk preserved: YCoCg gets the standard luma-coefficient
    /// formula with (0.25, 0.5, 0.25), not a real YCoCg transform.
    YCoCg,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ColorRange {
    Limited,
    Full,
}

/// How samples of a destination plane are stored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SampleFormat {
    U8,
    /// Little-endian 16-bit container (10/12-bit planar LE and P010).
    U16Le,
}

impl SampleFormat {
    /// FFmpeg's fixed-point alpha for this sample width (drawutils.c):
    /// 8-bit `(0x10307*o + 3) >> 8` in `[0, 0x10203]`; 16-bit
    /// `(0x101*o + 2) >> 8` in `[0, 0x100]`.
    pub(crate) fn alpha_fixed(self, opacity: u8) -> u32 {
        match self {
            SampleFormat::U8 => (0x10307 * u32::from(opacity) + 0x3) >> 8,
            SampleFormat::U16Le => (0x101 * u32::from(opacity) + 0x2) >> 8,
        }
    }
}

/// Static sample codec: load/store one sample and the FFmpeg blend at that
/// width. Monomorphized so the hot loops carry no per-pixel branching.
trait Sample {
    const BYTES: usize;
    /// Mask pixels zero-tested at once by the skip-ahead in the hot loops.
    /// Must be a multiple of 8 (the test reads whole u64 words). Values are
    /// per-width bench winners (`bench_kernels.rs`, x86-64 SSE2 baseline):
    /// wider wins for u8, narrower for u16 whose blend chunk is twice the
    /// bytes.
    const SKIP_CHUNK: usize;
    fn load(bytes: &[u8]) -> u32;
    fn store(bytes: &mut [u8], value: u32);
    /// Every intermediate fits u32 exactly: the constants are chosen so the
    /// worst-case numerator equals `u32::MAX` precisely
    /// (`0x1010101*255` / `0x10001*65535`).
    fn blend(dst: u32, src: u32, alpha: u32) -> u32;
}

struct SampleU8;
impl Sample for SampleU8 {
    const BYTES: usize = 1;
    const SKIP_CHUNK: usize = 16;
    #[inline(always)]
    fn load(bytes: &[u8]) -> u32 {
        u32::from(bytes[0])
    }
    #[inline(always)]
    fn store(bytes: &mut [u8], value: u32) {
        bytes[0] = value as u8;
    }
    #[inline(always)]
    fn blend(dst: u32, src: u32, alpha: u32) -> u32 {
        ((0x0101_0101 - alpha) * dst + alpha * src) >> 24
    }
}

struct SampleU16Le;
impl Sample for SampleU16Le {
    const BYTES: usize = 2;
    const SKIP_CHUNK: usize = 8;
    #[inline(always)]
    fn load(bytes: &[u8]) -> u32 {
        u32::from(u16::from_le_bytes([bytes[0], bytes[1]]))
    }
    #[inline(always)]
    fn store(bytes: &mut [u8], value: u32) {
        bytes[..2].copy_from_slice(&(value as u16).to_le_bytes());
    }
    #[inline(always)]
    fn blend(dst: u32, src: u32, alpha: u32) -> u32 {
        ((0x0001_0001 - alpha) * dst + alpha * src) >> 16
    }
}

/// Converts sRGB to the frame's YCbCr component values, replicating FFmpeg's
/// `ff_draw_color` exactly: normalized double math, limited-range
/// scale+offset (or +0.5 chroma recentering for full range), then
/// `value = normalized * ((1 << scale_bits) - 1) + 0.5` truncated.
///
/// `scale_bits` is the component's depth + shift (8 for 8-bit, 10/12 for
/// planar high-depth, 16 for P010 whose samples are MSB-aligned).
pub(crate) fn yuv_components(
    rgb: [u8; 3],
    matrix: ColorMatrix,
    range: ColorRange,
    scale_bits: u32,
) -> [u32; 3] {
    let (kr, kb) = match matrix {
        ColorMatrix::Bt601 => (0.299, 0.114),
        ColorMatrix::Bt709 => (0.2126, 0.0722),
        ColorMatrix::Bt2020 => (0.2627, 0.0593),
        ColorMatrix::Fcc => (0.30, 0.11),
        ColorMatrix::Smpte240m => (0.212, 0.087),
        ColorMatrix::YCoCg => (0.25, 0.25),
    };
    let kg = 1.0 - kr - kb;
    let r = f64::from(rgb[0]) / 255.0;
    let g = f64::from(rgb[1]) / 255.0;
    let b = f64::from(rgb[2]) / 255.0;
    let y = kr * r + kg * g + kb * b;
    let cb = 0.5 * (b - y) / (1.0 - kb);
    let cr = 0.5 * (r - y) / (1.0 - kr);
    let (y, cb, cr) = match range {
        ColorRange::Limited => (
            y * (219.0 / 255.0) + 16.0 / 255.0,
            cb * (224.0 / 255.0) + 128.0 / 255.0,
            cr * (224.0 / 255.0) + 128.0 / 255.0,
        ),
        ColorRange::Full => (y, cb + 0.5, cr + 0.5),
    };
    [
        scale_component(y, scale_bits),
        scale_component(cb, scale_bits),
        scale_component(cr, scale_bits),
    ]
}

/// RGB-model frames take the color scaled to the component depth. Like
/// `ff_draw_color`, an explicit limited range applies the luma scale and
/// offset to every RGB channel (`chroma` is false for RGB components).
pub(crate) fn rgb_components(rgb: [u8; 3], range: ColorRange, scale_bits: u32) -> [u32; 3] {
    let map = |component: u8| -> u32 {
        let normalized = f64::from(component) / 255.0;
        let ranged = match range {
            ColorRange::Limited => normalized * (219.0 / 255.0) + 16.0 / 255.0,
            ColorRange::Full => normalized,
        };
        scale_component(ranged, scale_bits)
    };
    [map(rgb[0]), map(rgb[1]), map(rgb[2])]
}

fn scale_component(normalized: f64, scale_bits: u32) -> u32 {
    let max = ((1u64 << scale_bits) - 1) as f64;
    let value = normalized * max + 0.5;
    if value <= 0.0 {
        0
    } else if value >= u32::MAX as f64 {
        u32::MAX
    } else {
        value as u32 // C-style truncation, matching ff_draw_color
    }
}

#[allow(clippy::too_many_arguments)] // internal hot path; a params struct only adds ceremony
pub(crate) fn blend_component(
    plane: &mut PlaneView<'_>,
    grid_w: usize,
    grid_h: usize,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    hsub: u32,
    vsub: u32,
    sample: SampleFormat,
) {
    match sample {
        SampleFormat::U8 => {
            blend_typed::<SampleU8>(plane, grid_w, grid_h, image, src, alpha, hsub, vsub)
        }
        SampleFormat::U16Le => {
            blend_typed::<SampleU16Le>(plane, grid_w, grid_h, image, src, alpha, hsub, vsub)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn blend_typed<S: Sample>(
    plane: &mut PlaneView<'_>,
    grid_w: usize,
    grid_h: usize,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    hsub: u32,
    vsub: u32,
) {
    if alpha == 0 || image.w == 0 || image.h == 0 {
        return;
    }
    let Some((x0, xm0, w)) = clip_interval(grid_w, image.dst_x, image.w) else {
        return;
    };
    let Some((y0, ym0, h)) = clip_interval(grid_h, image.dst_y, image.h) else {
        return;
    };

    if hsub == 0 && vsub == 0 {
        // Tightly-packed planes take the AVX2 kernels when the CPU has
        // them; `S::BYTES` uniquely identifies the codec among the two
        // `Sample` impls, so the constant branches fold at monomorphization.
        #[cfg(target_arch = "x86_64")]
        if plane.pixel_step == S::BYTES && avx2::enabled() {
            if S::BYTES == SampleU8::BYTES {
                // SAFETY: `avx2::enabled()` just verified AVX2 support.
                unsafe {
                    avx2::blend_direct_u8(plane, image, src, alpha, (x0, y0), (xm0, ym0), (w, h))
                };
                return;
            }
            if S::BYTES == SampleU16Le::BYTES {
                // SAFETY: `avx2::enabled()` just verified AVX2 support.
                unsafe {
                    avx2::blend_direct_u16(plane, image, src, alpha, (x0, y0), (xm0, ym0), (w, h))
                };
                return;
            }
        }
        blend_direct::<S>(plane, image, src, alpha, (x0, y0), (xm0, ym0), (w, h));
    } else if hsub == 1 && vsub <= 1 {
        // Every subsampled format in the layout table (4:2:0, 4:2:2, NV12,
        // P010) lands here.
        blend_pooled_h2::<S>(plane, image, src, alpha, (x0, y0), (xm0, ym0), (w, h), vsub);
    } else {
        blend_pooled::<S>(
            plane,
            image,
            src,
            alpha,
            (x0, y0),
            (xm0, ym0),
            (w, h),
            (hsub, vsub),
        );
    }
}

/// 1:1 grid fast path (luma, 4:4:4 planes, packed RGB, gray): rows are
/// processed in `S::SKIP_CHUNK`-pixel blocks. A block whose mask words OR to
/// zero is skipped without touching the destination (common inside a glyph
/// bounding box); any other block is blended without per-pixel branching —
/// a zero mask blends to the exact identity and the straight-line body
/// autovectorizes. Bench (`bench_kernels.rs`): 2.0-2.6x over the previous
/// per-pixel skip on real libass masks, 2.2x on solid coverage.
fn blend_direct<S: Sample>(
    plane: &mut PlaneView<'_>,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    (x0, y0): (usize, usize),
    (xm0, ym0): (usize, usize),
    (w, h): (usize, usize),
) {
    let step = plane.pixel_step;
    for row in 0..h {
        let mask_start = (ym0 + row) * image.stride + xm0;
        let mask_row = &image.bitmap[mask_start..mask_start + w];
        let dst_start = (y0 + row) * plane.linesize + x0 * step;
        let dst_row = &mut plane.data[dst_start..dst_start + w * step];

        let mut dst_blocks = dst_row.chunks_exact_mut(S::SKIP_CHUNK * step);
        let mut mask_blocks = mask_row.chunks_exact(S::SKIP_CHUNK);
        for (dst_block, mask_block) in (&mut dst_blocks).zip(&mut mask_blocks) {
            if mask_is_zero(mask_block) {
                continue;
            }
            blend_span::<S>(dst_block, mask_block, src, alpha, step);
        }
        let tail_dst = dst_blocks.into_remainder();
        let tail_mask = mask_blocks.remainder();
        if !tail_mask.iter().all(|&mask| mask == 0) {
            blend_span::<S>(tail_dst, tail_mask, src, alpha, step);
        }
    }
}

/// Blends one contiguous pixel span without per-pixel branching: a zero mask
/// yields alpha 0, and `S::blend(dst, src, 0)` returns `dst` bit-exactly
/// (the blend constants replicate `dst` across the numerator).
fn blend_span<S: Sample>(dst: &mut [u8], mask: &[u8], src: u32, alpha: u32, step: usize) {
    for (chunk, &mask) in dst.chunks_exact_mut(step).zip(mask) {
        let sample = &mut chunk[..S::BYTES];
        let blended = S::blend(S::load(sample), src, u32::from(mask) * alpha);
        S::store(sample, blended);
    }
}

/// True when every mask byte is zero. Reads whole u64 words; callers pass
/// slices whose length is a multiple of 8.
fn mask_is_zero(mask: &[u8]) -> bool {
    mask.chunks_exact(8)
        .all(|word| u64::from_ne_bytes(word.try_into().expect("8-byte word")) == 0)
}

/// Horizontally-subsampled fast path (hsub == 1, vsub <= 1): each output
/// pixel pools a 2-wide mask block over one or two rows. Interior pixels
/// (full blocks) run through the same skip-block/branchless structure as
/// [`blend_direct`]; the at-most-one partial pixel per side and the
/// odd clipped rows keep FFmpeg's zero-pad semantics via [`pool_edge_px`].
/// Bench: the general path below spends most of its time on per-pixel
/// interval clamping and scattered reads (3.5ms per 1080p chroma pair on the
/// dense scenario — 3.5x the luma cost).
#[allow(clippy::too_many_arguments)]
fn blend_pooled_h2<S: Sample>(
    plane: &mut PlaneView<'_>,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    (x0, y0): (usize, usize),
    (xm0, ym0): (usize, usize),
    (w, h): (usize, usize),
    vsub: u32,
) {
    let step = plane.pixel_step;
    let shift = 1 + vsub;
    let px0 = x0 >> 1;
    let px1 = (x0 + w - 1) >> 1; // inclusive
    let py0 = y0 >> vsub;
    let py1 = (y0 + h - 1) >> vsub;

    // Interior pixels have their whole 2-wide block inside the clipped
    // columns; at most one partial pixel exists on each side.
    let left_partial = (px0 << 1) < x0;
    let right_partial = ((px1 + 1) << 1) > x0 + w;
    let ipx0 = px0 + usize::from(left_partial);
    let ipx1 = px1 + 1 - usize::from(right_partial); // exclusive

    for py in py0..=py1 {
        let gy0 = (py << vsub).max(y0);
        let gy1 = ((py + 1) << vsub).min(y0 + h);
        let two_rows = gy1 - gy0 == 2;
        let row0 = (gy0 - y0 + ym0) * image.stride;
        let dst_row = py * plane.linesize;

        if left_partial {
            let cols = (px0 << 1).max(x0)..((px0 + 1) << 1).min(x0 + w);
            let mask_off = row0 + (cols.start - x0 + xm0);
            let span = cols.len();
            pool_edge_px::<S>(
                plane,
                image,
                (mask_off, span, two_rows),
                src,
                alpha,
                shift,
                dst_row + px0 * step,
            );
        }
        if ipx0 < ipx1 {
            let n = ipx1 - ipx0;
            let mask0 = row0 + ((ipx0 << 1) - x0 + xm0);
            let top = &image.bitmap[mask0..mask0 + 2 * n];
            let dst_start = dst_row + ipx0 * step;
            let dst = &mut plane.data[dst_start..dst_start + n * step];
            if two_rows {
                let bot = &image.bitmap[mask0 + image.stride..mask0 + image.stride + 2 * n];
                pool_interior::<S, true>(dst, top, bot, src, alpha, shift, step);
            } else {
                pool_interior::<S, false>(dst, top, top, src, alpha, shift, step);
            }
        }
        if right_partial && (px1 > px0 || !left_partial) {
            let cols = (px1 << 1).max(x0)..((px1 + 1) << 1).min(x0 + w);
            let mask_off = row0 + (cols.start - x0 + xm0);
            let span = cols.len();
            pool_edge_px::<S>(
                plane,
                image,
                (mask_off, span, two_rows),
                src,
                alpha,
                shift,
                dst_row + px1 * step,
            );
        }
    }
}

/// Interior run of full 2-wide blocks: `S::SKIP_CHUNK` output pixels are
/// skipped at once when their mask words (both rows) OR to zero, otherwise
/// pooled and blended branchlessly. With `TWO_ROWS = false` the `bot` slice
/// is ignored (callers pass `top` again).
fn pool_interior<S: Sample, const TWO_ROWS: bool>(
    dst: &mut [u8],
    top: &[u8],
    bot: &[u8],
    src: u32,
    alpha: u32,
    shift: u32,
    step: usize,
) {
    let mask_block = 2 * S::SKIP_CHUNK;
    let mut dst_blocks = dst.chunks_exact_mut(S::SKIP_CHUNK * step);
    let mut top_blocks = top.chunks_exact(mask_block);
    let mut bot_blocks = bot.chunks_exact(mask_block);
    for ((dst_block, top_block), bot_block) in
        (&mut dst_blocks).zip(&mut top_blocks).zip(&mut bot_blocks)
    {
        if mask_is_zero(top_block) && (!TWO_ROWS || mask_is_zero(bot_block)) {
            continue;
        }
        pool_span::<S, TWO_ROWS>(dst_block, top_block, bot_block, src, alpha, shift, step);
    }
    let tail_dst = dst_blocks.into_remainder();
    let tail_top = top_blocks.remainder();
    let tail_bot = bot_blocks.remainder();
    let tail_zero = tail_top.iter().all(|&mask| mask == 0)
        && (!TWO_ROWS || tail_bot.iter().all(|&mask| mask == 0));
    if !tail_zero {
        pool_span::<S, TWO_ROWS>(tail_dst, tail_top, tail_bot, src, alpha, shift, step);
    }
}

/// Pools and blends one contiguous span of full 2-wide blocks branchlessly.
fn pool_span<S: Sample, const TWO_ROWS: bool>(
    dst: &mut [u8],
    top: &[u8],
    bot: &[u8],
    src: u32,
    alpha: u32,
    shift: u32,
    step: usize,
) {
    for ((chunk, pair), bot_pair) in dst
        .chunks_exact_mut(step)
        .zip(top.chunks_exact(2))
        .zip(bot.chunks_exact(2))
    {
        // Sums stay u16 (max 4*255) so LLVM can use narrow word lanes; the
        // widening happens once at the coverage multiply.
        let mut sum = u16::from(pair[0]) + u16::from(pair[1]);
        if TWO_ROWS {
            sum += u16::from(bot_pair[0]) + u16::from(bot_pair[1]);
        }
        let alpha_total = (u32::from(sum) >> shift) * alpha;
        let sample = &mut chunk[..S::BYTES];
        let blended = S::blend(S::load(sample), src, alpha_total);
        S::store(sample, blended);
    }
}

/// Pools one output pixel whose block is partially outside the clipped
/// columns (FFmpeg zero-pad semantics: missing samples count as zero, the
/// divisor stays `1 << shift`).
fn pool_edge_px<S: Sample>(
    plane: &mut PlaneView<'_>,
    image: &OverlayImage<'_>,
    (mask_off, span, two_rows): (usize, usize, bool),
    src: u32,
    alpha: u32,
    shift: u32,
    dst_index: usize,
) {
    let sum = edge_sum(image, mask_off, span, two_rows);
    let alpha_total = (u32::from(sum) >> shift) * alpha;
    if alpha_total == 0 {
        return;
    }
    let sample = &mut plane.data[dst_index..dst_index + S::BYTES];
    let blended = S::blend(S::load(sample), src, alpha_total);
    S::store(sample, blended);
}

/// Raw mask sum of one (possibly partial) 2-wide block. Max 4 * 255, fits
/// u16.
fn edge_sum(image: &OverlayImage<'_>, mask_off: usize, span: usize, two_rows: bool) -> u16 {
    let mut sum = 0u16;
    for &mask in &image.bitmap[mask_off..mask_off + span] {
        sum += u16::from(mask);
    }
    if two_rows {
        let bot = mask_off + image.stride;
        for &mask in &image.bitmap[bot..bot + span] {
            sum += u16::from(mask);
        }
    }
    sum
}

/// Placement of one node's pooled sums relative to a subsampled plane:
/// output pixels `[px0, px0+pw) x [py0, py0+ph)` with divisor `1 << shift`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PooledRect {
    pub(crate) px0: usize,
    pub(crate) py0: usize,
    pub(crate) pw: usize,
    pub(crate) ph: usize,
    pub(crate) shift: u32,
}

/// Pools one node's clipped mask at hsub == 1 into raw per-output-pixel
/// sums (row-major into `out`, `pw * ph` entries), so several chroma
/// components can be blended from ONE mask traversal
/// ([`blend_pooled_from_sums`] per component). Returns `None` when the node
/// is fully clipped away.
///
/// Geometry and zero-pad semantics are identical to the fused
/// [`blend_pooled_h2`] path; the equivalence test in `lab` holds the two
/// routes byte-exact against each other.
pub(crate) fn pool_sums_h2(
    grid_w: usize,
    grid_h: usize,
    image: &OverlayImage<'_>,
    vsub: u32,
    out: &mut Vec<u16>,
) -> Option<PooledRect> {
    pool_sums_h2_impl(grid_w, grid_h, image, vsub, out, avx2_available())
}

/// [`pool_sums_h2`] body. `allow_simd` lets the bench/test harness force
/// the scalar interior loops; the shipping wrapper passes
/// [`avx2_available`].
fn pool_sums_h2_impl(
    grid_w: usize,
    grid_h: usize,
    image: &OverlayImage<'_>,
    vsub: u32,
    out: &mut Vec<u16>,
    allow_simd: bool,
) -> Option<PooledRect> {
    if image.w == 0 || image.h == 0 {
        return None;
    }
    let (x0, xm0, w) = clip_interval(grid_w, image.dst_x, image.w)?;
    let (y0, ym0, h) = clip_interval(grid_h, image.dst_y, image.h)?;

    let shift = 1 + vsub;
    let px0 = x0 >> 1;
    let px1 = (x0 + w - 1) >> 1;
    let py0 = y0 >> vsub;
    let py1 = (y0 + h - 1) >> vsub;
    let pw = px1 - px0 + 1;
    let ph = py1 - py0 + 1;
    out.clear();
    out.resize(pw * ph, 0);

    let left_partial = (px0 << 1) < x0;
    let right_partial = ((px1 + 1) << 1) > x0 + w;
    let ipx0 = px0 + usize::from(left_partial);
    let ipx1 = px1 + 1 - usize::from(right_partial); // exclusive

    for py in py0..=py1 {
        let gy0 = (py << vsub).max(y0);
        let gy1 = ((py + 1) << vsub).min(y0 + h);
        let two_rows = gy1 - gy0 == 2;
        let row0 = (gy0 - y0 + ym0) * image.stride;
        let out_row = &mut out[(py - py0) * pw..(py - py0) * pw + pw];

        if left_partial {
            let cols = (px0 << 1).max(x0)..((px0 + 1) << 1).min(x0 + w);
            let mask_off = row0 + (cols.start - x0 + xm0);
            out_row[0] = edge_sum(image, mask_off, cols.len(), two_rows);
        }
        if ipx0 < ipx1 {
            let n = ipx1 - ipx0;
            let mask0 = row0 + ((ipx0 << 1) - x0 + xm0);
            let top = &image.bitmap[mask0..mask0 + 2 * n];
            let span = &mut out_row[ipx0 - px0..ipx0 - px0 + n];
            let bot =
                two_rows.then(|| &image.bitmap[mask0 + image.stride..mask0 + image.stride + 2 * n]);
            pair_sums(span, top, bot, allow_simd);
        }
        if right_partial && (px1 > px0 || !left_partial) {
            let cols = (px1 << 1).max(x0)..((px1 + 1) << 1).min(x0 + w);
            let mask_off = row0 + (cols.start - x0 + xm0);
            out_row[pw - 1] = edge_sum(image, mask_off, cols.len(), two_rows);
        }
    }
    Some(PooledRect {
        px0,
        py0,
        pw,
        ph,
        shift,
    })
}

/// Adjacent-pair sums of one interior span: `span[i] = top[2i] + top[2i+1]`
/// (+ the same pair from `bot` when present). Scalar sums stay u16 so LLVM
/// can use narrow lanes; the AVX2 kernel is bit-identical (max sum 1020,
/// no saturation anywhere).
fn pair_sums(span: &mut [u16], top: &[u8], bot: Option<&[u8]>, allow_simd: bool) {
    #[cfg(target_arch = "x86_64")]
    if allow_simd && avx2::enabled() {
        // SAFETY: `avx2::enabled()` just verified AVX2 support.
        unsafe {
            match bot {
                Some(bot) => avx2::pair_sums_two_rows(span, top, bot),
                None => avx2::pair_sums_one_row(span, top),
            }
        }
        return;
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = allow_simd;
    match bot {
        Some(bot) => {
            for ((sum, pair), bot_pair) in span
                .iter_mut()
                .zip(top.chunks_exact(2))
                .zip(bot.chunks_exact(2))
            {
                *sum = u16::from(pair[0])
                    + u16::from(pair[1])
                    + u16::from(bot_pair[0])
                    + u16::from(bot_pair[1]);
            }
        }
        None => {
            for (sum, pair) in span.iter_mut().zip(top.chunks_exact(2)) {
                *sum = u16::from(pair[0]) + u16::from(pair[1]);
            }
        }
    }
}

/// Blends one component plane from precomputed pooled sums: the apply half
/// of the two-phase pooled path. Same skip-block/branchless structure as the
/// other kernels, with the sums standing in for the mask. Tightly-packed
/// planes dispatch to the AVX2 apply kernels when available.
pub(crate) fn blend_pooled_from_sums(
    plane: &mut PlaneView<'_>,
    sums: &[u16],
    rect: PooledRect,
    src: u32,
    alpha: u32,
    sample: SampleFormat,
) {
    #[cfg(target_arch = "x86_64")]
    if avx2::enabled() {
        match (sample, plane.pixel_step) {
            (SampleFormat::U8, 1) => {
                // SAFETY: `avx2::enabled()` just verified AVX2 support.
                unsafe { avx2::apply_sums_u8(plane, sums, rect, src, alpha) };
                return;
            }
            (SampleFormat::U16Le, 2) => {
                // SAFETY: `avx2::enabled()` just verified AVX2 support.
                unsafe { avx2::apply_sums_u16(plane, sums, rect, src, alpha) };
                return;
            }
            _ => {}
        }
    }
    match sample {
        SampleFormat::U8 => apply_sums::<SampleU8>(plane, sums, rect, src, alpha),
        SampleFormat::U16Le => apply_sums::<SampleU16Le>(plane, sums, rect, src, alpha),
    }
}

/// Route choice for subsampled chroma pairs in `filter::blend_images`
/// (measured in `bench_kernels.rs`, dense 1080p chroma pair): pooling each
/// node once and applying per component beats the fused per-component path
/// for 8-bit always (scalar 372us vs 544us fused). For 16-bit the scalar
/// routes were a wash (487us vs 516us), so it kept the fused path — but
/// the AVX2 apply kernels put two-phase far ahead (125us vs 454us), so
/// 16-bit takes it exactly when the AVX2 kernels are live. Both routes are
/// byte-identical (lab parity tests).
pub(crate) fn two_phase_pooled_preferred(sample: SampleFormat) -> bool {
    match sample {
        SampleFormat::U8 => true,
        SampleFormat::U16Le => avx2_available(),
    }
}

fn apply_sums<S: Sample>(
    plane: &mut PlaneView<'_>,
    sums: &[u16],
    rect: PooledRect,
    src: u32,
    alpha: u32,
) {
    if alpha == 0 {
        return;
    }
    let step = plane.pixel_step;
    for row in 0..rect.ph {
        let sums_row = &sums[row * rect.pw..(row + 1) * rect.pw];
        let dst_start = (rect.py0 + row) * plane.linesize + rect.px0 * step;
        let dst_row = &mut plane.data[dst_start..dst_start + rect.pw * step];

        let mut dst_blocks = dst_row.chunks_exact_mut(S::SKIP_CHUNK * step);
        let mut sum_blocks = sums_row.chunks_exact(S::SKIP_CHUNK);
        for (dst_block, sum_block) in (&mut dst_blocks).zip(&mut sum_blocks) {
            if sum_block.iter().all(|&sum| sum == 0) {
                continue;
            }
            apply_sums_span::<S>(dst_block, sum_block, src, alpha, rect.shift, step);
        }
        let tail_dst = dst_blocks.into_remainder();
        let tail_sums = sum_blocks.remainder();
        if !tail_sums.iter().all(|&sum| sum == 0) {
            apply_sums_span::<S>(tail_dst, tail_sums, src, alpha, rect.shift, step);
        }
    }
}

fn apply_sums_span<S: Sample>(
    dst: &mut [u8],
    sums: &[u16],
    src: u32,
    alpha: u32,
    shift: u32,
    step: usize,
) {
    for (chunk, &sum) in dst.chunks_exact_mut(step).zip(sums) {
        let alpha_total = (u32::from(sum) >> shift) * alpha;
        let sample = &mut chunk[..S::BYTES];
        let blended = S::blend(S::load(sample), src, alpha_total);
        S::store(sample, blended);
    }
}

/// General subsampled path (reference implementation, kept for any future
/// `(hsub, vsub)` outside the h2 fast path): the mask is summed over each
/// output pixel's `(1<<hsub) x (1<<vsub)` block (edge blocks sum fewer
/// samples — FFmpeg zero-pad semantics) and truncated by `>> (hsub+vsub)`.
#[allow(clippy::too_many_arguments)]
fn blend_pooled<S: Sample>(
    plane: &mut PlaneView<'_>,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    (x0, y0): (usize, usize),
    (xm0, ym0): (usize, usize),
    (w, h): (usize, usize),
    (hsub, vsub): (u32, u32),
) {
    let shift = hsub + vsub;
    // First/last plane pixels touched by the clipped mask rectangle.
    let px0 = x0 >> hsub;
    let px1 = (x0 + w - 1) >> hsub;
    let py0 = y0 >> vsub;
    let py1 = (y0 + h - 1) >> vsub;

    for py in py0..=py1 {
        // Grid rows covered by this plane row, clipped to the mask rect.
        let gy0 = (py << vsub).max(y0);
        let gy1 = ((py + 1) << vsub).min(y0 + h);
        let dst_row = py * plane.linesize;
        for px in px0..=px1 {
            let gx0 = (px << hsub).max(x0);
            let gx1 = ((px + 1) << hsub).min(x0 + w);

            let mut sum: u32 = 0;
            for gy in gy0..gy1 {
                let row = (gy - y0 + ym0) * image.stride + (gx0 - x0 + xm0);
                for &mask in &image.bitmap[row..row + (gx1 - gx0)] {
                    sum += u32::from(mask);
                }
            }

            let a = (sum >> shift) * alpha;
            if a == 0 {
                continue;
            }
            let index = dst_row + px * plane.pixel_step;
            let sample = &mut plane.data[index..index + S::BYTES];
            let blended = S::blend(S::load(sample), src, a);
            S::store(sample, blended);
        }
    }
}

/// Clips `[dst, dst+len)` against `[0, limit)`.
/// Returns (clipped start, offset into the mask, clipped length).
fn clip_interval(limit: usize, dst: i32, len: usize) -> Option<(usize, usize, usize)> {
    let start = i64::from(dst);
    let end = start + len as i64;
    let clipped_start = start.max(0);
    let clipped_end = end.min(limit as i64);
    if clipped_start >= clipped_end {
        return None;
    }
    Some((
        clipped_start as usize,
        (clipped_start - start) as usize,
        (clipped_end - clipped_start) as usize,
    ))
}

/// Candidate kernels for the ignored micro-benchmark (`bench_kernels.rs`).
/// Test-only: the winning shape gets folded into the shipping kernels above
/// and losing variants are deleted, so nothing here reaches release builds.
#[cfg(test)]
pub(crate) mod lab {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum DirectKernel {
        /// The pre-optimization kernel: per-pixel `mask != 0` skip.
        PerPixelSkip,
        /// No skip at all; `mask == 0` blends to the exact identity, and the
        /// straight-line body is autovectorization-friendly.
        Branchless,
        /// Zero-test the mask 8 pixels at a time (one u64), branchless body.
        ChunkSkip8,
        /// Zero-test the mask 16 pixels at a time (two u64s), branchless body.
        ChunkSkip16,
        /// The scalar shipping kernel (`blend_direct`, `S::SKIP_CHUNK`
        /// blocks) — the AVX2 fallback and parity reference.
        Shipping,
        /// The runtime-dispatched AVX2 kernels (falls back to `Shipping`
        /// when unavailable or the plane is not tightly packed).
        Avx2,
    }

    pub(crate) const ALL_DIRECT: [DirectKernel; 6] = [
        DirectKernel::PerPixelSkip,
        DirectKernel::Branchless,
        DirectKernel::ChunkSkip8,
        DirectKernel::ChunkSkip16,
        DirectKernel::Shipping,
        DirectKernel::Avx2,
    ];

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum PooledKernel {
        /// The general per-pixel interval-clamping implementation
        /// (`blend_pooled`, now the exotic-subsampling fallback).
        Reference,
        /// The shipping dispatch (h2 fast path for hsub == 1).
        Shipping,
    }

    /// Same contract as [`blend_component`] restricted to subsampled planes,
    /// with a selectable pooled kernel.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn blend_pooled_variant(
        kernel: PooledKernel,
        plane: &mut PlaneView<'_>,
        grid_w: usize,
        grid_h: usize,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        hsub: u32,
        vsub: u32,
        sample: SampleFormat,
    ) {
        match kernel {
            PooledKernel::Shipping => {
                blend_component(plane, grid_w, grid_h, image, src, alpha, hsub, vsub, sample)
            }
            PooledKernel::Reference => match sample {
                SampleFormat::U8 => dispatch_pooled_reference::<SampleU8>(
                    plane, grid_w, grid_h, image, src, alpha, hsub, vsub,
                ),
                SampleFormat::U16Le => dispatch_pooled_reference::<SampleU16Le>(
                    plane, grid_w, grid_h, image, src, alpha, hsub, vsub,
                ),
            },
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_pooled_reference<S: Sample>(
        plane: &mut PlaneView<'_>,
        grid_w: usize,
        grid_h: usize,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        hsub: u32,
        vsub: u32,
    ) {
        if alpha == 0 || image.w == 0 || image.h == 0 {
            return;
        }
        let Some((x0, xm0, w)) = clip_interval(grid_w, image.dst_x, image.w) else {
            return;
        };
        let Some((y0, ym0, h)) = clip_interval(grid_h, image.dst_y, image.h) else {
            return;
        };
        blend_pooled::<S>(
            plane,
            image,
            src,
            alpha,
            (x0, y0),
            (xm0, ym0),
            (w, h),
            (hsub, vsub),
        );
    }

    /// Same contract as [`blend_component`] restricted to hsub == vsub == 0,
    /// with a selectable row kernel.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn blend_direct_variant(
        kernel: DirectKernel,
        plane: &mut PlaneView<'_>,
        grid_w: usize,
        grid_h: usize,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        sample: SampleFormat,
    ) {
        match sample {
            SampleFormat::U8 => {
                dispatch::<SampleU8>(kernel, plane, grid_w, grid_h, image, src, alpha)
            }
            SampleFormat::U16Le => {
                dispatch::<SampleU16Le>(kernel, plane, grid_w, grid_h, image, src, alpha)
            }
        }
    }

    fn dispatch<S: Sample>(
        kernel: DirectKernel,
        plane: &mut PlaneView<'_>,
        grid_w: usize,
        grid_h: usize,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
    ) {
        if alpha == 0 || image.w == 0 || image.h == 0 {
            return;
        }
        let Some((x0, xm0, w)) = clip_interval(grid_w, image.dst_x, image.w) else {
            return;
        };
        let Some((y0, ym0, h)) = clip_interval(grid_h, image.dst_y, image.h) else {
            return;
        };
        let coords = ((x0, y0), (xm0, ym0), (w, h));
        match kernel {
            DirectKernel::PerPixelSkip => per_pixel_skip::<S>(plane, image, src, alpha, coords),
            DirectKernel::Branchless => branchless::<S>(plane, image, src, alpha, coords),
            DirectKernel::ChunkSkip8 => chunked::<S, 8>(plane, image, src, alpha, coords),
            DirectKernel::ChunkSkip16 => chunked::<S, 16>(plane, image, src, alpha, coords),
            DirectKernel::Shipping => {
                blend_direct::<S>(plane, image, src, alpha, coords.0, coords.1, coords.2)
            }
            DirectKernel::Avx2 => {
                // Mirror of the shipping dispatch in `blend_typed`.
                #[cfg(target_arch = "x86_64")]
                if plane.pixel_step == S::BYTES && avx2::enabled() {
                    if S::BYTES == SampleU8::BYTES {
                        // SAFETY: `avx2::enabled()` verified AVX2 support.
                        unsafe {
                            avx2::blend_direct_u8(
                                plane, image, src, alpha, coords.0, coords.1, coords.2,
                            )
                        };
                        return;
                    }
                    if S::BYTES == SampleU16Le::BYTES {
                        // SAFETY: `avx2::enabled()` verified AVX2 support.
                        unsafe {
                            avx2::blend_direct_u16(
                                plane, image, src, alpha, coords.0, coords.1, coords.2,
                            )
                        };
                        return;
                    }
                }
                blend_direct::<S>(plane, image, src, alpha, coords.0, coords.1, coords.2);
            }
        }
    }

    /// Scalar-forced [`pool_sums_h2`]: the bench/test reference for the
    /// AVX2 interior pair-sum kernel.
    pub(crate) fn pool_sums_h2_scalar(
        grid_w: usize,
        grid_h: usize,
        image: &OverlayImage<'_>,
        vsub: u32,
        out: &mut Vec<u16>,
    ) -> Option<PooledRect> {
        pool_sums_h2_impl(grid_w, grid_h, image, vsub, out, false)
    }

    /// Scalar-forced [`blend_pooled_from_sums`]: the bench/test reference
    /// for the AVX2 apply kernels.
    pub(crate) fn blend_pooled_from_sums_scalar(
        plane: &mut PlaneView<'_>,
        sums: &[u16],
        rect: PooledRect,
        src: u32,
        alpha: u32,
        sample: SampleFormat,
    ) {
        match sample {
            SampleFormat::U8 => apply_sums::<SampleU8>(plane, sums, rect, src, alpha),
            SampleFormat::U16Le => apply_sums::<SampleU16Le>(plane, sums, rect, src, alpha),
        }
    }

    type Coords = ((usize, usize), (usize, usize), (usize, usize));

    /// Copy of the pre-optimization shipping kernel (kept as the bench
    /// baseline).
    fn per_pixel_skip<S: Sample>(
        plane: &mut PlaneView<'_>,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        ((x0, y0), (xm0, ym0), (w, h)): Coords,
    ) {
        let step = plane.pixel_step;
        for row in 0..h {
            let mask_start = (ym0 + row) * image.stride + xm0;
            let mask_row = &image.bitmap[mask_start..mask_start + w];
            let dst_start = (y0 + row) * plane.linesize + x0 * step;
            let dst_row = &mut plane.data[dst_start..dst_start + w * step];
            for (chunk, &mask) in dst_row.chunks_exact_mut(step).zip(mask_row) {
                if mask != 0 {
                    let sample = &mut chunk[..S::BYTES];
                    let blended = S::blend(S::load(sample), src, u32::from(mask) * alpha);
                    S::store(sample, blended);
                }
            }
        }
    }

    fn branchless<S: Sample>(
        plane: &mut PlaneView<'_>,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        ((x0, y0), (xm0, ym0), (w, h)): Coords,
    ) {
        let step = plane.pixel_step;
        for row in 0..h {
            let mask_start = (ym0 + row) * image.stride + xm0;
            let mask_row = &image.bitmap[mask_start..mask_start + w];
            let dst_start = (y0 + row) * plane.linesize + x0 * step;
            let dst_row = &mut plane.data[dst_start..dst_start + w * step];
            for (chunk, &mask) in dst_row.chunks_exact_mut(step).zip(mask_row) {
                let sample = &mut chunk[..S::BYTES];
                let blended = S::blend(S::load(sample), src, u32::from(mask) * alpha);
                S::store(sample, blended);
            }
        }
    }

    fn chunked<S: Sample, const CHUNK: usize>(
        plane: &mut PlaneView<'_>,
        image: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        ((x0, y0), (xm0, ym0), (w, h)): Coords,
    ) {
        let step = plane.pixel_step;
        for row in 0..h {
            let mask_start = (ym0 + row) * image.stride + xm0;
            let mask_row = &image.bitmap[mask_start..mask_start + w];
            let dst_start = (y0 + row) * plane.linesize + x0 * step;
            let dst_row = &mut plane.data[dst_start..dst_start + w * step];

            let mut dst_chunks = dst_row.chunks_exact_mut(CHUNK * step);
            let mut mask_chunks = mask_row.chunks_exact(CHUNK);
            for (dst_chunk, mask_chunk) in (&mut dst_chunks).zip(&mut mask_chunks) {
                if mask_is_zero(mask_chunk) {
                    continue;
                }
                for (chunk, &mask) in dst_chunk.chunks_exact_mut(step).zip(mask_chunk) {
                    let sample = &mut chunk[..S::BYTES];
                    let blended = S::blend(S::load(sample), src, u32::from(mask) * alpha);
                    S::store(sample, blended);
                }
            }
            let tail_dst = dst_chunks.into_remainder();
            let tail_mask = mask_chunks.remainder();
            for (chunk, &mask) in tail_dst.chunks_exact_mut(step).zip(tail_mask) {
                if mask != 0 {
                    let sample = &mut chunk[..S::BYTES];
                    let blended = S::blend(S::load(sample), src, u32::from(mask) * alpha);
                    S::store(sample, blended);
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Deterministic pseudo-random mask with glyph-like structure: runs of
        /// zeros, gradient ramps, and solid spans.
        fn structured_mask(len: usize, seed: u64) -> Vec<u8> {
            let mut state = seed | 1;
            let mut out = Vec::with_capacity(len);
            while out.len() < len {
                // xorshift64
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let run = 1 + (state as usize % 23);
                let kind = (state >> 32) % 3;
                for i in 0..run.min(len - out.len()) {
                    out.push(match kind {
                        0 => 0,
                        1 => 255,
                        _ => ((i * 37) % 256) as u8,
                    });
                }
            }
            out
        }

        /// Every variant must produce byte-identical output to the shipping
        /// kernel for both sample widths and all interleave steps.
        #[test]
        fn variants_match_shipping_kernel_exactly() {
            let (w, h, stride) = (61usize, 13usize, 67usize);
            let bitmap = structured_mask(stride * (h - 1) + w, 0x00C0_FFEE);
            let image = OverlayImage {
                w,
                h,
                stride,
                bitmap: &bitmap,
                color: 0xFFFFFF00,
                dst_x: -3,
                dst_y: 2,
            };
            for (sample, step, src) in [
                (SampleFormat::U8, 1usize, 235u32),
                (SampleFormat::U8, 3, 235),
                (SampleFormat::U16Le, 2, 943),
                (SampleFormat::U16Le, 4, 60395),
            ] {
                let (grid_w, grid_h) = (55usize, 17usize);
                let linesize = grid_w * step + 5;
                let base = structured_mask(linesize * grid_h, 0xBEEF ^ step as u64);
                let alpha = sample.alpha_fixed(200);
                let mut expected = base.clone();
                blend_component(
                    &mut PlaneView {
                        data: &mut expected,
                        linesize,
                        pixel_step: step,
                    },
                    grid_w,
                    grid_h,
                    &image,
                    src,
                    alpha,
                    0,
                    0,
                    sample,
                );
                for kernel in ALL_DIRECT {
                    let mut got = base.clone();
                    blend_direct_variant(
                        kernel,
                        &mut PlaneView {
                            data: &mut got,
                            linesize,
                            pixel_step: step,
                        },
                        grid_w,
                        grid_h,
                        &image,
                        src,
                        alpha,
                        sample,
                    );
                    assert_eq!(got, expected, "{kernel:?} sample={sample:?} step={step}");
                }
            }
        }

        /// The h2 pooled fast path must be byte-identical to the general
        /// reference across subsampling modes, sample widths, interleave
        /// steps, and awkward clip geometry (odd offsets, partial edge
        /// blocks, single pixels, negative placement).
        #[test]
        fn pooled_h2_matches_general_reference() {
            for (sample, step, src) in [
                (SampleFormat::U8, 1usize, 128u32),
                (SampleFormat::U8, 2, 128), // NV12-style interleave
                (SampleFormat::U16Le, 2, 514),
                (SampleFormat::U16Le, 4, 33000), // P010-style interleave
            ] {
                for (hsub, vsub) in [(1u32, 0u32), (1, 1)] {
                    for (dst_x, dst_y) in [(-3i32, -2i32), (0, 0), (7, 5), (2, 1)] {
                        for (w, h) in [(61usize, 13usize), (64, 16), (1, 1), (2, 3), (17, 1)] {
                            let stride = w + 6;
                            let bitmap =
                                structured_mask(stride * (h - 1) + w, 0xABCD ^ (w * h) as u64);
                            let image = OverlayImage {
                                w,
                                h,
                                stride,
                                bitmap: &bitmap,
                                color: 0xFFFFFF00,
                                dst_x,
                                dst_y,
                            };
                            let (grid_w, grid_h) = (49usize, 23usize);
                            let plane_w = (grid_w + (1 << hsub) - 1) >> hsub;
                            let plane_h = (grid_h + (1 << vsub) - 1) >> vsub;
                            let linesize = plane_w * step + 3;
                            let base = structured_mask(linesize * plane_h, 0x5EED ^ w as u64);
                            let alpha = sample.alpha_fixed(200);
                            let case = format!(
                                "sample={sample:?} step={step} sub=({hsub},{vsub}) \
                                 dst=({dst_x},{dst_y}) size=({w},{h})"
                            );

                            let mut expected = base.clone();
                            blend_pooled_variant(
                                PooledKernel::Reference,
                                &mut PlaneView {
                                    data: &mut expected,
                                    linesize,
                                    pixel_step: step,
                                },
                                grid_w,
                                grid_h,
                                &image,
                                src,
                                alpha,
                                hsub,
                                vsub,
                                sample,
                            );
                            let mut got = base.clone();
                            blend_pooled_variant(
                                PooledKernel::Shipping,
                                &mut PlaneView {
                                    data: &mut got,
                                    linesize,
                                    pixel_step: step,
                                },
                                grid_w,
                                grid_h,
                                &image,
                                src,
                                alpha,
                                hsub,
                                vsub,
                                sample,
                            );
                            assert_eq!(got, expected, "{case}");
                        }
                    }
                }
            }
        }

        /// The two-phase pooled route (pool once, apply per component) must
        /// be byte-identical to sequential per-component `blend_component`
        /// calls, for planar (two separate planes) and interleaved
        /// (NV12/P010-style, one plane with byte offsets) chroma layouts.
        #[test]
        fn two_phase_pooled_matches_component_blend() {
            let geometries = [(-3i32, -2i32), (0, 0), (7, 5), (2, 1)];
            let sizes = [(61usize, 13usize), (64, 16), (1, 1), (2, 3), (17, 1)];
            let (grid_w, grid_h) = (49usize, 23usize);
            let mut sums = Vec::new();

            for (hsub, vsub) in [(1u32, 0u32), (1, 1)] {
                let plane_w = (grid_w + 1) >> 1;
                let plane_h = (grid_h + (1 << vsub) - 1) >> vsub;
                for (dst_x, dst_y) in geometries {
                    for (w, h) in sizes {
                        let stride = w + 6;
                        let bitmap = structured_mask(stride * (h - 1) + w, 0x7A57 ^ (w * h) as u64);
                        let image = OverlayImage {
                            w,
                            h,
                            stride,
                            bitmap: &bitmap,
                            color: 0xFFFFFF00,
                            dst_x,
                            dst_y,
                        };
                        let alpha = SampleFormat::U8.alpha_fixed(200);
                        let case =
                            format!("sub=({hsub},{vsub}) dst=({dst_x},{dst_y}) size=({w},{h})");

                        // Planar layout: two separate u8 planes.
                        let linesize = plane_w + 3;
                        let base = structured_mask(linesize * plane_h, 0x1234 ^ w as u64);
                        let (mut expected_u, mut expected_v) = (base.clone(), base.clone());
                        for (data, src) in [(&mut expected_u, 100u32), (&mut expected_v, 200u32)] {
                            blend_component(
                                &mut PlaneView {
                                    data,
                                    linesize,
                                    pixel_step: 1,
                                },
                                grid_w,
                                grid_h,
                                &image,
                                src,
                                alpha,
                                hsub,
                                vsub,
                                SampleFormat::U8,
                            );
                        }
                        let (mut got_u, mut got_v) = (base.clone(), base.clone());
                        let rect = pool_sums_h2(grid_w, grid_h, &image, vsub, &mut sums);
                        if let Some(rect) = rect {
                            for (data, src) in [(&mut got_u, 100u32), (&mut got_v, 200u32)] {
                                blend_pooled_from_sums(
                                    &mut PlaneView {
                                        data,
                                        linesize,
                                        pixel_step: 1,
                                    },
                                    &sums,
                                    rect,
                                    src,
                                    alpha,
                                    SampleFormat::U8,
                                );
                            }
                        }
                        assert_eq!(got_u, expected_u, "planar U {case}");
                        assert_eq!(got_v, expected_v, "planar V {case}");

                        // Interleaved layout: P010-style one plane, u16
                        // samples at byte offsets 0 and 2, pixel_step 4.
                        let linesize16 = plane_w * 4 + 5;
                        let base16 = structured_mask(linesize16 * plane_h, 0x9876 ^ h as u64);
                        let alpha16 = SampleFormat::U16Le.alpha_fixed(200);
                        let mut expected = base16.clone();
                        for (offset, src) in [(0usize, 514u32), (2, 33000)] {
                            blend_component(
                                &mut PlaneView {
                                    data: &mut expected[offset..],
                                    linesize: linesize16,
                                    pixel_step: 4,
                                },
                                grid_w,
                                grid_h,
                                &image,
                                src,
                                alpha16,
                                hsub,
                                vsub,
                                SampleFormat::U16Le,
                            );
                        }
                        let mut got = base16.clone();
                        let rect = pool_sums_h2(grid_w, grid_h, &image, vsub, &mut sums);
                        if let Some(rect) = rect {
                            for (offset, src) in [(0usize, 514u32), (2, 33000)] {
                                blend_pooled_from_sums(
                                    &mut PlaneView {
                                        data: &mut got[offset..],
                                        linesize: linesize16,
                                        pixel_step: 4,
                                    },
                                    &sums,
                                    rect,
                                    src,
                                    alpha16,
                                    SampleFormat::U16Le,
                                );
                            }
                        }
                        assert_eq!(got, expected, "interleaved {case}");
                    }
                }
            }
        }

        /// The shipping two-phase dispatchers (AVX2 on capable CPUs) must
        /// match their scalar-forced twins exactly: same rects, same sums,
        /// same blended bytes, at both sample widths and awkward clip
        /// geometry.
        #[test]
        fn simd_dispatch_matches_scalar_two_phase() {
            let (grid_w, grid_h) = (49usize, 23usize);
            let mut sums_auto = Vec::new();
            let mut sums_scalar = Vec::new();
            for vsub in [0u32, 1] {
                for (dst_x, dst_y) in [(-3i32, -2i32), (0, 0), (7, 5)] {
                    for (w, h) in [(61usize, 13usize), (64, 16), (2, 3), (33, 1)] {
                        let stride = w + 6;
                        let bitmap = structured_mask(stride * (h - 1) + w, 0xD15C ^ (w * h) as u64);
                        let image = OverlayImage {
                            w,
                            h,
                            stride,
                            bitmap: &bitmap,
                            color: 0xFFFFFF00,
                            dst_x,
                            dst_y,
                        };
                        let case = format!("vsub={vsub} dst=({dst_x},{dst_y}) size=({w},{h})");
                        let rect = pool_sums_h2(grid_w, grid_h, &image, vsub, &mut sums_auto);
                        let rect_scalar =
                            pool_sums_h2_scalar(grid_w, grid_h, &image, vsub, &mut sums_scalar);
                        assert_eq!(rect, rect_scalar, "{case}");
                        let Some(rect) = rect else { continue };
                        let used = rect.pw * rect.ph;
                        assert_eq!(sums_auto[..used], sums_scalar[..used], "{case}");

                        let plane_w = (grid_w + 1) >> 1;
                        let plane_h = (grid_h + (1 << vsub) - 1) >> vsub;
                        for (sample, step, src) in [
                            (SampleFormat::U8, 1usize, 200u32),
                            (SampleFormat::U16Le, 2, 943),
                        ] {
                            let linesize = plane_w * step + 3;
                            let base = structured_mask(linesize * plane_h, 0xFACE ^ w as u64);
                            let alpha = sample.alpha_fixed(200);
                            let mut expected = base.clone();
                            blend_pooled_from_sums_scalar(
                                &mut PlaneView {
                                    data: &mut expected,
                                    linesize,
                                    pixel_step: step,
                                },
                                &sums_auto,
                                rect,
                                src,
                                alpha,
                                sample,
                            );
                            let mut got = base.clone();
                            blend_pooled_from_sums(
                                &mut PlaneView {
                                    data: &mut got,
                                    linesize,
                                    pixel_step: step,
                                },
                                &sums_auto,
                                rect,
                                src,
                                alpha,
                                sample,
                            );
                            assert_eq!(got, expected, "{case} sample={sample:?}");
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full_mask(w: usize, h: usize, value: u8) -> Vec<u8> {
        vec![value; w * h]
    }

    fn image<'a>(
        bitmap: &'a [u8],
        w: usize,
        h: usize,
        stride: usize,
        color: u32,
    ) -> OverlayImage<'a> {
        OverlayImage {
            w,
            h,
            stride,
            bitmap,
            color,
            dst_x: 0,
            dst_y: 0,
        }
    }

    fn plane_of(data: &mut [u8], linesize: usize) -> PlaneView<'_> {
        PlaneView {
            data,
            linesize,
            pixel_step: 1,
        }
    }

    fn blend_u8(
        plane: &mut PlaneView<'_>,
        grid: (usize, usize),
        img: &OverlayImage<'_>,
        src: u32,
        alpha: u32,
        subs: (u32, u32),
    ) {
        blend_component(
            plane,
            grid.0,
            grid.1,
            img,
            src,
            alpha,
            subs.0,
            subs.1,
            SampleFormat::U8,
        );
    }

    #[test]
    fn yuv_components_reference_triplets_8bit() {
        // Textbook 8-bit values (identical to the pre-high-depth implementation).
        assert_eq!(
            yuv_components([255, 255, 255], ColorMatrix::Bt601, ColorRange::Limited, 8),
            [235, 128, 128]
        );
        assert_eq!(
            yuv_components([0, 0, 0], ColorMatrix::Bt601, ColorRange::Limited, 8),
            [16, 128, 128]
        );
        assert_eq!(
            yuv_components([255, 0, 0], ColorMatrix::Bt601, ColorRange::Limited, 8),
            [81, 90, 240]
        );
        assert_eq!(
            yuv_components([255, 0, 0], ColorMatrix::Bt709, ColorRange::Limited, 8),
            [63, 102, 240]
        );
        assert_eq!(
            yuv_components([255, 255, 255], ColorMatrix::Bt709, ColorRange::Full, 8),
            [255, 128, 128]
        );
    }

    /// ff_draw_color scales by (2^bits - 1)/255, so 10-bit limited white is
    /// 943 (not the broadcast 940) and neutral chroma is 514 (not 512) —
    /// parity with FFmpeg, verified by hand from the drawutils formula.
    #[test]
    fn yuv_components_high_depth_matches_ffmpeg_stretch() {
        assert_eq!(
            yuv_components([255, 255, 255], ColorMatrix::Bt601, ColorRange::Limited, 10),
            [943, 514, 514]
        );
        assert_eq!(
            yuv_components([255, 0, 0], ColorMatrix::Bt601, ColorRange::Limited, 10),
            [327, 362, 963]
        );
        // P010: components scale to the full 16-bit container (depth+shift);
        // 65535/255 == 257 exactly, so white luma is 235*257 = 60395.
        assert_eq!(
            yuv_components([255, 255, 255], ColorMatrix::Bt601, ColorRange::Limited, 16)[0],
            60395
        );
        // RGB identity at 8 bit (full range), limited-range compression like
        // ff_draw_color (chroma=false for every RGB component).
        assert_eq!(
            rgb_components([12, 200, 255], ColorRange::Full, 8),
            [12, 200, 255]
        );
        assert_eq!(
            rgb_components([0, 128, 255], ColorRange::Limited, 8),
            [16, 126, 235]
        );
    }

    #[test]
    fn alpha_fixed_endpoints() {
        assert_eq!(SampleFormat::U8.alpha_fixed(0), 0);
        assert_eq!(SampleFormat::U8.alpha_fixed(255), 0x10203);
        assert_eq!(SampleFormat::U16Le.alpha_fixed(0), 0);
        assert_eq!(SampleFormat::U16Le.alpha_fixed(255), 0x100);
    }

    /// Fully-opaque full-coverage blend replaces dst with src (the FFmpeg
    /// fixed-point form is exact to the LSB here).
    #[test]
    fn opaque_blend_replaces_destination() {
        let bitmap = full_mask(4, 4, 255);
        let img = image(&bitmap, 4, 4, 4, 0xFFFFFF00); // white, opaque
        for dst_value in [0u8, 100, 255] {
            let mut data = vec![dst_value; 16];
            blend_u8(
                &mut plane_of(&mut data, 4),
                (4, 4),
                &img,
                200,
                SampleFormat::U8.alpha_fixed(255),
                (0, 0),
            );
            assert!(
                data.iter().all(|&d| d == 200),
                "dst {dst_value} -> {data:?}"
            );
        }
    }

    #[test]
    fn zero_opacity_is_a_no_op() {
        let bitmap = full_mask(4, 4, 255);
        let img = image(&bitmap, 4, 4, 4, 0xFFFF_FFFF); // alpha 255 = transparent
        let mut data = vec![7u8; 16];
        blend_u8(
            &mut plane_of(&mut data, 4),
            (4, 4),
            &img,
            200,
            SampleFormat::U8.alpha_fixed(img.opacity()),
            (0, 0),
        );
        assert!(data.iter().all(|&d| d == 7));
    }

    /// opacity=128 over mask=255 lands within 1 LSB of the exact midpoint.
    #[test]
    fn half_opacity_blends_midway() {
        let bitmap = full_mask(1, 1, 255);
        let img = image(&bitmap, 1, 1, 1, 0xFFFFFF00);
        let mut data = vec![0u8];
        blend_u8(
            &mut plane_of(&mut data, 1),
            (1, 1),
            &img,
            200,
            SampleFormat::U8.alpha_fixed(128),
            (0, 0),
        );
        let expected: i32 = 200 * 128 / 255; // ≈ 100
        assert!(
            (i32::from(data[0]) - expected).abs() <= 1,
            "got {}, expected ~{expected}",
            data[0]
        );
    }

    /// 2x2 pooling: a full block gives full alpha; a block only half covered
    /// by the mask (image narrower than the block) contributes half — FFmpeg
    /// zero-pad semantics, not edge clamping.
    #[test]
    fn chroma_pooling_full_and_partial_blocks() {
        // Image 1 luma pixel wide, 2 tall at origin; chroma plane 1x1 (hsub=vsub=1).
        let bitmap = full_mask(1, 2, 255);
        let img = image(&bitmap, 1, 2, 1, 0xFFFFFF00);
        let mut chroma = vec![0u8];
        blend_u8(
            &mut plane_of(&mut chroma, 1),
            (2, 2),
            &img,
            200,
            SampleFormat::U8.alpha_fixed(255),
            (1, 1),
        );
        // sum = 255*2, >>2 = 127 -> a = 127*0x10203; expected ≈ 200*127/255 ≈ 99.6
        let a = 127u64 * 0x10203;
        let dst = 0u64;
        let expected = ((0x0101_0101 - a) * dst + a * 200) >> 24;
        assert_eq!(u64::from(chroma[0]), expected);
        assert!((99..=100).contains(&chroma[0]), "got {}", chroma[0]);

        // Full 2x2 coverage for comparison: alpha ~255 -> ~src.
        let bitmap = full_mask(2, 2, 255);
        let img = image(&bitmap, 2, 2, 2, 0xFFFFFF00);
        let mut chroma = vec![0u8];
        blend_u8(
            &mut plane_of(&mut chroma, 1),
            (2, 2),
            &img,
            200,
            SampleFormat::U8.alpha_fixed(255),
            (1, 1),
        );
        assert_eq!(chroma[0], 200);
    }

    /// The last bitmap row may be allocated short of the stride; blending must
    /// not read past `stride*(h-1)+w`.
    #[test]
    fn short_last_row_is_never_overread() {
        let (w, h, stride) = (3usize, 3usize, 8usize);
        let bitmap = vec![255u8; stride * (h - 1) + w]; // exactly the minimum
        let img = image(&bitmap, w, h, stride, 0xFFFFFF00);
        let mut data = vec![0u8; 9];
        // Would panic on slice OOB if any index exceeded the minimum length.
        blend_u8(
            &mut plane_of(&mut data, 3),
            (3, 3),
            &img,
            255,
            SampleFormat::U8.alpha_fixed(255),
            (0, 0),
        );
        assert!(data.iter().all(|&d| d == 255));
    }

    /// Images are composited sequentially: the second blend reads the result
    /// of the first.
    #[test]
    fn sequential_compositing_order_matters() {
        let bitmap = full_mask(1, 1, 255);
        let opaque_a = image(&bitmap, 1, 1, 1, 0xFFFFFF00);
        let half_b = image(&bitmap, 1, 1, 1, 0xFFFFFF00);
        let mut data = vec![0u8];
        let mut plane = plane_of(&mut data, 1);
        blend_u8(
            &mut plane,
            (1, 1),
            &opaque_a,
            240,
            SampleFormat::U8.alpha_fixed(255),
            (0, 0),
        );
        blend_u8(
            &mut plane,
            (1, 1),
            &half_b,
            0,
            SampleFormat::U8.alpha_fixed(128),
            (0, 0),
        );
        // 240 -> then 50% toward 0 ≈ 119/120.
        assert!(
            (119..=121).contains(&data[0]),
            "expected ~120, got {}",
            data[0]
        );
    }

    /// Negative placement and overhang are clipped; pixels outside the
    /// clipped rect stay untouched.
    #[test]
    fn clipping_touches_only_in_frame_pixels() {
        let bitmap = full_mask(4, 4, 255);
        let mut img = image(&bitmap, 4, 4, 4, 0xFFFFFF00);
        img.dst_x = -2;
        img.dst_y = 2;
        let mut data = vec![9u8; 16]; // 4x4 frame
        blend_u8(
            &mut plane_of(&mut data, 4),
            (4, 4),
            &img,
            255,
            SampleFormat::U8.alpha_fixed(255),
            (0, 0),
        );
        // Rows 0-1 untouched; rows 2-3 columns 0-1 blended, columns 2-3 untouched.
        for y in 0..2 {
            for x in 0..4 {
                assert_eq!(data[y * 4 + x], 9, "({x},{y})");
            }
        }
        for y in 2..4 {
            for x in 0..2 {
                assert_eq!(data[y * 4 + x], 255, "({x},{y})");
            }
            for x in 2..4 {
                assert_eq!(data[y * 4 + x], 9, "({x},{y})");
            }
        }
        // Entirely off-frame image: no-op, no panic.
        img.dst_x = 100;
        let mut untouched = vec![9u8; 16];
        blend_u8(
            &mut plane_of(&mut untouched, 4),
            (4, 4),
            &img,
            255,
            SampleFormat::U8.alpha_fixed(255),
            (0, 0),
        );
        assert_eq!(untouched, vec![9u8; 16]);
    }

    /// pixel_step models interleaved components (NV12 chroma / packed RGB):
    /// only every step-th byte is written.
    #[test]
    fn pixel_step_writes_interleaved_component_only() {
        let bitmap = full_mask(2, 1, 255);
        let img = image(&bitmap, 2, 1, 2, 0xFFFFFF00);
        let mut data = vec![10u8; 4]; // two interleaved components, 2 samples
        let mut plane = PlaneView {
            data: &mut data,
            linesize: 4,
            pixel_step: 2,
        };
        blend_u8(
            &mut plane,
            (2, 1),
            &img,
            200,
            SampleFormat::U8.alpha_fixed(255),
            (0, 0),
        );
        assert_eq!(data, vec![200, 10, 200, 10]);
    }

    /// 16-bit blend: FFmpeg's formula with the exact residual it implies —
    /// max alpha is 255*0x100 = 65280, so an "opaque" blend keeps 257/65537
    /// of dst: src 943 over 0 lands on 939 (identical to vf_ass at 10 bit).
    #[test]
    fn high_depth_blend_matches_ffmpeg_formula() {
        let bitmap = full_mask(2, 1, 255);
        let img = image(&bitmap, 2, 1, 2, 0xFFFFFF00);
        let mut data = vec![0u8; 4]; // two u16le samples
        let mut plane = PlaneView {
            data: &mut data,
            linesize: 4,
            pixel_step: 2,
        };
        blend_component(
            &mut plane,
            2,
            1,
            &img,
            943,
            SampleFormat::U16Le.alpha_fixed(255),
            0,
            0,
            SampleFormat::U16Le,
        );
        let sample0 = u16::from_le_bytes([data[0], data[1]]);
        let sample1 = u16::from_le_bytes([data[2], data[3]]);
        let dst = 0u64;
        let expected = ((0x0001_0001u64 - 65280) * dst + 65280 * 943) >> 16;
        assert_eq!(u64::from(sample0), expected);
        assert_eq!(sample0, 939);
        assert_eq!(sample0, sample1);
    }

    /// P010-style interleaved u16 chroma: step 4 bytes, offset handled by the
    /// caller slicing; only the addressed component changes.
    #[test]
    fn high_depth_interleaved_step_writes_one_component() {
        let bitmap = full_mask(1, 1, 255);
        let img = image(&bitmap, 1, 1, 1, 0xFFFFFF00);
        let mut data = vec![0u8; 4]; // one U+V u16le pair
        let mut plane = PlaneView {
            data: &mut data,
            linesize: 4,
            pixel_step: 4,
        };
        blend_component(
            &mut plane,
            2,
            2,
            &img,
            30000,
            SampleFormat::U16Le.alpha_fixed(255),
            1,
            1,
            SampleFormat::U16Le,
        );
        let u = u16::from_le_bytes([data[0], data[1]]);
        let v = u16::from_le_bytes([data[2], data[3]]);
        assert!(u > 0, "addressed component must change");
        assert_eq!(v, 0, "other interleaved component must stay untouched");
    }
}
