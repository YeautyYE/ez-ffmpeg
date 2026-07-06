//! Runtime-dispatched NEON blend kernels (aarch64 only).
//!
//! Every kernel reproduces its scalar counterpart in `super` bit-for-bit:
//! the same u32 lane arithmetic (`((C - alpha_px) * dst + alpha_px * src)
//! >> SHIFT` with the FFmpeg constants, whose worst-case numerator is
//! exactly `u32::MAX`), the same left-to-right pixel order, and the same
//! zero-mask identity (`alpha_px == 0` blends to exactly `dst`). Because of
//! that identity, skipping all-zero blocks — in wider blocks than the
//! scalar kernels use — cannot change a single output byte. Parity against
//! the scalar kernels is enforced by the tests at the bottom of this file,
//! by `super::lab`'s variant tests, and by the captured-mask test in
//! `bench_kernels.rs`.
//!
//! Dispatch contract: callers check [`enabled`] and only route tightly-packed
//! planes here (`pixel_step` == sample bytes); every other shape stays on the
//! scalar kernels. All `unsafe fn`s below require NEON as their only safety
//! precondition beyond ordinary slice validity.
//!
//! u16 samples are little-endian in memory (matching scalar `SampleU16Le`);
//! the vector code loads/stores them as bytes and `vreinterpret`s to `u16`,
//! which is the same value as `u16::from_le_bytes` on the little-endian
//! aarch64 targets this module compiles for.

use super::{OverlayImage, PlaneView, PooledRect, SampleU16Le, SampleU8};
use core::arch::aarch64::*;

/// True when the running CPU supports NEON. On aarch64 NEON is baseline-
/// mandatory, so this is effectively always true; still probed at runtime for
/// correctness (mirrors the AVX2 backend's CPU gate).
pub(super) fn enabled() -> bool {
    std::arch::is_aarch64_feature_detected!("neon")
}

/// NEON counterpart of `blend_direct::<SampleU8>` for `pixel_step == 1`.
/// Same clipped-coords contract as the scalar kernel.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn blend_direct_u8(
    plane: &mut PlaneView<'_>,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    (x0, y0): (usize, usize),
    (xm0, ym0): (usize, usize),
    (w, h): (usize, usize),
) {
    debug_assert_eq!(plane.pixel_step, 1);
    for row in 0..h {
        let mask_start = (ym0 + row) * image.stride + xm0;
        let mask_row = &image.bitmap[mask_start..mask_start + w];
        let dst_start = (y0 + row) * plane.linesize + x0;
        let dst_row = &mut plane.data[dst_start..dst_start + w];
        blend_row_u8(dst_row, mask_row, src, alpha);
    }
}

/// NEON counterpart of `blend_direct::<SampleU16Le>` for `pixel_step == 2`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn blend_direct_u16(
    plane: &mut PlaneView<'_>,
    image: &OverlayImage<'_>,
    src: u32,
    alpha: u32,
    (x0, y0): (usize, usize),
    (xm0, ym0): (usize, usize),
    (w, h): (usize, usize),
) {
    debug_assert_eq!(plane.pixel_step, 2);
    for row in 0..h {
        let mask_start = (ym0 + row) * image.stride + xm0;
        let mask_row = &image.bitmap[mask_start..mask_start + w];
        let dst_start = (y0 + row) * plane.linesize + x0 * 2;
        let dst_row = &mut plane.data[dst_start..dst_start + w * 2];
        blend_row_u16(dst_row, mask_row, src, alpha);
    }
}

/// NEON counterpart of `apply_sums::<SampleU8>` for `pixel_step == 1`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn apply_sums_u8(
    plane: &mut PlaneView<'_>,
    sums: &[u16],
    rect: PooledRect,
    src: u32,
    alpha: u32,
) {
    if alpha == 0 {
        return;
    }
    for row in 0..rect.ph {
        let sums_row = &sums[row * rect.pw..(row + 1) * rect.pw];
        let dst_start = (rect.py0 + row) * plane.linesize + rect.px0;
        let dst_row = &mut plane.data[dst_start..dst_start + rect.pw];
        apply_row_u8(dst_row, sums_row, src, alpha, rect.shift);
    }
}

/// NEON counterpart of `apply_sums::<SampleU16Le>` for `pixel_step == 2`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn apply_sums_u16(
    plane: &mut PlaneView<'_>,
    sums: &[u16],
    rect: PooledRect,
    src: u32,
    alpha: u32,
) {
    if alpha == 0 {
        return;
    }
    for row in 0..rect.ph {
        let sums_row = &sums[row * rect.pw..(row + 1) * rect.pw];
        let dst_start = (rect.py0 + row) * plane.linesize + rect.px0 * 2;
        let dst_row = &mut plane.data[dst_start..dst_start + rect.pw * 2];
        apply_row_u16(dst_row, sums_row, src, alpha, rect.shift);
    }
}

/// Interior adjacent-pair sums for `pool_sums_h2`, single mask row:
/// `span[i] = top[2i] + top[2i+1]`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn pair_sums_one_row(span: &mut [u16], top: &[u8]) {
    pair_sums_row::<false>(span, top, top);
}

/// Interior adjacent-pair sums for `pool_sums_h2`, two mask rows:
/// `span[i] = top[2i] + top[2i+1] + bot[2i] + bot[2i+1]`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn pair_sums_two_rows(span: &mut [u16], top: &[u8], bot: &[u8]) {
    pair_sums_row::<true>(span, top, bot);
}

// ---------------------------------------------------------------------------
// Shared vector helpers. Each is `#[target_feature(enable = "neon")]` (so the
// intrinsics are legal) and `unsafe` (so its body is an unsafe context); the
// callers below all carry the same feature, so no per-call unsafe blocks are
// needed inside them.
// ---------------------------------------------------------------------------

/// True when all sixteen mask bytes are zero. ORs the two 64-bit lanes (no
/// horizontal add, so no overflow risk).
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn is_zero_u8x16(v: uint8x16_t) -> bool {
    let words = vreinterpretq_u64_u8(v);
    (vgetq_lane_u64::<0>(words) | vgetq_lane_u64::<1>(words)) == 0
}

/// True when all eight sums (sixteen bytes) are zero.
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn is_zero_u16x8(v: uint16x8_t) -> bool {
    let words = vreinterpretq_u64_u16(v);
    (vgetq_lane_u64::<0>(words) | vgetq_lane_u64::<1>(words)) == 0
}

/// Zero-extends sixteen u8s to four `u32x4` groups (pixels 0..4, 4..8, 8..12,
/// 12..16).
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn widen_u8x16(v: uint8x16_t) -> [uint32x4_t; 4] {
    let lo = vmovl_u8(vget_low_u8(v)); // u16x8: bytes 0..8
    let hi = vmovl_u8(vget_high_u8(v)); // u16x8: bytes 8..16
    [
        vmovl_u16(vget_low_u16(lo)),
        vmovl_u16(vget_high_u16(lo)),
        vmovl_u16(vget_low_u16(hi)),
        vmovl_u16(vget_high_u16(hi)),
    ]
}

/// Zero-extends eight u16s to two `u32x4` groups (lanes 0..4, 4..8).
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn widen_u16x8(v: uint16x8_t) -> [uint32x4_t; 2] {
    [vmovl_u16(vget_low_u16(v)), vmovl_u16(vget_high_u16(v))]
}

/// Truncates four `u32x4` groups (each lane <= 255) back to sixteen u8s.
/// Exact because every lane already fits a byte.
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn narrow_to_u8x16(b: [uint32x4_t; 4]) -> uint8x16_t {
    let lo = vcombine_u16(vmovn_u32(b[0]), vmovn_u32(b[1])); // u16x8: pixels 0..8
    let hi = vcombine_u16(vmovn_u32(b[2]), vmovn_u32(b[3])); // u16x8: pixels 8..16
    vcombine_u8(vmovn_u16(lo), vmovn_u16(hi))
}

/// Truncates two `u32x4` groups (each lane <= 255) back to eight u8s.
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn narrow_to_u8x8(b: [uint32x4_t; 2]) -> uint8x8_t {
    vmovn_u16(vcombine_u16(vmovn_u32(b[0]), vmovn_u32(b[1])))
}

/// Truncates two `u32x4` groups (each lane <= 0xFFFF) back to eight u16s.
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn narrow_to_u16x8(b: [uint32x4_t; 2]) -> uint16x8_t {
    vcombine_u16(vmovn_u32(b[0]), vmovn_u32(b[1]))
}

/// Four u8 pixels through FFmpeg's `((0x0101_0101 - a) * dst + a * src) >> 24`
/// in u32 lanes (every numerator fits u32 exactly). `coverage`/`dst` carry the
/// mask (or pooled coverage) and destination zero-extended to u32; the result
/// is four u32 lanes each <= 255.
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn blend4_u8(
    coverage: uint32x4_t,
    dst: uint32x4_t,
    alpha_v: uint32x4_t,
    src_v: uint32x4_t,
) -> uint32x4_t {
    let a = vmulq_u32(coverage, alpha_v);
    let inv = vsubq_u32(vdupq_n_u32(0x0101_0101), a);
    let num = vaddq_u32(vmulq_u32(inv, dst), vmulq_u32(a, src_v));
    vshrq_n_u32::<24>(num)
}

/// Four u16 pixels through `((0x0001_0001 - a) * dst + a * src) >> 16` in u32
/// lanes (numerator <= u32::MAX exactly); the result is four u32 lanes each
/// <= 0xFFFF.
///
/// # Safety
/// Requires NEON.
#[inline]
#[target_feature(enable = "neon")]
unsafe fn blend4_u16(
    coverage: uint32x4_t,
    dst: uint32x4_t,
    alpha_v: uint32x4_t,
    src_v: uint32x4_t,
) -> uint32x4_t {
    let a = vmulq_u32(coverage, alpha_v);
    let inv = vsubq_u32(vdupq_n_u32(0x0001_0001), a);
    let num = vaddq_u32(vmulq_u32(inv, dst), vmulq_u32(a, src_v));
    vshrq_n_u32::<16>(num)
}

/// One row of the 1:1 u8 blend: 16 pixels per iteration (one 16-byte mask
/// vector, skipped when all-zero), scalar `blend_span` on the tail.
///
/// # Safety
/// Requires NEON; `dst.len() == mask.len()`.
#[target_feature(enable = "neon")]
unsafe fn blend_row_u8(dst: &mut [u8], mask: &[u8], src: u32, alpha: u32) {
    debug_assert_eq!(dst.len(), mask.len());
    let alpha_v = vdupq_n_u32(alpha);
    let src_v = vdupq_n_u32(src);
    let n = mask.len();
    let mut x = 0usize;
    while x + 16 <= n {
        // In-bounds: x + 16 <= n == dst.len() covers both 16-byte accesses.
        let m = vld1q_u8(mask.as_ptr().add(x));
        if !is_zero_u8x16(m) {
            let mg = widen_u8x16(m);
            let dg = widen_u8x16(vld1q_u8(dst.as_ptr().add(x)));
            let b = [
                blend4_u8(mg[0], dg[0], alpha_v, src_v),
                blend4_u8(mg[1], dg[1], alpha_v, src_v),
                blend4_u8(mg[2], dg[2], alpha_v, src_v),
                blend4_u8(mg[3], dg[3], alpha_v, src_v),
            ];
            vst1q_u8(dst.as_mut_ptr().add(x), narrow_to_u8x16(b));
        }
        x += 16;
    }
    if !mask[x..].iter().all(|&m| m == 0) {
        super::blend_span::<SampleU8>(&mut dst[x..], &mask[x..], src, alpha, 1);
    }
}

/// One row of the 1:1 u16 blend at `pixel_step == 2`: 16 pixels per iteration
/// (16 mask bytes / 32 dst bytes, skipped when the mask block is all-zero),
/// scalar `blend_span` on the tail.
///
/// # Safety
/// Requires NEON; `dst.len() == mask.len() * 2`.
#[target_feature(enable = "neon")]
unsafe fn blend_row_u16(dst: &mut [u8], mask: &[u8], src: u32, alpha: u32) {
    debug_assert_eq!(dst.len(), mask.len() * 2);
    let alpha_v = vdupq_n_u32(alpha);
    let src_v = vdupq_n_u32(src);
    let n = mask.len();
    let mut x = 0usize;
    while x + 16 <= n {
        // In-bounds: x + 16 <= n covers the mask read; dst accesses span
        // bytes [2x, 2x + 32) <= 2n == dst.len().
        let m = vld1q_u8(mask.as_ptr().add(x));
        if !is_zero_u8x16(m) {
            let mg = widen_u8x16(m);
            let base = dst.as_mut_ptr().add(2 * x);
            let d_lo = widen_u16x8(vreinterpretq_u16_u8(vld1q_u8(base))); // pixels 0..8
            let d_hi = widen_u16x8(vreinterpretq_u16_u8(vld1q_u8(base.add(16)))); // pixels 8..16
            let lo = narrow_to_u16x8([
                blend4_u16(mg[0], d_lo[0], alpha_v, src_v),
                blend4_u16(mg[1], d_lo[1], alpha_v, src_v),
            ]);
            let hi = narrow_to_u16x8([
                blend4_u16(mg[2], d_hi[0], alpha_v, src_v),
                blend4_u16(mg[3], d_hi[1], alpha_v, src_v),
            ]);
            vst1q_u8(base, vreinterpretq_u8_u16(lo));
            vst1q_u8(base.add(16), vreinterpretq_u8_u16(hi));
        }
        x += 16;
    }
    if !mask[x..].iter().all(|&m| m == 0) {
        super::blend_span::<SampleU16Le>(&mut dst[2 * x..], &mask[x..], src, alpha, 2);
    }
}

/// One row of the pooled u8 apply: 8 output pixels per iteration. Parity order
/// matters: coverage is `sum >> shift` FIRST (u16 lanes), then widened and
/// multiplied by alpha — exactly the scalar `(u32::from(sum) >> shift) * alpha`
/// (each pooled sum aggregates `1 << shift` mask bytes, so `sum >> shift` fits
/// a byte). The variable right shift is a `vshlq_u16` by a negative count.
///
/// # Safety
/// Requires NEON; `dst.len() == sums.len()`.
#[target_feature(enable = "neon")]
unsafe fn apply_row_u8(dst: &mut [u8], sums: &[u16], src: u32, alpha: u32, shift: u32) {
    debug_assert_eq!(dst.len(), sums.len());
    let alpha_v = vdupq_n_u32(alpha);
    let src_v = vdupq_n_u32(src);
    let shift_v = vdupq_n_s16(-(shift as i16)); // negative count => logical right shift
    let n = sums.len();
    let mut x = 0usize;
    while x + 8 <= n {
        // In-bounds: x + 8 <= n covers the 16-byte sums read and the 8-byte
        // dst read/write.
        let s = vld1q_u16(sums.as_ptr().add(x));
        if !is_zero_u16x8(s) {
            let cov = widen_u16x8(vshlq_u16(s, shift_v));
            let dg = widen_u16x8(vmovl_u8(vld1_u8(dst.as_ptr().add(x))));
            let b = [
                blend4_u8(cov[0], dg[0], alpha_v, src_v),
                blend4_u8(cov[1], dg[1], alpha_v, src_v),
            ];
            vst1_u8(dst.as_mut_ptr().add(x), narrow_to_u8x8(b));
        }
        x += 8;
    }
    if !sums[x..].iter().all(|&s| s == 0) {
        super::apply_sums_span::<SampleU8>(&mut dst[x..], &sums[x..], src, alpha, shift, 1);
    }
}

/// One row of the pooled u16 apply at `pixel_step == 2`: 8 output pixels per
/// iteration, coverage computed sum-shift-first like the scalar path.
///
/// # Safety
/// Requires NEON; `dst.len() == sums.len() * 2`.
#[target_feature(enable = "neon")]
unsafe fn apply_row_u16(dst: &mut [u8], sums: &[u16], src: u32, alpha: u32, shift: u32) {
    debug_assert_eq!(dst.len(), sums.len() * 2);
    let alpha_v = vdupq_n_u32(alpha);
    let src_v = vdupq_n_u32(src);
    let shift_v = vdupq_n_s16(-(shift as i16));
    let n = sums.len();
    let mut x = 0usize;
    while x + 8 <= n {
        // In-bounds: x + 8 <= n covers the 16-byte sums read; dst accesses
        // span bytes [2x, 2x + 16) <= 2n == dst.len().
        let s = vld1q_u16(sums.as_ptr().add(x));
        if !is_zero_u16x8(s) {
            let cov = widen_u16x8(vshlq_u16(s, shift_v));
            let base = dst.as_mut_ptr().add(2 * x);
            let dg = widen_u16x8(vreinterpretq_u16_u8(vld1q_u8(base)));
            let out = narrow_to_u16x8([
                blend4_u16(cov[0], dg[0], alpha_v, src_v),
                blend4_u16(cov[1], dg[1], alpha_v, src_v),
            ]);
            vst1q_u8(base, vreinterpretq_u8_u16(out));
        }
        x += 8;
    }
    if !sums[x..].iter().all(|&s| s == 0) {
        super::apply_sums_span::<SampleU16Le>(&mut dst[2 * x..], &sums[x..], src, alpha, shift, 2);
    }
}

/// Interior pair-sums: 8 output sums per iteration via `vpaddlq_u8` (adjacent
/// u8 pairs widened to u16; the max sum 4 * 255 = 1020 stays well within u16,
/// so the sums are exact), scalar loop on the tail.
///
/// # Safety
/// Requires NEON; `top.len() == span.len() * 2` (and `bot` likewise when
/// `TWO_ROWS`).
#[target_feature(enable = "neon")]
unsafe fn pair_sums_row<const TWO_ROWS: bool>(span: &mut [u16], top: &[u8], bot: &[u8]) {
    debug_assert_eq!(top.len(), span.len() * 2);
    if TWO_ROWS {
        debug_assert_eq!(bot.len(), span.len() * 2);
    }
    let n = span.len();
    let mut x = 0usize;
    while x + 8 <= n {
        // In-bounds: x + 8 <= n covers the 16-byte mask reads (offset 2x <=
        // 2n - 16) and the 16-byte span store.
        let mut s = vpaddlq_u8(vld1q_u8(top.as_ptr().add(2 * x)));
        if TWO_ROWS {
            s = vaddq_u16(s, vpaddlq_u8(vld1q_u8(bot.as_ptr().add(2 * x))));
        }
        vst1q_u16(span.as_mut_ptr().add(x), s);
        x += 8;
    }
    for i in x..n {
        let mut sum = u16::from(top[2 * i]) + u16::from(top[2 * i + 1]);
        if TWO_ROWS {
            sum += u16::from(bot[2 * i]) + u16::from(bot[2 * i + 1]);
        }
        span[i] = sum;
    }
}

#[cfg(test)]
mod tests {
    use super::super::{apply_sums, blend_direct, SampleFormat};
    use super::*;

    /// Deterministic LCG byte stream (fixed seed per call site).
    fn lcg_bytes(len: usize, seed: &mut u64) -> Vec<u8> {
        (0..len)
            .map(|_| {
                *seed = seed
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (*seed >> 33) as u8
            })
            .collect()
    }

    /// Mask rows exercising every byte value, zero runs (skip blocks) and
    /// random content, at widths that cover vector body + tail.
    fn mask_variants(w: usize, seed: &mut u64) -> Vec<Vec<u8>> {
        let ramp: Vec<u8> = (0..w).map(|i| (i % 256) as u8).collect();
        let mut sparse = lcg_bytes(w, seed);
        for (i, byte) in sparse.iter_mut().enumerate() {
            if (i / 24) % 2 == 0 {
                *byte = 0;
            }
        }
        vec![
            vec![0u8; w],
            vec![255u8; w],
            ramp,
            sparse,
            lcg_bytes(w, seed),
        ]
    }

    fn opacity_alphas(sample: SampleFormat) -> Vec<u32> {
        [0u8, 1, 127, 128, 254, 255]
            .into_iter()
            .map(|o| sample.alpha_fixed(o))
            .filter(|&a| a != 0) // alpha 0 never reaches the kernels
            .collect()
    }

    /// Direct NEON kernels vs `blend_direct` on identical rows: all alpha
    /// edge values, mask bytes 0..=255, widths through body + tail, u8 and
    /// u16 dst edge patterns.
    #[test]
    fn direct_rows_match_scalar_bitexact() {
        if !enabled() {
            eprintln!("skipping: NEON unavailable on this CPU");
            return;
        }
        let mut seed = 0x00C0_FFEE_1234_5678u64;
        for w in (1..=67).chain([256]) {
            for mask in mask_variants(w, &mut seed) {
                let image = OverlayImage {
                    w,
                    h: 1,
                    stride: w,
                    bitmap: &mask,
                    color: 0xFFFFFF00,
                    dst_x: 0,
                    dst_y: 0,
                };
                // u8: dst edge fills + random.
                for alpha in opacity_alphas(SampleFormat::U8) {
                    for src in [0u32, 16, 128, 235, 255] {
                        for base in [vec![0u8; w], vec![255u8; w], lcg_bytes(w, &mut seed)] {
                            let mut expected = base.clone();
                            blend_direct::<SampleU8>(
                                &mut PlaneView {
                                    data: &mut expected,
                                    linesize: w,
                                    pixel_step: 1,
                                },
                                &image,
                                src,
                                alpha,
                                (0, 0),
                                (0, 0),
                                (w, 1),
                            );
                            let mut got = base.clone();
                            // SAFETY: `enabled()` checked at test entry.
                            unsafe {
                                blend_direct_u8(
                                    &mut PlaneView {
                                        data: &mut got,
                                        linesize: w,
                                        pixel_step: 1,
                                    },
                                    &image,
                                    src,
                                    alpha,
                                    (0, 0),
                                    (0, 0),
                                    (w, 1),
                                )
                            };
                            assert_eq!(got, expected, "u8 w={w} alpha={alpha} src={src}");
                        }
                    }
                }
                // u16: dst 0x0000 / 0xFFFF / random, src across the range.
                for alpha in opacity_alphas(SampleFormat::U16Le) {
                    for src in [0u32, 514, 943, 60395, 65535] {
                        for base in [
                            vec![0u8; w * 2],
                            vec![255u8; w * 2],
                            lcg_bytes(w * 2, &mut seed),
                        ] {
                            let mut expected = base.clone();
                            blend_direct::<SampleU16Le>(
                                &mut PlaneView {
                                    data: &mut expected,
                                    linesize: w * 2,
                                    pixel_step: 2,
                                },
                                &image,
                                src,
                                alpha,
                                (0, 0),
                                (0, 0),
                                (w, 1),
                            );
                            let mut got = base.clone();
                            // SAFETY: `enabled()` checked at test entry.
                            unsafe {
                                blend_direct_u16(
                                    &mut PlaneView {
                                        data: &mut got,
                                        linesize: w * 2,
                                        pixel_step: 2,
                                    },
                                    &image,
                                    src,
                                    alpha,
                                    (0, 0),
                                    (0, 0),
                                    (w, 1),
                                )
                            };
                            assert_eq!(got, expected, "u16 w={w} alpha={alpha} src={src}");
                        }
                    }
                }
            }
        }
    }

    /// Multi-row geometry (stride > w, mask offsets, several rows) through
    /// the NEON wrappers vs the scalar kernel.
    #[test]
    fn direct_multi_row_geometry_matches_scalar() {
        if !enabled() {
            eprintln!("skipping: NEON unavailable on this CPU");
            return;
        }
        let mut seed = 0xDEAD_BEEF_0BAD_F00Du64;
        let (w, h, stride) = (61usize, 9usize, 71usize);
        let bitmap = lcg_bytes(stride * (h - 1) + w, &mut seed);
        let image = OverlayImage {
            w,
            h,
            stride,
            bitmap: &bitmap,
            color: 0xFFFFFF00,
            dst_x: 0,
            dst_y: 0,
        };
        let coords = ((3usize, 2usize), (1usize, 1usize), (w - 4, h - 3));
        for (bytes, src, alpha) in [
            (1usize, 235u32, SampleFormat::U8.alpha_fixed(200)),
            (2, 943, SampleFormat::U16Le.alpha_fixed(200)),
        ] {
            let linesize = (w + 8) * bytes;
            let base = lcg_bytes(linesize * (h + 4), &mut seed);
            let mut expected = base.clone();
            let mut got = base.clone();
            if bytes == 1 {
                blend_direct::<SampleU8>(
                    &mut PlaneView {
                        data: &mut expected,
                        linesize,
                        pixel_step: 1,
                    },
                    &image,
                    src,
                    alpha,
                    coords.0,
                    coords.1,
                    coords.2,
                );
                // SAFETY: `enabled()` checked at test entry.
                unsafe {
                    blend_direct_u8(
                        &mut PlaneView {
                            data: &mut got,
                            linesize,
                            pixel_step: 1,
                        },
                        &image,
                        src,
                        alpha,
                        coords.0,
                        coords.1,
                        coords.2,
                    )
                };
            } else {
                blend_direct::<SampleU16Le>(
                    &mut PlaneView {
                        data: &mut expected,
                        linesize,
                        pixel_step: 2,
                    },
                    &image,
                    src,
                    alpha,
                    coords.0,
                    coords.1,
                    coords.2,
                );
                // SAFETY: `enabled()` checked at test entry.
                unsafe {
                    blend_direct_u16(
                        &mut PlaneView {
                            data: &mut got,
                            linesize,
                            pixel_step: 2,
                        },
                        &image,
                        src,
                        alpha,
                        coords.0,
                        coords.1,
                        coords.2,
                    )
                };
            }
            assert_eq!(got, expected, "bytes={bytes}");
        }
    }

    /// `pair_sums_row` vs the scalar pair-sum loops for both row modes,
    /// spans covering vector body + tail, mask bytes up to 255.
    #[test]
    fn pair_sums_match_scalar_bitexact() {
        if !enabled() {
            eprintln!("skipping: NEON unavailable on this CPU");
            return;
        }
        let mut seed = 0x5EED_5EED_5EED_5EEDu64;
        for n in (0..=67).chain([256]) {
            let mut tops = mask_variants((2 * n).max(1), &mut seed);
            for top in &mut tops {
                top.truncate(2 * n);
            }
            for top in tops {
                let bot = lcg_bytes(2 * n, &mut seed);
                for two_rows in [false, true] {
                    let mut expected = vec![0u16; n];
                    for (i, sum) in expected.iter_mut().enumerate() {
                        let mut s = u16::from(top[2 * i]) + u16::from(top[2 * i + 1]);
                        if two_rows {
                            s += u16::from(bot[2 * i]) + u16::from(bot[2 * i + 1]);
                        }
                        *sum = s;
                    }
                    let mut got = vec![0xAAAAu16; n]; // poisoned: every slot must be written
                                                      // SAFETY: `enabled()` checked at test entry.
                    unsafe {
                        if two_rows {
                            pair_sums_two_rows(&mut got, &top, &bot);
                        } else {
                            pair_sums_one_row(&mut got, &top);
                        }
                    }
                    assert_eq!(got, expected, "n={n} two_rows={two_rows}");
                }
            }
        }
    }

    /// `apply_sums_u8`/`apply_sums_u16` vs the scalar `apply_sums` across
    /// shifts, alphas, widths and sum values up to the 1020 maximum.
    #[test]
    fn apply_sums_match_scalar_bitexact() {
        if !enabled() {
            eprintln!("skipping: NEON unavailable on this CPU");
            return;
        }
        let mut seed = 0x0123_4567_89AB_CDEFu64;
        for pw in (1..=67).step_by(3).chain([64, 256]) {
            let ph = 3usize;
            for shift in [1u32, 2] {
                // Pooling invariant: a sum aggregates `1 << shift` mask
                // bytes, so its ceiling is `(1 << shift) * 255` (any more
                // would overflow the u32 blend headroom).
                let max_sum = (1u16 << shift) * 255;
                let mut sums = Vec::with_capacity(pw * ph);
                for i in 0..pw * ph {
                    let value = match i % 5 {
                        0 => 0u16,
                        1 => 1,
                        2 => max_sum / 2,
                        3 => max_sum,
                        _ => {
                            seed = seed
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407);
                            ((seed >> 33) % u64::from(max_sum + 1)) as u16
                        }
                    };
                    sums.push(value);
                }
                for (sample, bytes, srcs) in [
                    (SampleFormat::U8, 1usize, [16u32, 128, 240]),
                    (SampleFormat::U16Le, 2, [514, 943, 60395]),
                ] {
                    for alpha in opacity_alphas(sample) {
                        for src in srcs {
                            let rect = PooledRect {
                                px0: 2,
                                py0: 1,
                                pw,
                                ph,
                                shift,
                            };
                            let linesize = (pw + 5) * bytes;
                            let base = lcg_bytes(linesize * (ph + 3), &mut seed);
                            let mut expected = base.clone();
                            let mut got = base.clone();
                            match sample {
                                SampleFormat::U8 => {
                                    apply_sums::<SampleU8>(
                                        &mut PlaneView {
                                            data: &mut expected,
                                            linesize,
                                            pixel_step: 1,
                                        },
                                        &sums,
                                        rect,
                                        src,
                                        alpha,
                                    );
                                    // SAFETY: `enabled()` checked at entry.
                                    unsafe {
                                        apply_sums_u8(
                                            &mut PlaneView {
                                                data: &mut got,
                                                linesize,
                                                pixel_step: 1,
                                            },
                                            &sums,
                                            rect,
                                            src,
                                            alpha,
                                        )
                                    };
                                }
                                SampleFormat::U16Le => {
                                    apply_sums::<SampleU16Le>(
                                        &mut PlaneView {
                                            data: &mut expected,
                                            linesize,
                                            pixel_step: 2,
                                        },
                                        &sums,
                                        rect,
                                        src,
                                        alpha,
                                    );
                                    // SAFETY: `enabled()` checked at entry.
                                    unsafe {
                                        apply_sums_u16(
                                            &mut PlaneView {
                                                data: &mut got,
                                                linesize,
                                                pixel_step: 2,
                                            },
                                            &sums,
                                            rect,
                                            src,
                                            alpha,
                                        )
                                    };
                                }
                            }
                            assert_eq!(
                                got, expected,
                                "sample={sample:?} pw={pw} shift={shift} alpha={alpha} src={src}"
                            );
                        }
                    }
                }
            }
        }
    }
}
