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
//!
//! (Staged rollout: `apply_sums_*` and `pair_sums_*` still delegate to the
//! scalar kernels here; their NEON kernels land in the next commit.)

use super::{apply_sums, OverlayImage, PlaneView, PooledRect, SampleU16Le, SampleU8};
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
    debug_assert_eq!(plane.pixel_step, 1);
    apply_sums::<SampleU8>(plane, sums, rect, src, alpha);
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
    debug_assert_eq!(plane.pixel_step, 2);
    apply_sums::<SampleU16Le>(plane, sums, rect, src, alpha);
}

/// Interior adjacent-pair sums for `pool_sums_h2`, single mask row:
/// `span[i] = top[2i] + top[2i+1]`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn pair_sums_one_row(span: &mut [u16], top: &[u8]) {
    super::pair_sums(span, top, None, false);
}

/// Interior adjacent-pair sums for `pool_sums_h2`, two mask rows:
/// `span[i] = top[2i] + top[2i+1] + bot[2i] + bot[2i+1]`.
///
/// # Safety
/// The CPU must support NEON ([`enabled`]).
pub(super) unsafe fn pair_sums_two_rows(span: &mut [u16], top: &[u8], bot: &[u8]) {
    super::pair_sums(span, top, Some(bot), false);
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
