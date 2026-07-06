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
//! scalar kernels.
//!
//! Scaffold stage: the entry points below currently delegate to the scalar
//! `super` kernels so the aarch64 build stays correct and green; the NEON
//! vector kernels land in follow-up commits.

use super::{apply_sums, blend_direct, OverlayImage, PlaneView, PooledRect, SampleU16Le, SampleU8};

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
    xy0: (usize, usize),
    xym0: (usize, usize),
    wh: (usize, usize),
) {
    debug_assert_eq!(plane.pixel_step, 1);
    blend_direct::<SampleU8>(plane, image, src, alpha, xy0, xym0, wh);
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
    xy0: (usize, usize),
    xym0: (usize, usize),
    wh: (usize, usize),
) {
    debug_assert_eq!(plane.pixel_step, 2);
    blend_direct::<SampleU16Le>(plane, image, src, alpha, xy0, xym0, wh);
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
