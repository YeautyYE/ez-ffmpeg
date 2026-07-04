//! Runtime-dispatched AVX2 blend kernels (x86-64 only).
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
//! Dispatch contract: callers check [`enabled`] (CPUID probed once) and
//! only route tightly-packed planes here (`pixel_step` == sample bytes);
//! every other shape stays on the scalar kernels. All `unsafe fn`s below
//! require AVX2 as their only safety precondition beyond ordinary slice
//! validity.

use super::{OverlayImage, PlaneView, PooledRect, SampleU16Le, SampleU8};
use core::arch::x86_64::*;
use std::sync::OnceLock;

/// True when the running CPU supports AVX2. Probed once, then a plain load.
pub(super) fn enabled() -> bool {
    static AVX2: OnceLock<bool> = OnceLock::new();
    *AVX2.get_or_init(|| std::arch::is_x86_feature_detected!("avx2"))
}

/// AVX2 counterpart of `blend_direct::<SampleU8>` for `pixel_step == 1`.
/// Same clipped-coords contract as the scalar kernel.
///
/// # Safety
/// The CPU must support AVX2 ([`enabled`]).
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

/// AVX2 counterpart of `blend_direct::<SampleU16Le>` for `pixel_step == 2`.
///
/// # Safety
/// The CPU must support AVX2 ([`enabled`]).
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

/// AVX2 counterpart of `apply_sums::<SampleU8>` for `pixel_step == 1`.
///
/// # Safety
/// The CPU must support AVX2 ([`enabled`]).
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

/// AVX2 counterpart of `apply_sums::<SampleU16Le>` for `pixel_step == 2`.
///
/// # Safety
/// The CPU must support AVX2 ([`enabled`]).
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
/// The CPU must support AVX2 ([`enabled`]).
pub(super) unsafe fn pair_sums_one_row(span: &mut [u16], top: &[u8]) {
    pair_sums_row::<false>(span, top, top);
}

/// Interior adjacent-pair sums for `pool_sums_h2`, two mask rows:
/// `span[i] = top[2i] + top[2i+1] + bot[2i] + bot[2i+1]`.
///
/// # Safety
/// The CPU must support AVX2 ([`enabled`]).
pub(super) unsafe fn pair_sums_two_rows(span: &mut [u16], top: &[u8], bot: &[u8]) {
    pair_sums_row::<true>(span, top, bot);
}

/// One row of the 1:1 u8 blend: 32 pixels per iteration (one 32-byte mask
/// vector, skipped when all-zero), scalar `blend_span` on the tail.
///
/// # Safety
/// Requires AVX2; `dst.len() == mask.len()`.
#[target_feature(enable = "avx2")]
unsafe fn blend_row_u8(dst: &mut [u8], mask: &[u8], src: u32, alpha: u32) {
    debug_assert_eq!(dst.len(), mask.len());
    let alpha_v = _mm256_set1_epi32(alpha as i32);
    let src_v = _mm256_set1_epi32(src as i32);
    // Word order fix-up for the final u32 -> u8 pack (see blend32_u8).
    let regroup = _mm256_setr_epi32(0, 4, 1, 5, 2, 6, 3, 7);
    let n = mask.len();
    let mut x = 0usize;
    while x + 32 <= n {
        // In-bounds: x + 32 <= n == dst.len() covers both 32-byte accesses.
        let m = _mm256_loadu_si256(mask.as_ptr().add(x).cast());
        if _mm256_testz_si256(m, m) == 0 {
            blend32_u8(dst.as_mut_ptr().add(x), m, alpha_v, src_v, regroup);
        }
        x += 32;
    }
    if !mask[x..].iter().all(|&m| m == 0) {
        super::blend_span::<SampleU8>(&mut dst[x..], &mask[x..], src, alpha, 1);
    }
}

/// Blends 32 contiguous u8 pixels: four 8-lane u32 groups through FFmpeg's
/// `((0x0101_0101 - a) * dst + a * src) >> 24` (every numerator fits u32
/// exactly), then packed back to bytes. The packus stages are exact because
/// every lane is <= 255 after the 24-bit shift, so saturation never fires.
///
/// # Safety
/// Requires AVX2; `dst` must be valid for a 32-byte read and write.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn blend32_u8(
    dst: *mut u8,
    mask: __m256i,
    alpha_v: __m256i,
    src_v: __m256i,
    regroup: __m256i,
) {
    let m_lo = _mm256_castsi256_si128(mask);
    let m_hi = _mm256_extracti128_si256::<1>(mask);
    let d = _mm256_loadu_si256(dst.cast());
    let d_lo = _mm256_castsi256_si128(d);
    let d_hi = _mm256_extracti128_si256::<1>(d);
    let b0 = blend8_u8(_mm256_cvtepu8_epi32(m_lo), d_lo, alpha_v, src_v);
    let b1 = blend8_u8(
        _mm256_cvtepu8_epi32(_mm_srli_si128::<8>(m_lo)),
        _mm_srli_si128::<8>(d_lo),
        alpha_v,
        src_v,
    );
    let b2 = blend8_u8(_mm256_cvtepu8_epi32(m_hi), d_hi, alpha_v, src_v);
    let b3 = blend8_u8(
        _mm256_cvtepu8_epi32(_mm_srli_si128::<8>(m_hi)),
        _mm_srli_si128::<8>(d_hi),
        alpha_v,
        src_v,
    );
    // u16 lanes: [b0.0-3, b1.0-3 | b0.4-7, b1.4-7], same for b2/b3.
    let p01 = _mm256_packus_epi32(b0, b1);
    let p23 = _mm256_packus_epi32(b2, b3);
    // u8, as u32 words: [b0l b1l b2l b3l | b0h b1h b2h b3h].
    let bytes = _mm256_packus_epi16(p01, p23);
    // Restore pixel order: words [0,4,1,5,2,6,3,7] = b0l b0h b1l b1h ...
    let bytes = _mm256_permutevar8x32_epi32(bytes, regroup);
    _mm256_storeu_si256(dst.cast(), bytes);
}

/// Eight u8 pixels -> eight blended u32 lanes (each <= 255). `coverage`
/// carries the per-pixel mask (or pooled coverage) zero-extended to u32.
///
/// # Safety
/// Requires AVX2.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn blend8_u8(coverage: __m256i, dst8: __m128i, alpha_v: __m256i, src_v: __m256i) -> __m256i {
    let a = _mm256_mullo_epi32(coverage, alpha_v);
    let inv = _mm256_sub_epi32(_mm256_set1_epi32(0x0101_0101), a);
    let d = _mm256_cvtepu8_epi32(dst8);
    let num = _mm256_add_epi32(_mm256_mullo_epi32(inv, d), _mm256_mullo_epi32(a, src_v));
    _mm256_srli_epi32::<24>(num)
}

/// One row of the 1:1 u16 blend at `pixel_step == 2`: 16 pixels per
/// iteration (16 mask bytes / 32 dst bytes, skipped when the mask block is
/// all-zero), scalar `blend_span` on the tail.
///
/// # Safety
/// Requires AVX2; `dst.len() == mask.len() * 2`.
#[target_feature(enable = "avx2")]
unsafe fn blend_row_u16(dst: &mut [u8], mask: &[u8], src: u32, alpha: u32) {
    debug_assert_eq!(dst.len(), mask.len() * 2);
    let alpha_v = _mm256_set1_epi32(alpha as i32);
    let src_v = _mm256_set1_epi32(src as i32);
    let n = mask.len();
    let mut x = 0usize;
    while x + 16 <= n {
        // In-bounds: x + 16 <= n covers the mask read; dst accesses span
        // bytes [2x, 2x + 32) <= 2n == dst.len().
        let m = _mm_loadu_si128(mask.as_ptr().add(x).cast());
        if _mm_testz_si128(m, m) == 0 {
            let base = dst.as_mut_ptr().add(2 * x);
            blend8_u16(base, _mm256_cvtepu8_epi32(m), alpha_v, src_v);
            blend8_u16(
                base.add(16),
                _mm256_cvtepu8_epi32(_mm_srli_si128::<8>(m)),
                alpha_v,
                src_v,
            );
        }
        x += 16;
    }
    if !mask[x..].iter().all(|&m| m == 0) {
        super::blend_span::<SampleU16Le>(&mut dst[2 * x..], &mask[x..], src, alpha, 2);
    }
}

/// Eight u16le pixels through `((0x0001_0001 - a) * dst + a * src) >> 16`
/// in u32 lanes (numerator <= u32::MAX exactly), truncated back to u16.
/// `coverage` carries the mask (or pooled coverage) zero-extended to u32.
/// The packus stage is exact: every lane is <= 0xFFFF after the shift.
///
/// # Safety
/// Requires AVX2; `dst` must be valid for a 16-byte read and write.
#[inline]
#[target_feature(enable = "avx2")]
unsafe fn blend8_u16(dst: *mut u8, coverage: __m256i, alpha_v: __m256i, src_v: __m256i) {
    let a = _mm256_mullo_epi32(coverage, alpha_v);
    let inv = _mm256_sub_epi32(_mm256_set1_epi32(0x0001_0001), a);
    let d = _mm256_cvtepu16_epi32(_mm_loadu_si128(dst.cast()));
    let num = _mm256_add_epi32(_mm256_mullo_epi32(inv, d), _mm256_mullo_epi32(a, src_v));
    let b = _mm256_srli_epi32::<16>(num);
    // u16 lanes: [b0-3, b0-3 | b4-7, b4-7]; pick quadwords 0 and 2.
    let packed = _mm256_packus_epi32(b, b);
    let packed = _mm256_permute4x64_epi64::<0b0000_1000>(packed);
    _mm_storeu_si128(dst.cast(), _mm256_castsi256_si128(packed));
}

/// One row of the pooled u8 apply: 16 output pixels per iteration. Parity
/// order matters: coverage is `sum >> shift` FIRST (u16 lanes), then
/// widened and multiplied by alpha — exactly the scalar
/// `(u32::from(sum) >> shift) * alpha` (identical for u16 sums).
///
/// # Safety
/// Requires AVX2; `dst.len() == sums.len()`.
#[target_feature(enable = "avx2")]
unsafe fn apply_row_u8(dst: &mut [u8], sums: &[u16], src: u32, alpha: u32, shift: u32) {
    debug_assert_eq!(dst.len(), sums.len());
    let alpha_v = _mm256_set1_epi32(alpha as i32);
    let src_v = _mm256_set1_epi32(src as i32);
    let shift_v = _mm_cvtsi32_si128(shift as i32);
    // Word order fix-up for the final pack: [b0l b1l b0l b1l | b0h b1h ..]
    // -> words [0,4,1,5] = b0l b0h b1l b1h (only the low xmm is stored).
    let regroup = _mm256_setr_epi32(0, 4, 1, 5, 0, 0, 0, 0);
    let n = sums.len();
    let mut x = 0usize;
    while x + 16 <= n {
        // In-bounds: x + 16 <= n covers the 32-byte sums read and the
        // 16-byte dst read/write.
        let s = _mm256_loadu_si256(sums.as_ptr().add(x).cast());
        if _mm256_testz_si256(s, s) == 0 {
            let cov = _mm256_srl_epi16(s, shift_v);
            let c0 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(cov));
            let c1 = _mm256_cvtepu16_epi32(_mm256_extracti128_si256::<1>(cov));
            let d = _mm_loadu_si128(dst.as_ptr().add(x).cast());
            let b0 = blend8_u8(c0, d, alpha_v, src_v);
            let b1 = blend8_u8(c1, _mm_srli_si128::<8>(d), alpha_v, src_v);
            let p = _mm256_packus_epi32(b0, b1);
            let bytes = _mm256_packus_epi16(p, p);
            let bytes = _mm256_permutevar8x32_epi32(bytes, regroup);
            _mm_storeu_si128(
                dst.as_mut_ptr().add(x).cast(),
                _mm256_castsi256_si128(bytes),
            );
        }
        x += 16;
    }
    if !sums[x..].iter().all(|&s| s == 0) {
        super::apply_sums_span::<SampleU8>(&mut dst[x..], &sums[x..], src, alpha, shift, 1);
    }
}

/// One row of the pooled u16 apply at `pixel_step == 2`: 16 output pixels
/// per iteration, coverage computed sum-shift-first like the scalar path.
///
/// # Safety
/// Requires AVX2; `dst.len() == sums.len() * 2`.
#[target_feature(enable = "avx2")]
unsafe fn apply_row_u16(dst: &mut [u8], sums: &[u16], src: u32, alpha: u32, shift: u32) {
    debug_assert_eq!(dst.len(), sums.len() * 2);
    let alpha_v = _mm256_set1_epi32(alpha as i32);
    let src_v = _mm256_set1_epi32(src as i32);
    let shift_v = _mm_cvtsi32_si128(shift as i32);
    let n = sums.len();
    let mut x = 0usize;
    while x + 16 <= n {
        // In-bounds: x + 16 <= n covers the 32-byte sums read; dst accesses
        // span bytes [2x, 2x + 32) <= 2n == dst.len().
        let s = _mm256_loadu_si256(sums.as_ptr().add(x).cast());
        if _mm256_testz_si256(s, s) == 0 {
            let cov = _mm256_srl_epi16(s, shift_v);
            let base = dst.as_mut_ptr().add(2 * x);
            blend8_u16(
                base,
                _mm256_cvtepu16_epi32(_mm256_castsi256_si128(cov)),
                alpha_v,
                src_v,
            );
            blend8_u16(
                base.add(16),
                _mm256_cvtepu16_epi32(_mm256_extracti128_si256::<1>(cov)),
                alpha_v,
                src_v,
            );
        }
        x += 16;
    }
    if !sums[x..].iter().all(|&s| s == 0) {
        super::apply_sums_span::<SampleU16Le>(&mut dst[2 * x..], &sums[x..], src, alpha, shift, 2);
    }
}

/// Interior pair-sums: 16 output sums per iteration via `maddubs` against
/// all-ones (adjacent u8 pairs never straddle a 128-bit lane, and 4 * 255
/// stays far below the i16 saturation bound, so the sums are exact).
///
/// # Safety
/// Requires AVX2; `top.len() == span.len() * 2` (and `bot` likewise when
/// `TWO_ROWS`).
#[target_feature(enable = "avx2")]
unsafe fn pair_sums_row<const TWO_ROWS: bool>(span: &mut [u16], top: &[u8], bot: &[u8]) {
    debug_assert_eq!(top.len(), span.len() * 2);
    if TWO_ROWS {
        debug_assert_eq!(bot.len(), span.len() * 2);
    }
    let ones = _mm256_set1_epi8(1);
    let n = span.len();
    let mut x = 0usize;
    while x + 16 <= n {
        // In-bounds: x + 16 <= n covers the 32-byte mask reads (2 * 16
        // bytes at offset 2x <= 2n - 32) and the 32-byte span store.
        let t = _mm256_loadu_si256(top.as_ptr().add(2 * x).cast());
        let mut s = _mm256_maddubs_epi16(t, ones);
        if TWO_ROWS {
            let b = _mm256_loadu_si256(bot.as_ptr().add(2 * x).cast());
            s = _mm256_add_epi16(s, _mm256_maddubs_epi16(b, ones));
        }
        _mm256_storeu_si256(span.as_mut_ptr().add(x).cast(), s);
        x += 16;
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

    /// Direct AVX2 kernels vs `blend_direct` on identical rows: all alpha
    /// edge values, mask bytes 0..=255, widths through body + tail, u8 and
    /// u16 dst edge patterns.
    #[test]
    fn direct_rows_match_scalar_bitexact() {
        if !enabled() {
            eprintln!("skipping: AVX2 unavailable on this CPU");
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
    /// the AVX2 wrappers vs the scalar kernel.
    #[test]
    fn direct_multi_row_geometry_matches_scalar() {
        if !enabled() {
            eprintln!("skipping: AVX2 unavailable on this CPU");
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
            eprintln!("skipping: AVX2 unavailable on this CPU");
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
            eprintln!("skipping: AVX2 unavailable on this CPU");
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
