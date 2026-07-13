use super::*;
use crate::wgpu_filter::tests::{drive, init_filter, make_yuv_frame_with, plane_to_vec};
use crate::wgpu_filter::WgpuFrameFilter;
use ffmpeg_sys_next::AVPixelFormat;

/// Every built-in effect shader must parse and validate offline — a WGSL
/// error would otherwise surface only at pipeline creation on a live GPU
/// device, which no CI lane has. Same naga wgpu compiles them with at
/// runtime.
#[test]
fn effect_shaders_parse_and_validate() {
    let beauty_fast = beauty::beauty_shader(BeautyQuality::Fast);
    let beauty_balanced = beauty::beauty_shader(BeautyQuality::Balanced);
    let modules: [(&str, String); 7] = [
        ("adjust", effect_module(adjust::ADJUST_BODY)),
        ("beauty_fast", beauty_fast),
        ("beauty_balanced", beauty_balanced),
        ("pixelate", effect_module(pixelate::PIXELATE_BODY)),
        ("sharpen", effect_module(sharpen::SHARPEN_BODY)),
        ("soft_blur", effect_module(soft_blur::SOFT_BLUR_BODY)),
        ("transform", effect_module(transform::TRANSFORM_BODY)),
    ];
    for (name, source) in modules {
        let module = naga::front::wgsl::parse_str(&source)
            .unwrap_or_else(|e| panic!("{name}: WGSL parse failed:\n{e}"));
        naga::valid::Validator::new(
            naga::valid::ValidationFlags::all(),
            naga::valid::Capabilities::all(),
        )
        .validate(&module)
        .unwrap_or_else(|e| panic!("{name}: WGSL validation failed: {e:?}"));
    }
}

/// The host params structs feed WGSL uniform structs byte-for-byte: all
/// scalar (f32/u32) members, so both sides pack tightly — but the struct
/// size must be a multiple of 16 (WGSL rounds a uniform struct's size up,
/// and the host must not disagree).
#[test]
fn params_structs_are_uniform_compatible() {
    assert_eq!(std::mem::size_of::<AdjustParams>(), 32);
    assert_eq!(std::mem::size_of::<BeautyParams>(), 16);
    assert_eq!(std::mem::size_of::<PixelateParams>(), 16);
    assert_eq!(std::mem::size_of::<SharpenParams>(), 16);
    assert_eq!(std::mem::size_of::<SoftBlurParams>(), 16);
    assert_eq!(std::mem::size_of::<TransformParams>(), 32);
}

/// `Default` must be a *neutral or mild* preset — a filter built with
/// defaults must never obviously distort the image.
#[test]
fn default_params_are_neutral_or_mild() {
    let a = AdjustParams::default();
    assert_eq!(
        (a.brightness, a.contrast, a.saturation, a.exposure_ev),
        (0.0, 1.0, 1.0, 0.0)
    );
    assert_eq!(
        (a.gamma, a.vibrance, a.temperature, a.tint),
        (1.0, 0.0, 0.0, 0.0)
    );

    let t = TransformParams::default();
    assert_eq!((t.rotate, t.scale_x, t.scale_y), (0.0, 1.0, 1.0));
    assert_eq!((t.translate_x, t.translate_y), (0.0, 0.0));
    assert_eq!((t.mirror_x, t.mirror_y), (0, 0));

    let b = BeautyParams::default();
    assert!(b.smooth <= 0.6 && b.whiten <= 0.3 && b.brighten <= 0.2);

    assert!(SharpenParams::default().amount <= 1.0);
    assert_eq!(PixelateParams::default().block_size, 16.0);
    assert!(SoftBlurParams::default().strength <= 0.8);
}

/// Builders must produce filters without touching a GPU (validation and
/// shader assembly are CPU-side; device work happens in `init`).
#[test]
fn builders_build_without_a_gpu() {
    assert!(adjust(AdjustParams::default()).build().is_ok());
    assert!(beauty_lite(BeautyParams::default())
        .quality(BeautyQuality::Fast)
        .build()
        .is_ok());
    assert!(portrait().build().is_ok());
    assert!(pixelate(PixelateParams::with_block_size(8.0))
        .build()
        .is_ok());
    assert!(sharpen(SharpenParams::with_amount(1.0)).build().is_ok());
    assert!(soft_blur(SoftBlurParams::privacy()).build().is_ok());
    assert!(transform(TransformParams::mirrored())
        .output_size(640, 360)
        .frames_in_flight(1)
        .build()
        .is_ok());
}

// --- GPU runtime tests (skipped when no adapter is present) ---

fn gradient_frame(w: i32, h: i32) -> ffmpeg_next::Frame {
    make_yuv_frame_with(
        w,
        h,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |c, r| ((c * 3 + r * 5) % 220 + 18) as u8,
        |c, r| (((c * 7) % 200 + 28) as u8, ((r * 9) % 200 + 28) as u8),
    )
}

/// Drives `filter` and the plain identity filter over the same input and
/// asserts every output plane matches within `tol` code values.
fn assert_matches_identity(mut filter: WgpuFrameFilter, tol: i32, what: &str) {
    let mut identity = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut identity) {
        return;
    }
    assert!(init_filter(&mut filter));
    let (w, h) = (128usize, 96usize);
    let a = drive(&mut identity, vec![gradient_frame(128, 96)], 1)
        .pop()
        .unwrap();
    let b = drive(&mut filter, vec![gradient_frame(128, 96)], 1)
        .pop()
        .unwrap();
    for (plane, pw, ph) in [(0, w, h), (1, w / 2, h / 2), (2, w / 2, h / 2)] {
        let pa = plane_to_vec(&a, plane, pw, ph);
        let pb = plane_to_vec(&b, plane, pw, ph);
        let max = pa
            .iter()
            .zip(&pb)
            .map(|(x, y)| (*x as i32 - *y as i32).abs())
            .max()
            .unwrap();
        assert!(
            max <= tol,
            "{what}: plane {plane} differs by {max} (> {tol})"
        );
    }
}

#[test]
fn adjust_defaults_match_the_identity_pipeline() {
    // Neutral defaults must pass frames through; only `pow(c, 1.0)` (an
    // exp2/log2 roundtrip on GPUs) may wobble the last bit or two.
    let filter = adjust(AdjustParams::default())
        .build()
        .unwrap()
        .into_inner();
    assert_matches_identity(filter, 2, "adjust defaults");
}

#[test]
fn sharpen_zero_amount_matches_identity() {
    let filter = sharpen(SharpenParams::with_amount(0.0))
        .build()
        .unwrap()
        .into_inner();
    assert_matches_identity(filter, 1, "sharpen amount=0");
}

#[test]
fn sharpen_increases_edge_contrast() {
    let mut filter = sharpen(SharpenParams::with_amount(2.0))
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    // A vertical luma step edge: unsharp masking must overshoot on both
    // sides (darker dark, brighter bright), increasing the local contrast.
    let step_frame = || {
        make_yuv_frame_with(
            64,
            48,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            |c, _| if c < 32 { 80 } else { 170 },
            |_, _| (128, 128),
        )
    };
    let out = drive(&mut filter, vec![step_frame()], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let row = 24usize;
    // With radius 1 the overshoot lives in the pixels adjacent to the
    // edge (their taps straddle it); pixels further away see a flat
    // neighborhood and keep their value.
    let dark_side = y[row * 64 + 31] as i32; // last dark column
    let bright_side = y[row * 64 + 32] as i32; // first bright column
    let sharpened_contrast = bright_side - dark_side;
    assert!(
        sharpened_contrast > (170 - 80) + 10,
        "edge contrast must overshoot the input step (got {sharpened_contrast})"
    );
    // Flat regions away from the edge stay put (no ringing across the frame).
    assert!((y[row * 64 + 8] as i32 - 80).abs() <= 3);
    assert!((y[row * 64 + 56] as i32 - 170).abs() <= 3);
}

#[test]
fn soft_blur_zero_strength_matches_identity() {
    let params = SoftBlurParams {
        strength: 0.0,
        ..SoftBlurParams::default()
    };
    let filter = soft_blur(params).build().unwrap().into_inner();
    assert_matches_identity(filter, 1, "soft_blur strength=0");
}

#[test]
fn soft_blur_softens_edge_contrast() {
    // The strong named preset: radius 12, strength 1.0 (full blur mix).
    let mut filter = soft_blur(SoftBlurParams::privacy())
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    // The same vertical luma step edge the sharpen oracle uses. A disc
    // blur must pull both sides toward the mean, SHRINKING the step across
    // the edge — the opposite direction of unsharp masking's overshoot.
    let step_frame = || {
        make_yuv_frame_with(
            64,
            48,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            |c, _| if c < 32 { 80 } else { 170 },
            |_, _| (128, 128),
        )
    };
    let out = drive(&mut filter, vec![step_frame()], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let row = 24usize;
    // For an edge-adjacent pixel, 5 of the 12 ring taps (weight 5.25 of
    // wsum 14) land across the edge regardless of radius, moving each side
    // 3/8 of the step toward the other: the 90-step collapses to ~22.
    let dark_side = y[row * 64 + 31] as i32; // last dark column
    let bright_side = y[row * 64 + 32] as i32; // first bright column
    let blurred_contrast = bright_side - dark_side;
    assert!(
        blurred_contrast <= (170 - 80) / 2,
        "edge contrast must shrink well below the input step (got {blurred_contrast})"
    );
    // The blur redistributes, it must not shift levels: columns further
    // from the edge than the outer tap ring (10.4 px) average a flat
    // neighborhood and keep their value.
    assert!((y[row * 64 + 8] as i32 - 80).abs() <= 3);
    assert!((y[row * 64 + 56] as i32 - 170).abs() <= 3);
}

#[test]
fn transform_identity_matches_identity() {
    let filter = transform(TransformParams::default())
        .build()
        .unwrap()
        .into_inner();
    assert_matches_identity(filter, 1, "transform identity");
}

#[test]
fn adjust_live_update_desaturates() {
    let built = adjust(AdjustParams::default()).build().unwrap();
    let handle = built.params_handle(); // typed: no turbofish, bound at build
    let mut filter = built.into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let colored = || {
        make_yuv_frame_with(
            64,
            48,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            |_, _| 120,
            |_, _| (90, 170),
        )
    };
    let first = drive(&mut filter, vec![colored()], 1).pop().unwrap();
    let u = plane_to_vec(&first, 1, 32, 24);
    assert!(
        u.iter().all(|&v| (v as i32 - 128).abs() > 10),
        "defaults must keep the input's chroma"
    );

    handle.update(|p| p.saturation = 0.0);
    let second = drive(&mut filter, vec![colored()], 1).pop().unwrap();
    for plane in [1, 2] {
        for &v in &plane_to_vec(&second, plane, 32, 24) {
            assert!(
                (v as i32 - 128).abs() <= 2,
                "saturation=0 must produce neutral chroma, got {v}"
            );
        }
    }
}

#[test]
fn transform_positive_rotation_is_counterclockwise() {
    let params = TransformParams {
        rotate: std::f32::consts::FRAC_PI_2,
        ..TransformParams::default()
    };
    let mut filter = transform(params).build().unwrap().into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    // Square frame, left half dark / right half bright. Rotating the image
    // 90° counterclockwise (as the viewer sees it) carries the bright right
    // edge to the TOP; a clockwise rotation would carry it to the bottom.
    let input = make_yuv_frame_with(
        64,
        64,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |c, _| if c < 32 { 60 } else { 200 },
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 64);
    for col in [8usize, 32, 56] {
        assert!(
            (y[8 * 64 + col] as i32 - 200).abs() <= 3,
            "top row must show the source's bright right edge (CCW)"
        );
        assert!(
            (y[56 * 64 + col] as i32 - 60).abs() <= 3,
            "bottom row must show the source's dark left edge (CCW)"
        );
    }
}

#[test]
fn transform_mirror_swaps_left_and_right() {
    let mut filter = transform(TransformParams::mirrored())
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |c, _| if c < 32 { 60 } else { 200 },
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    for row in [0usize, 20, 47] {
        // Interior columns, away from the seam and the frame edge.
        assert!((y[row * 64 + 8] as i32 - 200).abs() <= 3);
        assert!((y[row * 64 + 56] as i32 - 60).abs() <= 3);
    }
}

#[test]
fn pixelate_flattens_blocks() {
    let mut filter = pixelate(PixelateParams::with_block_size(16.0))
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |c, r| (30 + c * 3 + r) as u8,
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    for by in 0..3usize {
        for bx in 0..4usize {
            // Every pixel of a block equals its center sample.
            let base = y[(by * 16 + 1) * 64 + bx * 16 + 1] as i32;
            for dy in [3usize, 8, 14] {
                for dx in [3usize, 8, 14] {
                    let v = y[(by * 16 + dy) * 64 + bx * 16 + dx] as i32;
                    assert!((v - base).abs() <= 1, "block ({bx},{by}) not flat");
                }
            }
        }
    }
    // The x-gradient steps 48 luma per block, so neighbors must differ.
    let left = y[8 * 64 + 8] as i32;
    let right = y[8 * 64 + 24] as i32;
    assert!((right - left).abs() >= 30, "blocks unexpectedly merged");
}

#[test]
fn pixelate_oversized_block_samples_the_frame_center() {
    // One block larger than the whole frame: its visible region IS the
    // frame, so every pixel must take the frame's center value — not the
    // bottom-right texel that clamping a nominal out-of-frame center hits.
    let mut filter = pixelate(PixelateParams::with_block_size(256.0))
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        100,
        100,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        // Distinct center vs corner: value = 200 inside the middle 2x2, 60
        // at the bottom-right corner, 120 elsewhere.
        |c, r| {
            if (49..=50).contains(&c) && (49..=50).contains(&r) {
                200
            } else if c == 99 && r == 99 {
                60
            } else {
                120
            }
        },
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 100, 100);
    for (px, py) in [(0usize, 0usize), (50, 50), (99, 99), (10, 80)] {
        assert!(
            (y[py * 100 + px] as i32 - 200).abs() <= 1,
            "({px},{py}) must hold the frame-center sample, got {}",
            y[py * 100 + px]
        );
    }
}

#[test]
fn pixelate_block_size_one_resizes_smoothly() {
    // block_size=1 is documented as a pass-through: combined with output
    // resizing it must keep bilinear interpolation (a gradient stays a
    // gradient), not snap to nearest-neighbor.
    let mut filter = pixelate(PixelateParams::with_block_size(1.0))
        .output_size(32, 24)
        .build()
        .unwrap()
        .into_inner();
    let mut identity = WgpuFrameFilter::builder()
        .shader_wgsl(crate::wgpu_filter::shaders::IDENTITY_FS)
        .output_size(32, 24)
        .build()
        .unwrap();
    if !init_filter(&mut identity) {
        return;
    }
    assert!(init_filter(&mut filter));
    let a = drive(&mut identity, vec![gradient_frame(64, 48)], 1)
        .pop()
        .unwrap();
    let b = drive(&mut filter, vec![gradient_frame(64, 48)], 1)
        .pop()
        .unwrap();
    let ya = plane_to_vec(&a, 0, 32, 24);
    let yb = plane_to_vec(&b, 0, 32, 24);
    let max = ya
        .iter()
        .zip(&yb)
        .map(|(x, y)| (*x as i32 - *y as i32).abs())
        .max()
        .unwrap();
    assert!(
        max <= 1,
        "block_size=1 + resize must match the identity resize (max diff {max})"
    );
}

#[test]
fn beauty_keeps_constant_frames_uniform() {
    // Constant skin-toned input: smoothing must not invent gradients, and
    // whiten/brighten shift every pixel by the same amount.
    let mut filter = beauty_lite(BeautyParams::default())
        .quality(BeautyQuality::Fast)
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |_, _| 150,
        |_, _| (110, 150),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    for (plane, pw, ph) in [(0, 64usize, 48usize), (1, 32, 24), (2, 32, 24)] {
        let pv = plane_to_vec(&out, plane, pw, ph);
        let mn = *pv.iter().min().unwrap() as i32;
        let mx = *pv.iter().max().unwrap() as i32;
        assert!(mx - mn <= 1, "plane {plane} not uniform: {mn}..{mx}");
    }
}

/// Luma of the noisy-skin oracle frame: a 150-code base tone plus
/// deterministic integer-hash noise in -6..=6 (no rand dependency). The
/// +/-6 range keeps |luma_c - luma_s| under the shader's 0.05 edge_keep
/// knee, so smoothing stays fully active.
fn noisy_skin_luma(c: usize, r: usize) -> u8 {
    let mut x = (c as u32)
        .wrapping_mul(1_664_525)
        .wrapping_add((r as u32).wrapping_mul(1_013_904_223))
        .wrapping_add(0x9E37_79B9);
    x ^= x >> 16;
    x = x.wrapping_mul(0x045D_9F3B);
    x ^= x >> 16;
    (150 + (x % 13) as i32 - 6) as u8
}

/// A 64x48 skin-cluster base tone with per-pixel luma noise. U=117/V=153
/// sit at the shader's skin-mask cluster center: with the limited-range
/// BT.601 convert this frame gets, skin_confidence recovers
/// cb=(U-128)/224=-0.049 and cr=(V-128)/224=0.112, i.e. the mask's
/// (-0.05, 0.11) target, and the tone's saturation (~0.24) is inside the
/// 0.08..0.5 band — so the mask is ~1 and smoothing is fully active.
fn noisy_skin_frame() -> ffmpeg_next::Frame {
    make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        noisy_skin_luma,
        |_, _| (117, 153),
    )
}

/// Materializes `f(col, row)` over the noisy-skin frame's interior (8 px
/// in from every edge, away from the sampler's edge clamp).
fn interior_vals(f: impl Fn(usize, usize) -> i32) -> Vec<i32> {
    let mut vals = Vec::with_capacity(32 * 48);
    for r in 8..40 {
        for c in 8..56 {
            vals.push(f(c, r));
        }
    }
    vals
}

fn mean(vals: &[i32]) -> f64 {
    vals.iter().map(|&v| v as f64).sum::<f64>() / vals.len() as f64
}

fn mean_abs_dev(vals: &[i32]) -> f64 {
    let m = mean(vals);
    vals.iter().map(|&v| (v as f64 - m).abs()).sum::<f64>() / vals.len() as f64
}

#[test]
fn beauty_lite_smooths_skin_noise() {
    let mut filter = beauty_lite(BeautyParams {
        smooth: 1.0,
        whiten: 0.0,
        brighten: 0.0,
        detail_preserve: 0.3,
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![noisy_skin_frame()], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);

    let in_mad = mean_abs_dev(&interior_vals(|c, r| noisy_skin_luma(c, r) as i32));
    let out_mad = mean_abs_dev(&interior_vals(|c, r| y[r * 64 + c] as i32));
    assert!(
        in_mad > 2.5,
        "test bug: the noise pattern must actually be noisy (in {in_mad:.2})"
    );
    assert!(
        out_mad < in_mad * 0.6,
        "smoothing must cut skin luma noise substantially \
         (in {in_mad:.2}, out {out_mad:.2})"
    );
    // detail_preserve hands back part of the removed high frequency and
    // the 13-tap kernel keeps a noise residual: the region must retain
    // texture, not flatten to a plateau (which would read ~0 here).
    assert!(
        out_mad > 0.3,
        "output must not collapse to a constant region (out {out_mad:.2})"
    );
}

#[test]
fn portrait_smooths_and_brightens_more_than_default_beauty() {
    // `portrait()` is documented as a *stronger* fused preset than the
    // mild `BeautyParams::default()`; this pins that ordering. Degrading
    // `portrait()` to `beauty_lite(BeautyParams::default())` keeps every
    // other test green — this one must fail.
    let mut default_beauty = beauty_lite(BeautyParams::default())
        .build()
        .unwrap()
        .into_inner();
    let mut portrait_beauty = portrait().build().unwrap().into_inner();
    if !init_filter(&mut default_beauty) {
        return;
    }
    assert!(init_filter(&mut portrait_beauty));

    let out_d = drive(&mut default_beauty, vec![noisy_skin_frame()], 1)
        .pop()
        .unwrap();
    let out_p = drive(&mut portrait_beauty, vec![noisy_skin_frame()], 1)
        .pop()
        .unwrap();
    let yd = plane_to_vec(&out_d, 0, 64, 48);
    let yp = plane_to_vec(&out_p, 0, 64, 48);
    let d_vals = interior_vals(|c, r| yd[r * 64 + c] as i32);
    let p_vals = interior_vals(|c, r| yp[r * 64 + c] as i32);

    let in_mad = mean_abs_dev(&interior_vals(|c, r| noisy_skin_luma(c, r) as i32));
    let d_mad = mean_abs_dev(&d_vals);
    let p_mad = mean_abs_dev(&p_vals);
    let d_mean = mean(&d_vals);
    let p_mean = mean(&p_vals);
    assert!(
        in_mad > 2.5,
        "test bug: the noise pattern must actually be noisy (in {in_mad:.2})"
    );
    // The comparison is meaningless if the mild preset didn't smooth.
    assert!(
        d_mad < in_mad * 0.9,
        "default preset must smooth at all (in {in_mad:.2}, default {d_mad:.2})"
    );
    // Net high-frequency attenuation is smooth*(1-detail_preserve) at
    // mask~1: default 0.5*0.7 = 0.35, portrait 0.65*0.65 = 0.4225, so
    // portrait keeps ~0.58 of the noise vs ~0.65 — about 0.9x the default
    // output's MAD. 0.97 splits that from the 1.0 a degraded (identical)
    // preset would measure.
    assert!(
        p_mad < d_mad * 0.97,
        "portrait must smooth strictly harder than default params \
         (default {d_mad:.2}, portrait {p_mad:.2})"
    );
    // Whiten/brighten lift every channel by k*c*(1-c), k >= 0, monotone
    // in k for c in (0,1) — no highlight-compression non-monotonicity on
    // this frame's mid-tones (~0.53..0.77). At mask~1 the fused lift is
    // whiten*0.35 + brighten*0.25: default 0.095, portrait 0.1725, about
    // +4 luma codes of separation; require half of it.
    assert!(
        p_mean > d_mean + 2.0,
        "portrait must whiten/brighten strictly above default params \
         (default mean {d_mean:.2}, portrait mean {p_mean:.2})"
    );
}
