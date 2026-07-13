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
    let modules: [(&str, String); 14] = [
        ("adjust", effect_module(adjust::ADJUST_BODY)),
        ("beauty_fast", beauty_fast),
        ("beauty_balanced", beauty_balanced),
        ("chroma_key", effect_module(chroma_key::CHROMA_KEY_BODY)),
        ("fisheye", effect_module(fisheye::FISHEYE_BODY)),
        ("magnifier", effect_module(magnifier::MAGNIFIER_BODY)),
        ("pixelate", effect_module(pixelate::PIXELATE_BODY)),
        ("sharpen", effect_module(sharpen::SHARPEN_BODY)),
        ("soft_blur", effect_module(soft_blur::SOFT_BLUR_BODY)),
        ("soul", effect_module(soul::SOUL_BODY)),
        ("sway", effect_module(sway::SWAY_BODY)),
        ("swirl", effect_module(swirl::SWIRL_BODY)),
        ("transform", effect_module(transform::TRANSFORM_BODY)),
        ("wave", effect_module(wave::WAVE_BODY)),
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
    assert_eq!(std::mem::size_of::<ChromaKeyParams>(), 48);
    assert_eq!(std::mem::size_of::<FisheyeParams>(), 16);
    assert_eq!(std::mem::size_of::<MagnifierParams>(), 16);
    assert_eq!(std::mem::size_of::<PixelateParams>(), 16);
    assert_eq!(std::mem::size_of::<SharpenParams>(), 16);
    assert_eq!(std::mem::size_of::<SoftBlurParams>(), 16);
    assert_eq!(std::mem::size_of::<SoulParams>(), 16);
    assert_eq!(std::mem::size_of::<SwayParams>(), 16);
    assert_eq!(std::mem::size_of::<SwirlParams>(), 32);
    assert_eq!(std::mem::size_of::<TransformParams>(), 32);
    assert_eq!(std::mem::size_of::<WaveParams>(), 16);
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

    // The lens/motion effects are inherently visible; their defaults must
    // still be the documented moderate values, and the animated ones must
    // start from a subtle motion, not a violent one.
    let s = SoulParams::default();
    assert!(s.period > 0.0 && s.max_alpha <= 0.5 && s.max_scale <= 1.5);

    let w = SwayParams::default();
    assert!(w.x_amplitude <= 0.01 && w.y_amplitude <= 0.01);
    assert!(w.zoom_amplitude <= 0.05);

    let v = WaveParams::default();
    assert!(v.amplitude <= 0.01 && v.frequency <= 4.0);

    let sw = SwirlParams::default();
    assert_eq!((sw.center_x, sw.center_y, sw.speed), (0.5, 0.5, 0.0));
    assert!(sw.twist.abs() <= 4.0);

    let m = MagnifierParams::default();
    assert_eq!((m.center_x, m.center_y), (0.5, 0.5));
    assert!(m.magnification <= 2.0 && m.radius <= 0.3);

    assert!(FisheyeParams::default().strength <= 1.0);

    // Chroma key defaults must match the documented FFmpeg-scale values
    // and key pure green over black.
    let ck = ChromaKeyParams::default();
    assert_eq!((ck.key_r, ck.key_g, ck.key_b), (0.0, 1.0, 0.0));
    assert_eq!((ck.bg_r, ck.bg_g, ck.bg_b), (0.0, 0.0, 0.0));
    assert!(ck.similarity > 0.0 && ck.similarity <= 0.4);
    assert!(ck.blend <= 0.1);
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
    assert!(soul(SoulParams::default()).build().is_ok());
    assert!(sway(SwayParams::breathe()).build().is_ok());
    assert!(sway(SwayParams::handheld()).build().is_ok());
    assert!(wave(WaveParams::default()).build().is_ok());
    assert!(swirl(SwirlParams::tornado()).build().is_ok());
    assert!(magnifier(MagnifierParams::default()).build().is_ok());
    assert!(fisheye(FisheyeParams::with_strength(0.8)).build().is_ok());
    assert!(chroma_key(ChromaKeyParams::blue_screen()).build().is_ok());
    assert!(
        chroma_key(ChromaKeyParams::green_screen().with_background(0.0, 0.0, 1.0))
            .build()
            .is_ok()
    );
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
    let out = drive(&mut filter, vec![noisy_skin_frame()], 1)
        .pop()
        .unwrap();
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

// --- Lens / motion / keying effect oracles ---

/// Sets the frame's pts (the harness frames carry `time_base` 1/30, so
/// `play_time = pts / 30` seconds drives the animated effects).
fn with_pts(mut frame: ffmpeg_next::Frame, pts: i64) -> ffmpeg_next::Frame {
    unsafe { (*frame.as_mut_ptr()).pts = pts };
    frame
}

/// Vertical luma step edge at `edge_x` (fraction of the width): dark 60
/// left of it, bright 200 right of it, neutral chroma.
fn step_edge_frame(w: i32, h: i32, edge_x: f32) -> ffmpeg_next::Frame {
    let edge_col = (edge_x * w as f32) as usize;
    make_yuv_frame_with(
        w,
        h,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        move |c, _| if c < edge_col { 60 } else { 200 },
        |_, _| (128, 128),
    )
}

#[test]
fn soul_cycle_start_matches_identity() {
    // At the cycle start (pts=0 => progress 0) the ghost has scale 1 and
    // coincides with the image: mixing a pixel with itself is the identity
    // regardless of max_alpha.
    let filter = soul(SoulParams {
        max_alpha: 1.0,
        ..SoulParams::default()
    })
    .build()
    .unwrap()
    .into_inner();
    assert_matches_identity(filter, 1, "soul cycle start");
}

#[test]
fn soul_mid_cycle_blends_an_enlarged_ghost() {
    let mut filter = soul(SoulParams {
        period: 1.0,
        max_alpha: 0.4,
        max_scale: 1.25,
        _pad: 0.0,
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    // Mid-cycle (pts 15 of 30/s => t=0.5s => progress 0.5): alpha 0.2,
    // ghost scale 1.125. The input edge sits at x=0.25 (col 16 of 64);
    // the enlarged ghost's edge lands at 0.5 - 0.28125 = 0.219 (col 14).
    // Between the two edges the dark base blends with the bright ghost:
    // 60 + 0.2*(200-60) = 88.
    let input = with_pts(step_edge_frame(64, 48, 0.25), 15);
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let row = 24usize;
    for col in [14usize, 15] {
        let v = y[row * 64 + col] as i32;
        assert!(
            (75..=105).contains(&v),
            "ghost band col {col} must blend toward the bright ghost, got {v}"
        );
    }
    // Outside the band both layers agree, so the image is unchanged.
    assert!((y[row * 64 + 8] as i32 - 60).abs() <= 3, "far dark side");
    assert!(
        (y[row * 64 + 56] as i32 - 200).abs() <= 3,
        "far bright side"
    );
}

#[test]
fn sway_zero_amplitudes_match_identity() {
    // All amplitudes zero: the overscan formula must degenerate to exactly
    // 1 and the oscillation must vanish — bit-stable pass-through.
    let filter = sway(SwayParams {
        x_amplitude: 0.0,
        y_amplitude: 0.0,
        zoom_amplitude: 0.0,
        speed: 5.0,
    })
    .build()
    .unwrap()
    .into_inner();
    assert_matches_identity(filter, 1, "sway zero amplitudes");
}

#[test]
fn sway_translation_shifts_the_edge_through_the_overscan() {
    // Pure horizontal travel at the range maximum (xa = 0.05): overscan
    // is 1/0.9, so sampling is c = 0.9·x + 0.1 at the sin = 1 peak
    // (pts 15, speed pi => t·speed = pi/2). The edge at x = 0.25 lands at
    // output x = 1/6 (col ~10.7). Col 12 samples c = 0.276 — bright.
    // This pins BOTH terms: dropping the translation samples c = 0.226
    // and dropping the overscan samples c = 0.245 — dark either way.
    let params = SwayParams {
        x_amplitude: 0.05,
        y_amplitude: 0.0,
        zoom_amplitude: 0.0,
        speed: std::f32::consts::PI,
    };
    let mut filter = sway(params).build().unwrap().into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(
        &mut filter,
        vec![with_pts(step_edge_frame(64, 48, 0.25), 15)],
        1,
    )
    .pop()
    .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let row = 24usize;
    assert!(
        y[row * 64 + 12] as i32 >= 170,
        "col 12 must go bright once the edge translated to col ~11, got {}",
        y[row * 64 + 12]
    );
    assert!(
        (y[row * 64 + 9] as i32 - 60).abs() <= 3,
        "col 9 stays left of the translated edge, got {}",
        y[row * 64 + 9]
    );
    // The right border must show genuine in-frame content: with the
    // overscan the sampling window peaks at exactly c = 1.0.
    assert!(
        (y[row * 64 + 62] as i32 - 200).abs() <= 3,
        "right border keeps in-frame bright content, got {}",
        y[row * 64 + 62]
    );
}

#[test]
fn sway_zoom_pulses_and_never_leaves_the_frame() {
    // Pure zoom pulse (breathe), amplitude 0.2, speed pi rad/s: overscan
    // is 1/0.8 = 1.25, so the scale swings between 1.0 and 1.5.
    let params = SwayParams {
        x_amplitude: 0.0,
        y_amplitude: 0.0,
        zoom_amplitude: 0.2,
        speed: std::f32::consts::PI,
    };
    let mut filter = sway(params).build().unwrap().into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    // Peak zoom at t=0.5s (sin = 1, scale 1.5): the edge at x=0.25
    // (col 16) moves out to 0.5 - 0.25*1.5 = 0.125 (col 8).
    let out = drive(
        &mut filter,
        vec![with_pts(step_edge_frame(64, 48, 0.25), 15)],
        1,
    )
    .pop()
    .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let row = 24usize;
    assert!(
        (y[row * 64 + 6] as i32 - 60).abs() <= 3,
        "left of the zoomed edge stays dark"
    );
    assert!(
        y[row * 64 + 10] as i32 >= 170,
        "col 10 must be bright once the edge zoomed out to col 8, got {}",
        y[row * 64 + 10]
    );

    // Trough at t=1.5s (sin = -1): the overscan and the pulse cancel to
    // scale exactly 1 — the frame passes through unchanged. This pins the
    // sampling window never exceeding the frame (scale >= 1 at all phases).
    let out = drive(
        &mut filter,
        vec![with_pts(step_edge_frame(64, 48, 0.25), 45)],
        1,
    )
    .pop()
    .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    assert!(
        (y[row * 64 + 14] as i32 - 60).abs() <= 3,
        "trough phase: col 14 keeps the input's dark value"
    );
    assert!(
        (y[row * 64 + 17] as i32 - 200).abs() <= 3,
        "trough phase: col 17 keeps the input's bright value"
    );
}

#[test]
fn wave_zero_amplitude_matches_identity() {
    let filter = wave(WaveParams {
        amplitude: 0.0,
        ..WaveParams::default()
    })
    .build()
    .unwrap()
    .into_inner();
    assert_matches_identity(filter, 1, "wave amplitude=0");
}

#[test]
fn wave_bends_the_edge_by_the_sine_of_the_row() {
    // Static phase (pts=0), amplitude 0.03, frequency 2: the x displacement
    // is cos(y*4pi)*0.03, so the vertical edge bends LEFT (~2 px) on rows
    // where the cosine is +1 and RIGHT on rows where it is -1. Luma varies
    // only with x, so the y displacement is a no-op and the sine SHAPE is
    // pinned by the two opposite rows.
    let mut filter = wave(WaveParams {
        amplitude: 0.03,
        frequency: 2.0,
        speed: 1.5,
        _pad: 0.0,
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![step_edge_frame(64, 48, 0.5)], 1)
        .pop()
        .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    // Row 24 (y=0.51, cos=+0.99): sampling shifts +0.03, the edge appears
    // at x=0.47, so col 31 (input: dark) flips bright.
    assert!(
        y[24 * 64 + 31] as i32 >= 170,
        "row 24 col 31 must flip bright (edge bent left), got {}",
        y[24 * 64 + 31]
    );
    // Row 12 (y=0.26, cos=-0.99): the edge appears at x=0.53, so col 32
    // (input: bright) flips dark.
    assert!(
        y[12 * 64 + 32] as i32 <= 90,
        "row 12 col 32 must flip dark (edge bent right), got {}",
        y[12 * 64 + 32]
    );
    // Far columns are flat on both sides of every bent edge position.
    assert!((y[24 * 64 + 8] as i32 - 60).abs() <= 3);
    assert!((y[12 * 64 + 56] as i32 - 200).abs() <= 3);
}

#[test]
fn wave_time_phase_travels_the_ripple() {
    // Same geometry as the static test but half a period later: speed 2π
    // at t = 0.5 s (pts 15) adds a π phase, flipping the displacement
    // sign on every row. Row 24 bent LEFT at t = 0 — now it bends RIGHT
    // (sampling shifts −0.03, the edge appears at x ≈ 0.53): col 32
    // (input: bright) flips dark. Row 12 flips the other way: col 31
    // (input: dark) goes bright. Freezing the time term re-renders the
    // static phase and fails both.
    let mut filter = wave(WaveParams {
        amplitude: 0.03,
        frequency: 2.0,
        speed: std::f32::consts::TAU,
        _pad: 0.0,
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(
        &mut filter,
        vec![with_pts(step_edge_frame(64, 48, 0.5), 15)],
        1,
    )
    .pop()
    .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    assert!(
        y[24 * 64 + 32] as i32 <= 90,
        "row 24 col 32 must flip dark half a period later, got {}",
        y[24 * 64 + 32]
    );
    assert!(
        y[12 * 64 + 31] as i32 >= 170,
        "row 12 col 31 must flip bright half a period later, got {}",
        y[12 * 64 + 31]
    );
}

#[test]
fn wave_displaces_vertically_by_the_sine_of_the_column() {
    // Horizontal edge (dark above y = 0.5, bright below) pins the SECOND
    // displacement line: luma varies only with y here, so the x term
    // alone moves nothing. At col 38 the sine of the (already
    // x-displaced) column sits near +1: sampling shifts +0.03 in y, the
    // edge appears at y ≈ 0.47, and row 23 (input: dark) flips bright.
    // At col 22 the sine is near −1 and row 24 (input: bright) flips
    // dark. Deleting the y line leaves both rows at their input values.
    let mut filter = wave(WaveParams {
        amplitude: 0.03,
        frequency: 2.0,
        speed: 1.5,
        _pad: 0.0,
    })
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
        |_, r| if r < 24 { 60 } else { 200 },
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    assert!(
        y[23 * 64 + 38] as i32 >= 170,
        "col 38 row 23 must flip bright (edge lifted), got {}",
        y[23 * 64 + 38]
    );
    assert!(
        y[24 * 64 + 22] as i32 <= 90,
        "col 22 row 24 must flip dark (edge pushed down), got {}",
        y[24 * 64 + 22]
    );
}

#[test]
fn swirl_animated_starts_untwisted() {
    // tornado() oscillates as twist*sin(speed*t); at t=0 that is zero
    // twist, so the first frame passes through unchanged.
    let filter = swirl(SwirlParams::tornado()).build().unwrap().into_inner();
    assert_matches_identity(filter, 1, "tornado at t=0");
}

#[test]
fn swirl_oscillation_peaks_at_the_static_twist() {
    // Animated swirl a quarter period in (speed pi, pts 15 => t=0.5s,
    // sin = 1): the effective twist equals the static default, so the
    // static test's oracle point must rotate identically. An
    // implementation whose animated path never twists (or ignores the
    // time term) leaves this point at its dark input value.
    let mut filter = swirl(SwirlParams {
        speed: std::f32::consts::PI,
        ..SwirlParams::default()
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(
        &mut filter,
        vec![with_pts(step_edge_frame(64, 64, 0.5), 15)],
        1,
    )
    .pop()
    .unwrap();
    let y = plane_to_vec(&out, 0, 64, 64);
    assert!(
        y[22 * 64 + 30] as i32 >= 170,
        "at sin=1 the animated vortex must rotate like the static one, got {}",
        y[22 * 64 + 30]
    );
    assert!(
        (y[32 * 64 + 2] as i32 - 60).abs() <= 3,
        "outside the radius stays untouched, got {}",
        y[32 * 64 + 2]
    );
}

#[test]
fn swirl_rotates_inside_the_radius_only() {
    // Static swirl (speed 0), default center/radius 0.4/twist 2.5 on a
    // square frame with the step edge through the center. A point above
    // and left of the center (r=0.15, theta ~ 0.97 rad CCW) samples from
    // the bright right half; a point outside the radius is untouched.
    let mut filter = swirl(SwirlParams::default()).build().unwrap().into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![step_edge_frame(64, 64, 0.5)], 1)
        .pop()
        .unwrap();
    let y = plane_to_vec(&out, 0, 64, 64);
    // (col 30, row 22): inside the vortex, input dark (x<0.5); the CCW
    // rotation brings the right half's bright content here.
    assert!(
        y[22 * 64 + 30] as i32 >= 170,
        "inside the vortex the dark left half must rotate bright, got {}",
        y[22 * 64 + 30]
    );
    // (col 2, row 32): r=0.46 > radius 0.4 — identity, stays dark.
    assert!(
        (y[32 * 64 + 2] as i32 - 60).abs() <= 3,
        "outside the radius the frame must be untouched, got {}",
        y[32 * 64 + 2]
    );
}

#[test]
fn magnifier_enlarges_under_the_lens_only() {
    // Bright 13x13 square (|x-0.5|<=0.1) on dark ground, default lens
    // (radius 0.25, magnification 1.8). Under the lens the square grows
    // past its input footprint; outside the lens radius nothing changes.
    let mut filter = magnifier(MagnifierParams::default())
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        64,
        64,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |c, r| {
            let inside = |v: usize| (26..=37).contains(&v);
            if inside(c) && inside(r) {
                200
            } else {
                60
            }
        },
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 64);
    // (col 39, row 32): input dark (outside the square), r=0.117 inside
    // the lens; the eased zoom (~1.44 here) contracts sampling to
    // x=0.58 < 0.6 — inside the square, so the magnified square covers it.
    assert!(
        y[32 * 64 + 39] as i32 >= 170,
        "the magnified square must cover col 39, got {}",
        y[32 * 64 + 39]
    );
    // (col 55, row 32): r=0.36 > radius — identity, stays dark.
    assert!(
        (y[32 * 64 + 55] as i32 - 60).abs() <= 3,
        "outside the lens the frame must be untouched, got {}",
        y[32 * 64 + 55]
    );
}

#[test]
fn magnifier_zoom_eases_off_toward_the_rim() {
    // Pins the smoothstep gradient, not just "inside is magnified".
    // Step edge at x = 0.65; probe (col 46, row 32): r = 0.227, near the
    // default rim (radius 0.25). Eased zoom there is only ~1.02, so
    // sampling lands at x ~ 0.722 — bright. A constant full-strength
    // zoom (1.8 across the lens) would sample x ~ 0.626 — dark.
    let mut filter = magnifier(MagnifierParams::default())
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![step_edge_frame(64, 64, 0.65)], 1)
        .pop()
        .unwrap();
    let y = plane_to_vec(&out, 0, 64, 64);
    assert!(
        y[32 * 64 + 46] as i32 >= 170,
        "near the rim the zoom must have eased off (~1.02, sampling stays \
         right of the edge), got {}",
        y[32 * 64 + 46]
    );
}

#[test]
fn fisheye_zero_strength_matches_identity() {
    let filter = fisheye(FisheyeParams::with_strength(0.0))
        .build()
        .unwrap()
        .into_inner();
    assert_matches_identity(filter, 1, "fisheye strength=0");
}

#[test]
fn fisheye_bulges_the_center_and_pins_the_corners() {
    let mut filter = fisheye(FisheyeParams::with_strength(1.0))
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    // Square frame, edge at x=0.75. Center magnification pushes off-center
    // content outward: col 52 (input bright, x=0.82) now samples x=0.69 —
    // dark. The corner-normalized mapping keeps r=1 fixed, so both corners
    // keep their input's content.
    let out = drive(&mut filter, vec![step_edge_frame(64, 64, 0.75)], 1)
        .pop()
        .unwrap();
    let y = plane_to_vec(&out, 0, 64, 64);
    assert!(
        y[32 * 64 + 52] as i32 <= 90,
        "center bulge must push the edge outward past col 52, got {}",
        y[32 * 64 + 52]
    );
    assert!(
        y[0 * 64 + 63] as i32 >= 170,
        "top-right corner must keep its bright content, got {}",
        y[0 * 64 + 63]
    );
    assert!(
        y[63 * 64 + 0] as i32 <= 90,
        "bottom-left corner must keep its dark content, got {}",
        y[63 * 64 + 0]
    );
}

/// Three-band chroma key input frame (YUV420P, limited-range BT.601
/// codes): pure green screen | near-green teal | skin tone.
///
///   green #00FF00     -> (145,  54,  34)
///   teal  (.1,.9,.2)  -> (143,  80,  51)   chroma distance ~0.10 from key
///   skin  (.8,.6,.5)  -> (158, 109, 152)   chroma distance ~0.40 from key
fn chroma_key_bands_frame() -> ffmpeg_next::Frame {
    make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |c, _| {
            if c < 24 {
                145
            } else if c < 40 {
                143
            } else {
                158
            }
        },
        |c, _| {
            // Chroma plane is half width: bands at cols 12 and 20.
            if c < 12 {
                (54, 34)
            } else if c < 20 {
                (80, 51)
            } else {
                (109, 152)
            }
        },
    )
}

#[test]
fn chroma_key_removes_green_tolerates_shade_keeps_foreground() {
    let mut filter = chroma_key(ChromaKeyParams::green_screen())
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![chroma_key_bands_frame()], 1)
        .pop()
        .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let u = plane_to_vec(&out, 1, 32, 24);
    let v = plane_to_vec(&out, 2, 32, 24);
    let row = 24usize;
    let crow = 12usize;

    // Pure green band -> the black background (Y 16, neutral chroma).
    assert!(
        y[row * 64 + 8] as i32 <= 22,
        "green screen must key to black, got Y {}",
        y[row * 64 + 8]
    );
    assert!((u[crow * 32 + 4] as i32 - 128).abs() <= 4);
    assert!((v[crow * 32 + 4] as i32 - 128).abs() <= 4);

    // Near-green teal (distance ~0.10 < similarity 0.3): ALSO keyed —
    // the tolerance keys shaded/uneven screen areas, not just the exact
    // key color. This is the point of chroma keying over exact matching.
    assert!(
        y[row * 64 + 32] as i32 <= 22,
        "near-green must fall inside the similarity tolerance, got Y {}",
        y[row * 64 + 32]
    );

    // Skin band (distance ~0.40 > similarity+blend): fully kept, and the
    // despill clamp must NOT touch it (its green is below the mean of
    // red+blue, so the suppression self-gates off).
    assert!(
        (y[row * 64 + 54] as i32 - 158).abs() <= 5,
        "skin luma must survive, got {}",
        y[row * 64 + 54]
    );
    assert!(
        (u[crow * 32 + 27] as i32 - 109).abs() <= 5,
        "skin U must survive untouched, got {}",
        u[crow * 32 + 27]
    );
    assert!(
        (v[crow * 32 + 27] as i32 - 152).abs() <= 5,
        "skin V must survive untouched, got {}",
        v[crow * 32 + 27]
    );
}

#[test]
fn chroma_key_background_color_is_honored() {
    // Same bands, blue background: the keyed area must show blue
    // (limited-range BT.601 pure blue ~= (41, 240, 110)), not black.
    let mut filter = chroma_key(ChromaKeyParams::green_screen().with_background(0.0, 0.0, 1.0))
        .build()
        .unwrap()
        .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![chroma_key_bands_frame()], 1)
        .pop()
        .unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    let u = plane_to_vec(&out, 1, 32, 24);
    assert!(
        (y[24 * 64 + 8] as i32 - 41).abs() <= 5,
        "keyed area must show the blue background's luma, got {}",
        y[24 * 64 + 8]
    );
    assert!(
        u[12 * 32 + 4] as i32 >= 220,
        "keyed area must show the blue background's chroma, got U {}",
        u[12 * 32 + 4]
    );
}

#[test]
fn chroma_key_despill_clamps_green_spill() {
    // A green-spilled foreground: RGB(0.6, 0.75, 0.5) = YUV(164, 106, 116).
    // Its chroma distance from the key is ~0.30, so key it as foreground
    // with a tighter similarity, and compare spill 1.0 against spill 0.0:
    // the clamp g' = g - max(g - (r+b)/2, 0) rewrites it to
    // RGB(0.6, 0.55, 0.5) = YUV(~139, ~121, ~135) — both chroma channels
    // move toward neutral and luma drops as the green energy is removed.
    let keyed = |spill: f32| ChromaKeyParams {
        similarity: 0.15,
        blend: 0.02,
        spill,
        ..ChromaKeyParams::green_screen()
    };
    let spilled_frame = || {
        make_yuv_frame_with(
            64,
            48,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            |_, _| 164,
            |_, _| (106, 116),
        )
    };

    let mut with_spill = chroma_key(keyed(1.0)).build().unwrap().into_inner();
    if !init_filter(&mut with_spill) {
        return;
    }
    let out = drive(&mut with_spill, vec![spilled_frame()], 1)
        .pop()
        .unwrap();
    let (y, u, v) = (
        plane_to_vec(&out, 0, 64, 48),
        plane_to_vec(&out, 1, 32, 24),
        plane_to_vec(&out, 2, 32, 24),
    );
    assert!(
        y[24 * 64 + 32] as i32 <= 150,
        "despill must remove green luma energy, got {}",
        y[24 * 64 + 32]
    );
    assert!(
        u[12 * 32 + 16] as i32 >= 115,
        "despill must lift U toward neutral, got {}",
        u[12 * 32 + 16]
    );
    assert!(
        v[12 * 32 + 16] as i32 >= 128,
        "despill must lift V past neutral, got {}",
        v[12 * 32 + 16]
    );

    let mut no_spill = chroma_key(keyed(0.0)).build().unwrap().into_inner();
    assert!(init_filter(&mut no_spill));
    let out = drive(&mut no_spill, vec![spilled_frame()], 1)
        .pop()
        .unwrap();
    let (y, u, v) = (
        plane_to_vec(&out, 0, 64, 48),
        plane_to_vec(&out, 1, 32, 24),
        plane_to_vec(&out, 2, 32, 24),
    );
    assert!(
        (y[24 * 64 + 32] as i32 - 164).abs() <= 4,
        "spill=0 must keep the foreground untouched, got Y {}",
        y[24 * 64 + 32]
    );
    assert!((u[12 * 32 + 16] as i32 - 106).abs() <= 4);
    assert!((v[12 * 32 + 16] as i32 - 116).abs() <= 4);
}

#[test]
fn chroma_key_blend_ramps_linearly() {
    // Pins the linear (d - similarity)/blend ramp against a hard
    // threshold. Uniform teal frame: d ~= 0.098 from the green key, so
    // similarity 0.05 / blend 0.1 puts it mid-ramp — alpha ~= 0.48 and
    // the output luma lands near 77, between the black background (16)
    // and the kept foreground (143). A hard threshold collapses to one
    // of the extremes.
    let mut filter = chroma_key(ChromaKeyParams {
        similarity: 0.05,
        blend: 0.1,
        spill: 0.0,
        ..ChromaKeyParams::green_screen()
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let teal = make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |_, _| 143,
        |_, _| (80, 51),
    );
    let out = drive(&mut filter, vec![teal], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    assert!(
        (60..=95).contains(&(y[24 * 64 + 32] as i32)),
        "mid-ramp teal must blend partway to the background (~77), got {}",
        y[24 * 64 + 32]
    );
}

#[test]
fn chroma_key_box_filter_softens_the_matte_boundary() {
    // Pins the 3x3 distance box filter against center-only sampling.
    // YUV444P (full-res chroma) with a hard green|skin boundary at
    // col 32. The first skin column's taps reach one texel into the
    // green field, dragging its averaged distance down to ~0.27, while
    // its center-only distance is ~0.41. With similarity 0.34 (near-hard
    // blend) the box filter keys that column to the background; sampling
    // only the center would keep it at skin luma 158. The next column's
    // taps see no green — it must survive.
    let mut filter = chroma_key(ChromaKeyParams {
        similarity: 0.34,
        blend: 0.001,
        spill: 0.0,
        ..ChromaKeyParams::green_screen()
    })
    .build()
    .unwrap()
    .into_inner();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV444P,
        |c, _| if c < 32 { 145 } else { 158 },
        |c, _| if c < 32 { (54, 34) } else { (109, 152) },
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    let y = plane_to_vec(&out, 0, 64, 48);
    assert!(
        y[24 * 64 + 32] as i32 <= 22,
        "the boundary column's box-averaged distance (~0.27) must key it \
         to the background, got Y {}",
        y[24 * 64 + 32]
    );
    assert!(
        (y[24 * 64 + 34] as i32 - 158).abs() <= 5,
        "two columns in, no tap reaches the green field — skin must \
         survive, got Y {}",
        y[24 * 64 + 34]
    );
}
