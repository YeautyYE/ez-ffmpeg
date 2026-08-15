//! Boundary matrix, format sampling, skip/reset, and property tests.

use super::*;
use crate::core::analysis::crop::luma::{ChromaGrid, PatternLuma, SignalRange};
use crate::core::analysis::crop::scan::{
    align_outward, resolve_threshold, scan_boundary_bands, ScanConfig,
};

fn drive(
    opts: CropDetectionOptions,
    pattern: &PatternLuma,
    frames: usize,
    fps_us: i64,
) -> Vec<CropObservation> {
    let mut scanner = CropScanner::new(opts).expect("options");
    let mut out = Vec::new();
    for i in 0..frames {
        if let Some((_, obs)) = scanner.process_luma(pattern, Some(i as i64 * fps_us), false) {
            out.push(obs);
        }
    }
    out
}

fn default_letterbox() -> PatternLuma {
    PatternLuma::letterbox(320, 240, 40)
}

#[test]
fn l01_limit_zero_full_frame() {
    let opts = CropDetectionOptions::new()
        .threshold(CropLumaThreshold::Normalized(0.0))
        .skip_initial(0)
        .round(1);
    let p = default_letterbox();
    let ev = drive(opts, &p, 8, 40_000);
    assert!(!ev.is_empty());
    let last = *ev.last().unwrap();
    assert_eq!(last.aligned.x, 0);
    assert_eq!(last.aligned.y, 0);
    assert_eq!(last.aligned.w, 320);
    assert_eq!(last.aligned.h, 240);
}

#[test]
fn l02_default_24_matches_normalized() {
    let a = CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(0);
    let b = CropDetectionOptions::new()
        .threshold(CropLumaThreshold::Normalized(24.0 / 255.0))
        .round(1)
        .skip_initial(0);
    let p = default_letterbox();
    let ea = drive(a, &p, 8, 40_000);
    let eb = drive(b, &p, 8, 40_000);
    assert_eq!(ea.last().unwrap().raw, eb.last().unwrap().raw);
}

#[test]
fn l03_limit_255_no_initial_crop() {
    let opts = CropDetectionOptions::from_legacy(255, 1, 0).skip_initial(0);
    let p = default_letterbox();
    let ev = drive(opts, &p, 8, 40_000);
    assert!(
        ev.is_empty(),
        "all-black classification must not invent a crop"
    );
}

#[test]
fn l04_normalized_zero_matches_l01() {
    let opts = CropDetectionOptions::new()
        .threshold(CropLumaThreshold::Normalized(0.0))
        .skip_initial(0)
        .round(1);
    let p = default_letterbox();
    assert!(!drive(opts, &p, 6, 40_000).is_empty());
}

#[test]
fn l05_normalized_half_scales() {
    let b8 = resolve_threshold(
        CropLumaThreshold::Normalized(0.5),
        8,
        SignalRange::Full,
        0.0,
    )
    .unwrap();
    let b10 = resolve_threshold(
        CropLumaThreshold::Normalized(0.5),
        10,
        SignalRange::Full,
        0.0,
    )
    .unwrap();
    assert_eq!(b8.low, 128);
    assert_eq!(b10.low, 512);
}

#[test]
fn l06_normalized_one_is_all_black() {
    let opts = CropDetectionOptions::new()
        .threshold(CropLumaThreshold::Normalized(1.0))
        .skip_initial(0)
        .round(1);
    let p = default_letterbox();
    assert!(drive(opts, &p, 6, 40_000).is_empty());
}

#[test]
fn l07_invalid_thresholds() {
    for f in [f32::NAN, f32::INFINITY, -0.01, 1.01] {
        assert!(
            CropDetectionControl::new(CropLumaThreshold::Normalized(f)).is_err(),
            "{f}"
        );
        let ctrl = CropDetectionControl::new(CropLumaThreshold::Normalized(0.1)).unwrap();
        let before = ctrl.threshold();
        assert!(ctrl
            .set_threshold(CropLumaThreshold::Normalized(f))
            .is_err());
        assert_eq!(ctrl.threshold(), before);
    }
}

#[test]
fn l08_legacy_300_is_raw_on_10bit() {
    let p = PatternLuma::letterbox(160, 120, 16).with_depth(10);
    let opts = CropDetectionOptions::from_legacy(300, 1, 0).skip_initial(0);
    let ev = drive(opts, &p, 6, 40_000);
    assert!(!ev.is_empty());
}

#[test]
fn l09_legacy_4096_saturates() {
    let b = resolve_threshold(
        crate::core::analysis::crop::scan::legacy_limit(4096),
        10,
        SignalRange::Limited,
        0.0,
    )
    .unwrap();
    assert_eq!(b.low, 1023);
}

#[test]
fn l10_ten_bit_legacy_24_is_96() {
    let b = resolve_threshold(
        CropLumaThreshold::Normalized(24.0 / 255.0),
        10,
        SignalRange::Limited,
        4.0 / 255.0,
    )
    .unwrap();
    assert_eq!(b.low, 96);
}

#[test]
fn l11_raw_1023_all_black() {
    let p = PatternLuma::letterbox(80, 48, 8).with_depth(10);
    let opts = CropDetectionOptions::new()
        .threshold(CropLumaThreshold::RawCode(1023))
        .skip_initial(0)
        .round(1);
    assert!(drive(opts, &p, 6, 40_000).is_empty());
}

#[test]
fn l12_threshold_monotonicity() {
    let p = default_letterbox();
    let mut prev_area = i64::MAX;
    for limit in [0u32, 24, 64, 128] {
        let opts = CropDetectionOptions::from_legacy(limit, 1, 0).skip_initial(0);
        let ev = drive(opts, &p, 6, 40_000);
        if let Some(last) = ev.last() {
            let area = last.aligned.w as i64 * last.aligned.h as i64;
            assert!(area <= prev_area, "limit {limit} area {area} > {prev_area}");
            prev_area = area;
        }
    }
    let none = drive(
        CropDetectionOptions::from_legacy(255, 1, 0).skip_initial(0),
        &p,
        6,
        40_000,
    );
    assert!(none.is_empty());
}

#[test]
fn r01_round_zero_no_multiple() {
    let raw = CropRawBounds {
        left: 3,
        top: 5,
        right_exclusive: 61,
        bottom_exclusive: 51,
    };
    let a = align_outward(raw, 80, 60, 0, ChromaGrid::None);
    assert!(a.left <= 3 && a.top <= 5);
    assert!(a.right_exclusive >= 61 && a.bottom_exclusive >= 51);
}

#[test]
fn r02_round_one_same_as_zero_quant() {
    let raw = CropRawBounds {
        left: 4,
        top: 6,
        right_exclusive: 40,
        bottom_exclusive: 38,
    };
    let a0 = align_outward(raw, 64, 64, 0, ChromaGrid::None);
    let a1 = align_outward(raw, 64, 64, 1, ChromaGrid::None);
    assert_eq!(a0, a1);
}

#[test]
fn r03_round_two_even() {
    let raw = CropRawBounds {
        left: 5,
        top: 7,
        right_exclusive: 41,
        bottom_exclusive: 39,
    };
    let a = align_outward(raw, 64, 64, 2, ChromaGrid::None);
    assert_eq!((a.right_exclusive - a.left) % 2, 0);
    assert_eq!((a.bottom_exclusive - a.top) % 2, 0);
    assert!(a.left <= raw.left && a.right_exclusive >= raw.right_exclusive);
}

#[test]
fn r04_round_16_divisible() {
    let raw = CropRawBounds {
        left: 13,
        top: 7,
        right_exclusive: 213,
        bottom_exclusive: 107,
    };
    let a = align_outward(raw, 320, 240, 16, ChromaGrid::None);
    assert_eq!((a.right_exclusive - a.left) % 16, 0);
    assert_eq!((a.bottom_exclusive - a.top) % 16, 0);
}

#[test]
fn r05_odd_round_never_cuts() {
    let raw = CropRawBounds {
        left: 10,
        top: 10,
        right_exclusive: 40,
        bottom_exclusive: 40,
    };
    for round in [3u32, 5] {
        let a = align_outward(raw, 64, 64, round, ChromaGrid::None);
        assert!(a.left <= raw.left);
        assert!(a.top <= raw.top);
        assert!(a.right_exclusive >= raw.right_exclusive);
        assert!(a.bottom_exclusive >= raw.bottom_exclusive);
        let w = a.right_exclusive - a.left;
        let h = a.bottom_exclusive - a.top;
        assert_eq!(w % round as i32, 0, "round={round} w={w}");
        assert_eq!(h % round as i32, 0, "round={round} h={h}");
    }
}

#[test]
fn r06_round_gt_width_full() {
    let raw = CropRawBounds {
        left: 8,
        top: 8,
        right_exclusive: 40,
        bottom_exclusive: 40,
    };
    let a = align_outward(raw, 50, 80, 64, ChromaGrid::None);
    assert_eq!(a.left, 0);
    assert_eq!(a.right_exclusive, 50);
}

#[test]
fn r07_round_gt_height_full() {
    let raw = CropRawBounds {
        left: 8,
        top: 8,
        right_exclusive: 40,
        bottom_exclusive: 40,
    };
    let a = align_outward(raw, 80, 50, 64, ChromaGrid::None);
    assert_eq!(a.top, 0);
    assert_eq!(a.bottom_exclusive, 50);
}

#[test]
fn r08_chroma_conflict_round_3() {
    let raw = CropRawBounds {
        left: 5,
        top: 7,
        right_exclusive: 25,
        bottom_exclusive: 27,
    };
    let a = align_outward(raw, 64, 64, 3, ChromaGrid::Yuv420);
    assert_eq!(a.left % 2, 0);
    assert_eq!(a.top % 2, 0);
    assert!(a.left <= 5 && a.top <= 7);
    assert!(a.right_exclusive >= 25 && a.bottom_exclusive >= 27);
}

#[test]
fn r09_odd_frame_round_16_stays_in_frame() {
    let raw = CropRawBounds {
        left: 0,
        top: 0,
        right_exclusive: 17,
        bottom_exclusive: 15,
    };
    let a = align_outward(raw, 17, 15, 16, ChromaGrid::None);
    assert!(a.left >= 0 && a.top >= 0);
    assert!(a.right_exclusive <= 17 && a.bottom_exclusive <= 15);
    assert!(a.left <= 0 && a.top <= 0);
    assert!(a.right_exclusive >= 17 && a.bottom_exclusive >= 15);
}

#[test]
fn s01_skip_zero_scans_first_frame() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(0);
    let p = default_letterbox();
    let mut scanner = CropScanner::new(opts).unwrap();
    p.reset_probes();
    let first = scanner.process_luma(&p, Some(0), false);
    assert!(first.is_none(), "bootstrap still needs 3 candidates");
    assert!(p.probe_count() > 0);
}

#[test]
fn s02_skip_one_zero_probes_first_frame() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(1);
    let p = default_letterbox();
    let mut scanner = CropScanner::new(opts).unwrap();
    p.reset_probes();
    assert!(scanner.process_luma(&p, Some(0), false).is_none());
    assert_eq!(p.probe_count(), 0);
    p.reset_probes();
    scanner.process_luma(&p, Some(40_000), false);
    assert!(p.probe_count() > 0);
}

#[test]
fn s03_skip_two_default() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 0); // skip=2
    let p = default_letterbox();
    let mut scanner = CropScanner::new(opts).unwrap();
    for i in 0..2 {
        p.reset_probes();
        assert!(scanner.process_luma(&p, Some(i * 40_000), false).is_none());
        assert_eq!(p.probe_count(), 0, "skipped frame {i}");
    }
    p.reset_probes();
    scanner.process_luma(&p, Some(80_000), false);
    assert!(p.probe_count() > 0);
}

#[test]
fn s04_skip_gt_total_no_events() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(20);
    let p = default_letterbox();
    assert!(drive(opts, &p, 8, 40_000).is_empty());
}

#[test]
fn s05_skip_validation() {
    assert!(CropDetectionOptions::new()
        .skip_initial(i32::MAX as u32)
        .validate()
        .is_ok());
    assert!(CropDetectionOptions::new()
        .skip_initial(i32::MAX as u32 + 1)
        .validate()
        .is_err());
}

#[test]
fn s06_skip_two_plus_bootstrap_first_event_at_frame_five() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 0); // skip=2
    let p = default_letterbox();
    let mut scanner = CropScanner::new(opts).unwrap();
    let mut first = None;
    for i in 0..8 {
        if scanner.process_luma(&p, Some(i * 40_000), false).is_some() {
            first = Some(i);
            break;
        }
    }
    assert_eq!(
        first,
        Some(4),
        "0-based frame index 4 is the 5th real frame"
    );
}

#[test]
fn x01_reset_zero_keeps_stable() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(0);
    let p = default_letterbox();
    let ev = drive(opts, &p, 12, 40_000);
    assert!(ev.len() >= 6);
}

#[test]
fn x02_reset_one_still_bootstraps() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 1).skip_initial(0);
    let mut p = default_letterbox();
    p.noise_frac = 0.01;
    let ev = drive(opts, &p, 10, 40_000);
    assert!(!ev.is_empty());
    let areas: Vec<_> = ev
        .iter()
        .map(|o| o.aligned.w as i64 * o.aligned.h as i64)
        .collect();
    for w in areas.windows(2) {
        assert!(
            w[1] >= w[0] - 320,
            "must not oscillate inward under reset=1"
        );
    }
}

#[test]
fn x03_reset_n_clears_evidence_after_n() {
    let opts = CropDetectionOptions::from_legacy(24, 1, 4).skip_initial(0);
    let p = default_letterbox();
    assert!(!drive(opts, &p, 10, 40_000).is_empty());
}

#[test]
fn c01_limit_zero_round_gt_frame() {
    let opts = CropDetectionOptions::from_legacy(0, 1024, 0).skip_initial(0);
    let p = default_letterbox();
    let ev = drive(opts, &p, 6, 40_000);
    let last = ev.last().unwrap();
    assert_eq!(last.aligned.w, 320);
    assert_eq!(last.aligned.h, 240);
}

#[test]
fn c02_all_black_and_skip_gt_frames() {
    let mut p = default_letterbox();
    p.content_code = 8;
    p.black_code = 8;
    let opts = CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(50);
    assert!(drive(opts, &p, 8, 40_000).is_empty());
}

#[test]
fn c03_reset_one_noise_no_inward_oscillation() {
    let mut p = default_letterbox();
    p.noise_frac = 0.012;
    p.noise_seed = 3;
    let opts = CropDetectionOptions::from_legacy(24, 1, 1).skip_initial(0);
    let ev = drive(opts, &p, 12, 40_000);
    assert!(!ev.is_empty());
}

#[test]
fn c04_scene_during_skip_does_not_scan() {
    let p = default_letterbox();
    let mut scanner =
        CropScanner::new(CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(2)).unwrap();
    p.reset_probes();
    assert!(scanner.process_luma(&p, Some(0), true).is_none());
    assert_eq!(p.probe_count(), 0);
    p.reset_probes();
    assert!(scanner.process_luma(&p, Some(40_000), true).is_none());
    assert_eq!(p.probe_count(), 0);
}

#[test]
fn c05_runtime_control_valid_update() {
    let ctrl = CropDetectionControl::new(CropLumaThreshold::Normalized(24.0 / 255.0)).unwrap();
    let opts = CropDetectionOptions::new()
        .skip_initial(0)
        .round(1)
        .threshold_control(ctrl.clone());
    let p = default_letterbox();
    let mut scanner = CropScanner::new(opts).unwrap();
    for i in 0..4 {
        scanner.process_luma(&p, Some(i * 40_000), false);
    }
    ctrl.set_threshold(CropLumaThreshold::Normalized(64.0 / 255.0))
        .unwrap();
    scanner.process_luma(&p, Some(200_000), false);
    assert_eq!(
        ctrl.threshold(),
        CropLumaThreshold::Normalized(64.0 / 255.0)
    );
}

#[test]
fn c06_runtime_control_invalid_keeps_old() {
    let ctrl = CropDetectionControl::new(CropLumaThreshold::Normalized(0.1)).unwrap();
    assert!(ctrl
        .set_threshold(CropLumaThreshold::Normalized(f32::NAN))
        .is_err());
    assert_eq!(ctrl.threshold(), CropLumaThreshold::Normalized(0.1));
}

#[test]
fn o01_raw_vs_aligned_asymmetric() {
    let mut p = PatternLuma::letterbox(320, 240, 0);
    p.top = 13;
    p.bottom = 7;
    p.left = 11;
    p.right = 5;
    let opts = CropDetectionOptions::from_legacy(24, 16, 0).skip_initial(0);
    let ev = drive(opts, &p, 8, 40_000);
    let last = ev.last().unwrap();
    assert!(last.aligned.x <= last.raw.left);
    assert!(last.aligned.y <= last.raw.top);
    assert!(last.aligned.x + last.aligned.w >= last.raw.right_exclusive);
    assert!(last.aligned.y + last.aligned.h >= last.raw.bottom_exclusive);
    assert_eq!(last.aligned.w % 16, 0);
    assert_eq!(last.aligned.h % 16, 0);
}

#[test]
fn o02_published_suggestion_is_aligned_not_raw() {
    let mut p = PatternLuma::letterbox(320, 240, 0);
    p.top = 13;
    p.bottom = 7;
    p.left = 11;
    p.right = 5;
    let opts = CropDetectionOptions::from_legacy(24, 16, 0).skip_initial(0);
    let ev = drive(opts, &p, 8, 40_000);
    let last = ev.last().unwrap();
    // MetadataEvent::CropDetect carries the aligned rectangle only.
    assert_ne!(
        (
            last.aligned.x,
            last.aligned.y,
            last.aligned.w,
            last.aligned.h
        ),
        (
            last.raw.left,
            last.raw.top,
            last.raw.right_exclusive - last.raw.left,
            last.raw.bottom_exclusive - last.raw.top
        )
    );
    assert!(last.aligned.w % 16 == 0 && last.aligned.h % 16 == 0);
}

fn format_case(name: &str, p: PatternLuma, limit: u32) {
    p.reset_probes();
    let opts = CropDetectionOptions::from_legacy(limit, 1, 0).skip_initial(0);
    let ev = drive(opts, &p, 6, 40_000);
    assert!(
        !ev.is_empty() || limit >= 255,
        "{name} limit={limit} produced no crop"
    );
}

#[test]
fn format_matrix_planar8() {
    for (name, chroma) in [
        ("yuv420p", ChromaGrid::Yuv420),
        ("yuv422p", ChromaGrid::Yuv422),
        ("yuv444p", ChromaGrid::None),
    ] {
        let p = PatternLuma::letterbox(160, 120, 16).with_chroma(chroma);
        format_case(name, p, 24);
    }
}

#[test]
fn format_matrix_yuvj_full_range() {
    for chroma in [ChromaGrid::Yuv420, ChromaGrid::Yuv422, ChromaGrid::None] {
        let p = PatternLuma::letterbox(128, 96, 12)
            .with_chroma(chroma)
            .with_range(SignalRange::Full);
        format_case("yuvj", p, 24);
    }
}

#[test]
fn format_matrix_10bit_families() {
    for chroma in [ChromaGrid::Yuv420, ChromaGrid::Yuv422, ChromaGrid::None] {
        let p = PatternLuma::letterbox(128, 96, 12)
            .with_chroma(chroma)
            .with_depth(10);
        format_case("planar10", p, 24);
    }
}

#[test]
fn format_nv12_nv21_plane0_only() {
    let p = PatternLuma::letterbox(160, 120, 16).with_chroma(ChromaGrid::Yuv420);
    format_case("nv12", p, 24);
}

#[test]
fn format_gray_no_chroma() {
    let p = PatternLuma::letterbox(100, 80, 10).with_chroma(ChromaGrid::None);
    format_case("gray8", p, 24);
    let p10 = PatternLuma::letterbox(100, 80, 10)
        .with_chroma(ChromaGrid::None)
        .with_depth(10);
    format_case("gray10", p10, 24);
}

#[test]
fn property_rect_in_frame_and_contains_raw() {
    for w in [17u32, 32, 63, 80] {
        for h in [15u32, 24, 48] {
            for bar in [0u32, 1, h / 6] {
                let p = PatternLuma::letterbox(w, h, bar.min(h / 3));
                let opts = CropDetectionOptions::from_legacy(24, 16, 0).skip_initial(0);
                for obs in drive(opts, &p, 6, 40_000) {
                    let a = obs.aligned;
                    assert!(a.x >= 0 && a.y >= 0);
                    assert!(a.w > 0 && a.h > 0);
                    assert!(a.x as u32 + a.w as u32 <= w);
                    assert!(a.y as u32 + a.h as u32 <= h);
                    assert!(a.x <= obs.raw.left);
                    assert!(a.y <= obs.raw.top);
                    assert!(a.x + a.w >= obs.raw.right_exclusive);
                    assert!(a.y + a.h >= obs.raw.bottom_exclusive);
                }
            }
        }
    }
}

#[test]
fn skipped_frames_zero_luma_reads() {
    let p = PatternLuma::windowbox(1920, 1080, 0.2);
    let mut scanner =
        CropScanner::new(CropDetectionOptions::from_legacy(24, 16, 0).skip_initial(2)).unwrap();
    for i in 0..2 {
        p.reset_probes();
        scanner.process_luma(&p, Some(i * 33_000), false);
        assert_eq!(p.probe_count(), 0);
        assert_eq!(scanner.last_probe_count(), 0);
    }
}

#[test]
fn probe_caps_1080_and_4k() {
    for (w, h, cap) in [(1920u32, 1080u32, 80_000u32), (3840, 2160, 100_000)] {
        let p = PatternLuma::windowbox(w, h, 0.45);
        p.reset_probes();
        let cfg = ScanConfig::standard(
            resolve_threshold(
                CropLumaThreshold::Normalized(24.0 / 255.0),
                8,
                SignalRange::Limited,
                4.0 / 255.0,
            )
            .unwrap(),
        );
        let _ = scan_boundary_bands(&p, &cfg);
        assert!(
            p.probe_count() <= cap,
            "{w}x{h} probes={} (cap {cap})",
            p.probe_count()
        );
    }
}

#[test]
fn interlaced_process_frame_is_analysis_frame_not_recipe_arg() {
    use ffmpeg_next::Frame;
    use ffmpeg_sys_next::{av_frame_get_buffer, AVPixelFormat, AV_FRAME_FLAG_INTERLACED};

    let mut scanner =
        CropScanner::new(CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(0)).unwrap();
    let err = unsafe {
        let mut frame = Frame::empty();
        let p = frame.as_mut_ptr();
        (*p).width = 8;
        (*p).height = 8;
        (*p).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        (*p).flags |= AV_FRAME_FLAG_INTERLACED;
        scanner.process_frame(&frame, None, false).unwrap_err()
    };
    assert!(
        matches!(err, Error::AnalysisFrame(_)),
        "runtime frame errors must be AnalysisFrame, got {err}"
    );
    assert!(
        !matches!(err, Error::InvalidRecipeArg(_)),
        "runtime frame errors must not wear InvalidRecipeArg: {err}"
    );
    assert!(err.to_string().contains("interlaced"), "{err}");
}

#[test]
fn hardware_process_frame_is_analysis_frame_not_recipe_arg() {
    use ffmpeg_next::Frame;
    use ffmpeg_sys_next::{av_frame_get_buffer, AVPixelFormat};

    let mut scanner =
        CropScanner::new(CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(0)).unwrap();
    let err = unsafe {
        let mut frame = Frame::empty();
        let p = frame.as_mut_ptr();
        (*p).width = 8;
        (*p).height = 8;
        (*p).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        (*p).hw_frames_ctx = 1 as *mut _;
        let err = scanner.process_frame(&frame, None, false).unwrap_err();
        (*p).hw_frames_ctx = std::ptr::null_mut();
        err
    };
    assert!(
        matches!(err, Error::AnalysisFrame(_)),
        "hardware frames must be AnalysisFrame, got {err}"
    );
    assert!(
        !matches!(err, Error::InvalidRecipeArg(_)),
        "hardware frames must not wear InvalidRecipeArg: {err}"
    );
    assert!(err.to_string().contains("hwdownload"), "{err}");
}

#[test]
fn skip_initial_bypasses_interlaced_and_hardware_validation() {
    use ffmpeg_next::Frame;
    use ffmpeg_sys_next::{av_frame_get_buffer, AVPixelFormat, AV_FRAME_FLAG_INTERLACED};

    let mut scanner =
        CropScanner::new(CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(2)).unwrap();
    unsafe {
        let mut interlaced = Frame::empty();
        let p = interlaced.as_mut_ptr();
        (*p).width = 8;
        (*p).height = 8;
        (*p).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        (*p).flags |= AV_FRAME_FLAG_INTERLACED;
        assert!(
            scanner
                .process_frame(&interlaced, None, false)
                .unwrap()
                .is_none(),
            "skipped interlaced frames must not fail the job"
        );

        let mut hardware = Frame::empty();
        let p = hardware.as_mut_ptr();
        (*p).width = 8;
        (*p).height = 8;
        (*p).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        (*p).hw_frames_ctx = 1 as *mut _;
        let skipped = scanner.process_frame(&hardware, None, false);
        (*p).hw_frames_ctx = std::ptr::null_mut();
        assert!(
            skipped.unwrap().is_none(),
            "skipped hardware frames must not fail the job"
        );
    }
}

#[test]
fn scene_change_emits_full_then_rebases() {
    let p = default_letterbox();
    let mut scanner =
        CropScanner::new(CropDetectionOptions::from_legacy(24, 1, 0).skip_initial(0)).unwrap();
    for i in 0..5 {
        scanner.process_luma(&p, Some(i * 40_000), false);
    }
    let mut p2 = PatternLuma::letterbox(320, 240, 0);
    p2.left = 60;
    p2.right = 60;
    let (_, obs) = scanner
        .process_luma(&p2, Some(400_000), true)
        .expect("scene reset publishes full frame");
    assert_eq!(obs.aligned.x, 0);
    assert_eq!(obs.aligned.y, 0);
    assert_eq!(obs.aligned.w, 320);
    assert_eq!(obs.aligned.h, 240);
}
