//! Native crop detection integration tests.
//!
//! These run against the linked FFmpeg **without** requiring the GPL
//! `cropdetect` filter. Inputs are lavfi graphs.

use ez_ffmpeg::analysis::{
    Analysis, CropDetectionControl, CropDetectionOptions, CropLumaThreshold, VideoDetector,
};
use ez_ffmpeg::capabilities::is_filter_available;
use ez_ffmpeg::error::Error;
use ez_ffmpeg::Input;

fn lavfi(graph: &str) -> Input {
    Input::from(graph).set_format("lavfi")
}

fn letterbox_white() -> Input {
    // 320x240 with 40 px black bars top/bottom; 25 fps, 1 s → 25 frames.
    lavfi("color=c=white:s=320x160:r=25:d=1,pad=320:240:0:40:black")
}

#[test]
fn i01_legacy_defaults_letterbox() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 2,
            reset: 0,
        })
        .run()
        .expect("native crop on lavfi letterbox");
    let crop = report.crop.expect("expected a crop suggestion");
    assert!(
        crop.y >= 32 && crop.y <= 48,
        "letterbox y={} (want ~40)",
        crop.y
    );
    assert!(
        crop.h >= 144 && crop.h <= 176,
        "letterbox h={} (want ~160)",
        crop.h
    );
    assert_eq!(crop.w % 2, 0);
    assert_eq!(crop.h % 2, 0);
    assert!(crop.x >= 0 && crop.y >= 0);
    assert!(crop.x + crop.w <= 320);
    assert!(crop.y + crop.h <= 240);
}

#[test]
fn i02_limit_zero_full_frame() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Crop {
            limit: 0,
            round: 1,
            reset: 0,
        })
        .run()
        .expect("limit=0");
    let crop = report.crop.expect("limit=0 is full frame after bootstrap");
    assert_eq!(crop.x, 0);
    assert_eq!(crop.y, 0);
    assert_eq!(crop.w, 320);
    assert_eq!(crop.h, 240);
}

#[test]
fn i03_limit_255_no_crop() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Crop {
            limit: 255,
            round: 16,
            reset: 0,
        })
        .run()
        .expect("limit=255");
    assert!(
        report.crop.is_none(),
        "all-black classification must not invent a crop, got {:?}",
        report.crop
    );
}

#[test]
fn i05_round_16_divisible_when_feasible() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 16,
            reset: 0,
        })
        .run()
        .expect("round=16");
    let crop = report.crop.expect("crop");
    assert_eq!(crop.w % 16, 0);
    assert_eq!(crop.h % 16, 0);
}

#[test]
fn i06_round_gt_frame_stays_in_bounds() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 1024,
            reset: 0,
        })
        .run()
        .expect("round>frame");
    let crop = report.crop.expect("crop");
    assert_eq!(crop.x, 0);
    assert_eq!(crop.y, 0);
    assert_eq!(crop.w, 320);
    assert_eq!(crop.h, 240);
}

#[test]
fn i07_skip_zero_still_bootstraps() {
    let detailed = Analysis::new(letterbox_white())
        .crop_detection(CropDetectionOptions::from_legacy(24, 2, 0).skip_initial(0))
        .run_detailed()
        .expect("skip=0");
    assert!(detailed.report.crop.is_some());
    assert!(detailed.last_crop_observation.is_some());
}

#[test]
fn i08_skip_gt_total_none() {
    let report = Analysis::new(lavfi(
        "color=c=white:s=160x120:r=25:d=0.2,pad=160:160:0:20:black",
    ))
    .crop_detection(CropDetectionOptions::from_legacy(24, 2, 0).skip_initial(10_000))
    .run()
    .expect("skip>total");
    assert!(report.crop.is_none());
}

#[test]
fn i09_reset_one_completes() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 2,
            reset: 1,
        })
        .run()
        .expect("reset=1");
    assert!(report.crop.is_some());
}

#[test]
fn i11_crop_with_black_single_decode() {
    let report = Analysis::new(letterbox_white())
        .video_detector(VideoDetector::Black {
            min_duration_s: 0.05,
            pixel_th: 0.10,
            picture_th: 0.98,
        })
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 2,
            reset: 0,
        })
        .run()
        .expect("crop+black");
    assert!(report.crop.is_some());
}

#[test]
fn i13_crop_with_scene_single_decode() {
    // White 40px letterbox, then a red 10px letterbox. The concat cut is a
    // hard scene change (scdet at threshold 10). Native crop shares the
    // decode with Scene (and Black, which is cheap metadata).
    let report = Analysis::new(lavfi(
        "color=c=white:s=320x160:r=25:d=1,pad=320:240:0:40:black,format=yuv420p[a];\
color=c=red:s=320x220:r=25:d=1,pad=320:240:0:10:black,format=yuv420p[b];\
[a][b]concat=n=2:v=1:a=0",
    ))
    .video_detector(VideoDetector::Black {
        min_duration_s: 0.05,
        pixel_th: 0.10,
        picture_th: 0.98,
    })
    .video_detector(VideoDetector::Scene {
        threshold_pct: 10.0,
    })
    .video_detector(VideoDetector::Crop {
        limit: 24,
        round: 2,
        reset: 0,
    })
    .run()
    .expect("crop+scene(+black)");

    assert!(
        !report.scenes.is_empty(),
        "expected scdet at the white->red concat cut, got {:?}",
        report.scenes
    );
    let crop = report.crop.expect("expected a crop suggestion");
    // Pre-cut letterbox is ~40px (h≈160). A scene reset may publish the full
    // frame and then re-converge to the 10px bars (h≈220). Either is fine;
    // remaining stuck on the first letterbox is not.
    assert!(crop.h > 180, "crop stuck on pre-cut letterbox: {crop:?}");
    assert!(crop.x >= 0 && crop.y >= 0);
    assert!(crop.x + crop.w <= 320);
    assert!(crop.y + crop.h <= 240);
}

#[test]
fn i12_observer_and_control() {
    let ctrl = CropDetectionControl::new(CropLumaThreshold::Normalized(24.0 / 255.0)).unwrap();
    let detailed = Analysis::new(letterbox_white())
        .crop_detection(
            CropDetectionOptions::from_legacy(24, 2, 0)
                .skip_initial(2)
                .threshold_control(ctrl.clone()),
        )
        .run_detailed()
        .expect("observer path");
    assert!(detailed.report.crop.is_some());
    let obs = detailed.last_crop_observation.expect("raw observation");
    assert!(obs.aligned.w > 0 && obs.aligned.h > 0);
    assert!(obs.raw.right_exclusive > obs.raw.left);
    assert!(ctrl
        .set_threshold(CropLumaThreshold::Normalized(f32::NAN))
        .is_err());
    assert_eq!(
        ctrl.threshold(),
        CropLumaThreshold::Normalized(24.0 / 255.0)
    );
}

#[test]
fn i04_ten_bit_legacy_24() {
    let report = Analysis::new(lavfi(
        "color=c=white:s=320x160:r=25:d=1,pad=320:240:0:40:black,format=yuv420p10le",
    ))
    .video_detector(VideoDetector::Crop {
        limit: 24,
        round: 2,
        reset: 0,
    })
    .run()
    .expect("10-bit letterbox");
    let crop = report.crop.expect("10-bit crop");
    assert!(
        crop.y >= 32 && crop.y <= 48,
        "10-bit letterbox y={}",
        crop.y
    );
}

fn interlaced_lavfi() -> Option<Input> {
    if !is_filter_available("tinterlace") {
        return None;
    }
    Some(lavfi(
        "testsrc=size=64x64:rate=25:duration=1,tinterlace=interleave_top",
    ))
}

#[test]
fn interlaced_analysis_run_is_analysis_frame() {
    let Some(input) = interlaced_lavfi() else {
        eprintln!("skipping: linked FFmpeg has no tinterlace filter");
        return;
    };
    match Analysis::new(input)
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 2,
            reset: 0,
        })
        .run()
    {
        Err(Error::AnalysisFrame(msg)) => {
            assert!(
                msg.contains("interlaced") || msg.to_string().contains("interlaced"),
                "{msg}"
            );
        }
        other => panic!("expected Error::AnalysisFrame from Analysis::run, got {other:?}"),
    }
}

#[test]
fn interlaced_skip_initial_does_not_fail_analysis_run() {
    let Some(input) = interlaced_lavfi() else {
        eprintln!("skipping: linked FFmpeg has no tinterlace filter");
        return;
    };
    Analysis::new(input)
        .crop_detection(CropDetectionOptions::from_legacy(24, 2, 0).skip_initial(100))
        .run()
        .expect("skip_initial must step over interlaced leading frames");
}
