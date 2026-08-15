//! Four-variant crop parity / marginal-cost harness (optional GPL `ffmpeg`
//! executable + native Rust `Analysis`).
//!
//! Rust variants share the native `VideoDetector::Crop` path: `rust_off` uses
//! `limit=0` (full-frame sentinel, no luma scan) and `rust_on` uses a normal
//! limit. `rust_off` is not `blackdetect` — that is a different lavfi graph
//! and would make `paired_ratio` incomparable.
//!
//! This is **not** part of the default test lane. CI runs
//! `rust_on_off_letterbox_1080_smoke` as a dedicated `--ignored --exact`
//! step on Linux Test (rust-only when the executable or `cropdetect` is
//! missing) and on the macOS Homebrew FFmpeg 7.1 lane (GPL, `cropdetect`
//! typically present). The main cargo test lines must not gain `--ignored`.
//! Run locally with:
//!
//! ```text
//! cargo test --release --test crop_parity_bench -- --ignored --nocapture
//! ```
//!
//! Clean-room: this binary only times the `ffmpeg` executable (user-facing
//! `-vf cropdetect=...`) and this crate's native scanner. It never reads
//! FFmpeg filter sources. The native rust half is LGPL-safe and always
//! asserted. The ffmpeg half runs only when `ffmpeg -filters` lists
//! `cropdetect`.
//!
//! CSV columns (stdout):
//! `cell,invocation,round,variant,frames,wall_ns,events,result_ok,rect_digest,probe_count`

#![cfg(not(miri))]

use ez_ffmpeg::analysis::{Analysis, VideoDetector};
use ez_ffmpeg::Input;
use std::process::Command;
use std::time::Instant;

fn have_ffmpeg() -> bool {
    Command::new("ffmpeg")
        .args(["-nostdin", "-nostats", "-loglevel", "error", "-version"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

/// Black-box probe: the `ffmpeg` on PATH lists the GPL `cropdetect` filter.
fn have_cropdetect() -> bool {
    let output = match Command::new("ffmpeg")
        .args(["-hide_banner", "-filters"])
        .output()
    {
        Ok(o) => o,
        Err(_) => return false,
    };
    if !output.status.success() {
        return false;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    stdout.contains("cropdetect") || stderr.contains("cropdetect")
}

fn lavfi_letterbox(frames: u32, w: u32, h: u32) -> String {
    let bar = h / 8;
    let inner_h = h - 2 * bar;
    format!(
        "color=c=white:s={w}x{inner_h}:r=25:d={},pad={w}:{h}:0:{bar}:black",
        frames as f32 / 25.0
    )
}

#[test]
#[ignore]
fn rust_on_off_letterbox_1080_smoke() {
    let frames = 60u32;
    let graph = lavfi_letterbox(frames, 1920, 1080);
    let input = || Input::from(graph.as_str()).set_format("lavfi");

    // Same native Crop graph as rust_on; limit=0 is the full-frame sentinel
    // (no luma scan). Timing this against rust_on isolates scanner cost.
    let t0 = Instant::now();
    let off = Analysis::new(input())
        .video_detector(VideoDetector::Crop {
            limit: 0,
            round: 16,
            reset: 0,
        })
        .run()
        .expect("rust_off");
    let rust_off_ns = t0.elapsed().as_nanos();
    let off_crop = off.crop.expect("limit=0 full-frame sentinel");
    assert_eq!(off_crop.x, 0);
    assert_eq!(off_crop.y, 0);
    assert_eq!(off_crop.w, 1920);
    assert_eq!(off_crop.h, 1080);

    let t1 = Instant::now();
    let on = Analysis::new(input())
        .video_detector(VideoDetector::Crop {
            limit: 24,
            round: 16,
            reset: 0,
        })
        .run()
        .expect("rust_on");
    let rust_on_ns = t1.elapsed().as_nanos();
    assert!(on.crop.is_some());

    println!(
        "cell,invocation,round,variant,frames,wall_ns,events,result_ok,rect_digest,probe_count"
    );
    println!(
        "1080_letterbox,1,0,rust_off,{frames},{rust_off_ns},1,1,{:?},0",
        off.crop
    );
    println!(
        "1080_letterbox,1,0,rust_on,{frames},{rust_on_ns},1,1,{:?},0",
        on.crop
    );

    if !have_ffmpeg() {
        eprintln!("ffmpeg executable not on PATH; recorded rust_on/rust_off only");
        return;
    }
    if !have_cropdetect() {
        eprintln!(
            "ffmpeg is on PATH but cropdetect is not listed by ffmpeg -filters; skipped ffmpeg half"
        );
        return;
    }

    let t2 = Instant::now();
    let ff_off = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-nostats",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            &graph,
            "-frames:v",
            &frames.to_string(),
            "-f",
            "null",
            "-",
        ])
        .status()
        .expect("spawn ffmpeg off");
    let ff_off_ns = t2.elapsed().as_nanos();
    assert!(ff_off.success(), "ffmpeg baseline (no cropdetect) failed");

    let t3 = Instant::now();
    let ff_on = Command::new("ffmpeg")
        .args([
            "-nostdin",
            "-nostats",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            &graph,
            "-frames:v",
            &frames.to_string(),
            "-vf",
            "cropdetect=mode=black:limit=24:round=16:skip=2:reset=0",
            "-f",
            "null",
            "-",
        ])
        .status()
        .expect("spawn ffmpeg on");
    let ff_on_ns = t3.elapsed().as_nanos();
    assert!(
        ff_on.success(),
        "ffmpeg cropdetect half failed after ffmpeg -filters listed the filter: {ff_on}"
    );
    println!(
        "1080_letterbox,1,0,ff_off,{frames},{ff_off_ns},0,{},none,0",
        i32::from(ff_off.success())
    );
    println!(
        "1080_letterbox,1,0,ff_on,{frames},{ff_on_ns},0,{},none,0",
        i32::from(ff_on.success())
    );
    if rust_on_ns > rust_off_ns && ff_on_ns > ff_off_ns {
        let ratio = (ff_on_ns - ff_off_ns) as f64 / (rust_on_ns - rust_off_ns) as f64;
        println!("paired_ratio={ratio:.3} (ff_marginal/rust_marginal)");
    }
}
