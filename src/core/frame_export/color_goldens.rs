//! Golden color tests — the module's central correctness claim, proven
//! numerically: `ColorPolicy::Tagged` honors the frame's embedded colorspace
//! (YUV→RGB matrix) tag (BT.601 vs BT.709), and `ColorPolicy::Force` overrides it.
//!
//! The colorspace variable is **isolated**: fixtures fix `color_primaries` and
//! `color_trc` at `bt709` and vary ONLY the `colorspace` (matrix) tag, so any
//! RGB divergence is attributable to the matrix alone — not to primaries/transfer
//! (which swscale's YUV→RGB conversion does not apply). Within a single fixture
//! (identical encoded YUV), `Tagged` is compared against `Force(<matrix>)`:
//!   - `Tagged` matches `Force(<the embedded matrix>)` within rounding (<=4/255), and
//!   - diverges sharply (>=20/255 on some channel) from `Force(<the other matrix>)`.
//!
//! Robust across FFmpeg 7/8 (unlike brittle hand-authored sRGB constants): the
//! observed same-matrix diff is 0 and the wrong-matrix divergence is 24-39.
//! Fixtures use `mpeg2video` (writes colour_description) with `setparams`; the
//! whole thing runs under `cargo test --lib`, so it rides both CI FFmpeg lanes
//! with no wiring.

use super::{ColorPolicy, FrameExtractor, PixelLayout, YuvMatrix, YuvRange};
use crate::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::path::{Path, PathBuf};

const LIMITED_601: ColorPolicy = ColorPolicy::Force {
    matrix: YuvMatrix::Bt601,
    range: YuvRange::Limited,
};
const LIMITED_709: ColorPolicy = ColorPolicy::Force {
    matrix: YuvMatrix::Bt709,
    range: YuvRange::Limited,
};

/// A unique scratch directory per test, cleaned on drop.
struct Scratch(PathBuf);
impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "ez_frame_export_goldens_{}_{}",
            std::process::id(),
            tag
        ));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        Self(dir)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Encodes a solid `hex` color whose YUV→RGB `matrix` tag (`colorspace`) is set,
/// while `color_primaries`/`color_trc` are FIXED at bt709 — so only the matrix
/// varies between fixtures. Uses native encoder `codec`. Panics with a clear,
/// encoder-naming message on failure — this doubles as the availability canary.
fn generate(path: &Path, hex: &str, matrix: &str, codec: &str) {
    let input = Input::from(format!("color=c={hex}:s=64x64:r=10:d=0.3")).set_format("lavfi");
    let output = Output::from(path.to_str().unwrap()).set_video_codec(codec);
    let ctx = FfmpegContext::builder()
        .input(input)
        .filter_desc(format!(
            "setparams=colorspace={matrix}:color_primaries=bt709:color_trc=bt709"
        ))
        .output(output)
        .build()
        .unwrap_or_else(|e| panic!("golden fixture build failed (encoder '{codec}'?): {e:?}"));
    FfmpegScheduler::new(ctx)
        .start()
        .and_then(|s| s.wait())
        .unwrap_or_else(|e| {
            panic!(
                "golden fixture encode failed — is '{codec}' available in this FFmpeg build? {e:?}"
            )
        });
}

/// The center-pixel RGB of the first extracted frame under `policy`.
fn center_rgb(path: &Path, policy: ColorPolicy) -> [u8; 3] {
    let frames = FrameExtractor::new(path.to_str().unwrap())
        .color(policy)
        .pixel(PixelLayout::Rgb24)
        .max_frames(1)
        .collect_frames()
        .expect("extraction");
    let f = frames.first().expect("one frame");
    let (w, h) = (f.width() as usize, f.height() as usize);
    let b = f.as_bytes();
    let i = ((h / 2) * w + w / 2) * 3;
    [b[i], b[i + 1], b[i + 2]]
}

fn max_channel_diff(a: [u8; 3], b: [u8; 3]) -> u8 {
    (0..3).map(|i| a[i].abs_diff(b[i])).max().unwrap()
}

#[test]
fn canary_native_encoders_present() {
    // If a lane ever drops these, the failure names the missing encoder here
    // instead of surfacing as a cryptic golden diff.
    let s = Scratch::new("canary");
    generate(&s.path("m2v.mkv"), "0x808080", "bt709", "mpeg2video");
    generate(&s.path("mjpg.mkv"), "0x808080", "bt709", "mjpeg");
    assert!(
        crate::hwaccel::is_filter_available("setparams"),
        "setparams filter missing"
    );
}

#[test]
fn tagged_honors_bt709_matrix() {
    // Identical encoded YUV, colorspace isolated (primaries/trc fixed at bt709):
    // Tagged must read the embedded BT.709 matrix, matching Force(709) and
    // diverging from Force(601).
    let s = Scratch::new("t709");
    let p = s.path("red709.mkv");
    generate(&p, "0xFF0000", "bt709", "mpeg2video");
    let tagged = center_rgb(&p, ColorPolicy::Tagged);
    let forced_709 = center_rgb(&p, LIMITED_709);
    let forced_601 = center_rgb(&p, LIMITED_601);
    assert!(
        max_channel_diff(tagged, forced_709) <= 4,
        "Tagged {tagged:?} should match Force(709) {forced_709:?}"
    );
    assert!(
        max_channel_diff(tagged, forced_601) >= 20,
        "Tagged {tagged:?} vs Force(601) {forced_601:?} must diverge >=20 (the wedge)"
    );
    // Absolute sanity: a red source stays red (catches R/B swap, gross errors).
    assert!(
        tagged[0] > 200 && tagged[1] < 80 && tagged[2] < 80,
        "red stays red: {tagged:?}"
    );
}

#[test]
fn tagged_honors_bt601_matrix() {
    // Same isolation, other matrix: Tagged reads the embedded BT.601 (smpte170m)
    // matrix — NOT a fixed one — so it matches Force(601) and diverges from
    // Force(709).
    let s = Scratch::new("t601");
    let p = s.path("green601.mkv");
    generate(&p, "0x00FF00", "smpte170m", "mpeg2video");
    let tagged = center_rgb(&p, ColorPolicy::Tagged);
    let forced_601 = center_rgb(&p, LIMITED_601);
    let forced_709 = center_rgb(&p, LIMITED_709);
    assert!(
        max_channel_diff(tagged, forced_601) <= 4,
        "Tagged {tagged:?} should match Force(601) {forced_601:?}"
    );
    assert!(
        max_channel_diff(tagged, forced_709) >= 20,
        "Tagged {tagged:?} vs Force(709) {forced_709:?} must diverge >=20"
    );
    assert!(
        tagged[1] > 180 && tagged[0] < 80 && tagged[2] < 80,
        "green stays green: {tagged:?}"
    );
}

#[test]
fn force_applies_the_named_matrix() {
    // The SAME encoded YUV, forced two ways, must differ — proves Force actually
    // drives the conversion rather than passing tags through. Colorspace is the
    // only variable (primaries/trc fixed in the fixture).
    let s = Scratch::new("force");
    let p = s.path("green709.mkv");
    generate(&p, "0x00FF00", "bt709", "mpeg2video");
    let as_709 = center_rgb(&p, LIMITED_709);
    let as_601 = center_rgb(&p, LIMITED_601);
    assert!(
        max_channel_diff(as_709, as_601) >= 20,
        "Force(709) {as_709:?} vs Force(601) {as_601:?} must diverge >=20"
    );
}

#[test]
fn hdr_input_is_typed_error() {
    let s = Scratch::new("hdr");
    let p = s.path("hdr.mkv");
    // BT.2020 non-constant-luminance + PQ transfer = HDR (a distinct fixture:
    // here primaries/trc ARE the HDR markers the resolver rejects).
    let input = Input::from("color=c=0x808080:s=64x64:r=10:d=0.3").set_format("lavfi");
    let output = Output::from(p.to_str().unwrap()).set_video_codec("mpeg2video");
    FfmpegScheduler::new(
        FfmpegContext::builder()
            .input(input)
            .filter_desc("setparams=colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084")
            .output(output)
            .build()
            .expect("hdr fixture build"),
    )
    .start()
    .and_then(|s| s.wait())
    .expect("hdr fixture encode");

    let err = FrameExtractor::new(p.to_str().unwrap())
        .max_frames(1)
        .collect_frames()
        .expect_err("HDR must be rejected");
    assert!(
        matches!(
            err,
            crate::error::Error::FrameExport(super::FrameExportError::HdrRequiresToneMapping)
        ),
        "expected HdrRequiresToneMapping, got {err:?}"
    );
}
