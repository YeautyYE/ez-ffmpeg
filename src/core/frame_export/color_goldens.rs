//! Golden color tests — the module's central correctness claim, proven
//! numerically: `ColorPolicy::Tagged` honors the frame's embedded colorspace
//! (YUV→RGB matrix) tag (BT.601 vs BT.709), `ColorPolicy::Force` overrides it,
//! `ColorPolicy::TaggedOrResolutionGuess` fills only what is untagged, and HDR
//! input — declared or spliced in mid-stream — fails as a typed error.
//!
//! Every assertion is **self-relative**: within a single fixture (identical
//! encoded YUV), the policy under test is compared against `Force(<expected>)`
//! extractions of the SAME file — matching within rounding (<=4/255) on the
//! expected side and diverging sharply (>=20/255 on some channel) from the
//! wrong side. That keeps the vectors robust across FFmpeg 7/8 (no brittle
//! hand-authored sRGB constants; observed same-side diff is 0, wrong-side
//! divergence 24-39). The one exception is `default_tier_matches_cli_bytes`,
//! which compares against an external `ffmpeg` binary — it gates itself on a
//! libswscale build match (strict under `EZ_FFMPEG_CLI`, see `cli_parity_gate`)
//! instead of being self-relative. Everything runs under `cargo test --lib`,
//! so it rides both CI FFmpeg lanes with no wiring.
//!
//! Fixture families:
//! - **Tagged matrix fixtures** (`mpeg2video` + `setparams`): `colorspace`
//!   varies, `color_primaries`/`color_trc` are FIXED at `bt709`, so RGB
//!   divergence is attributable to the matrix alone.
//! - **Untagged fixtures** (`rawvideo` in NUT): ALL color properties cleared
//!   to unknown — the input the resolution guess exists for.
//! - **Full-range fixture** (`rawvideo` in MKV): range tagged full over
//!   range-agnostic yuv420p, matrix cleared — proves the guess never clobbers
//!   a real range tag.
//! - **Splice fixtures** (byte-concatenated `mpeg2video` elementary streams):
//!   color properties CHANGE mid-stream — the SDR→HDR splice varies
//!   matrix/primaries/transfer together (the runtime-guard scenario), and the
//!   untagged→tagged splice flips only the matrix tag (the no-freezing
//!   contract).

use super::{
    ColorPolicy, ConversionPrecision, FrameExportError, FrameExtractor, PixelLayout, Sampling,
    YuvMatrix, YuvRange,
};
use crate::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::path::{Path, PathBuf};
use std::process::Command;

const GUESS: ColorPolicy = ColorPolicy::TaggedOrResolutionGuess;

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
    generate_sized(path, hex, matrix, codec, 64, 64);
}

/// [`generate`] with an explicit frame size (the resolution-guess tests need
/// SD- and HD-sized fixtures).
fn generate_sized(path: &Path, hex: &str, matrix: &str, codec: &str, w: u32, h: u32) {
    let input = Input::from(format!("color=c={hex}:s={w}x{h}:r=10:d=0.3")).set_format("lavfi");
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

/// Encodes a solid color as UNTAGGED yuv420p rawvideo in a NUT container: the
/// rawvideo bitstream carries no color metadata and `setparams=...unknown`
/// clears whatever the generation-side conversion tagged, so decoded frames
/// come back with UNSPECIFIED colorspace/range — the input the resolution
/// guess exists for. Panics name the encoder/muxer (availability canary).
fn generate_untagged(path: &Path, hex: &str, w: u32, h: u32) {
    let input = Input::from(format!("color=c={hex}:s={w}x{h}:r=10:d=0.3")).set_format("lavfi");
    let output = Output::from(path.to_str().unwrap())
        .set_format("nut")
        .set_video_codec("rawvideo");
    let ctx = FfmpegContext::builder()
        .input(input)
        .filter_desc(
            "format=yuv420p,setparams=range=unknown:colorspace=unknown:\
             color_primaries=unknown:color_trc=unknown",
        )
        .output(output)
        .build()
        .unwrap_or_else(|e| panic!("untagged fixture build failed (rawvideo/nut?): {e:?}"));
    FfmpegScheduler::new(ctx)
        .start()
        .and_then(|s| s.wait())
        .unwrap_or_else(|e| {
            panic!("untagged fixture encode failed — are 'rawvideo' and 'nut' available? {e:?}")
        });
}

/// The center-pixel RGB of the first extracted frame under `policy`, pinned
/// to [`ConversionPrecision::Standard`] explicitly — the legacy goldens must
/// not move if the enum's `Default` ever changes.
fn center_rgb(path: &Path, policy: ColorPolicy) -> [u8; 3] {
    center_rgb_at(path, policy, ConversionPrecision::Standard)
}

/// [`center_rgb`] with an explicit conversion-precision tier.
fn center_rgb_at(path: &Path, policy: ColorPolicy, precision: ConversionPrecision) -> [u8; 3] {
    let frames = FrameExtractor::new(path.to_str().unwrap())
        .color(policy)
        .conversion_precision(precision)
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
    generate_untagged(&s.path("raw.nut"), "0x808080", 64, 64);
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
fn resolution_guess_untagged_sd_is_bt601() {
    // Untagged 640x480: the per-frame guess must fill in BT.601, so the guess
    // policy matches Force(601) on the SAME encoded YUV and diverges from
    // Force(709). A reverted stamp would fall through to swscale's 601 default
    // here — the HD twin below is the side that catches that revert.
    let s = Scratch::new("guess_sd");
    let p = s.path("red_sd.nut");
    generate_untagged(&p, "0xFF0000", 640, 480);
    let guessed = center_rgb(&p, GUESS);
    let forced_601 = center_rgb(&p, LIMITED_601);
    let forced_709 = center_rgb(&p, LIMITED_709);
    assert!(
        max_channel_diff(guessed, forced_601) <= 4,
        "SD guess {guessed:?} should match Force(601) {forced_601:?}"
    );
    assert!(
        max_channel_diff(guessed, forced_709) >= 20,
        "SD guess {guessed:?} vs Force(709) {forced_709:?} must diverge >=20"
    );
    assert!(
        guessed[0] > 200 && guessed[1] < 80 && guessed[2] < 80,
        "red stays red: {guessed:?}"
    );
}

#[test]
fn resolution_guess_untagged_hd_is_bt709() {
    // Untagged 1920x1080: the guess must fill in BT.709. This is the
    // discriminating side: without the per-frame stamp, untagged HD falls
    // through to swscale's BT.601 default and this assertion goes red.
    let s = Scratch::new("guess_hd");
    let p = s.path("green_hd.nut");
    generate_untagged(&p, "0x00FF00", 1920, 1080);
    let guessed = center_rgb(&p, GUESS);
    let forced_709 = center_rgb(&p, LIMITED_709);
    let forced_601 = center_rgb(&p, LIMITED_601);
    assert!(
        max_channel_diff(guessed, forced_709) <= 4,
        "HD guess {guessed:?} should match Force(709) {forced_709:?}"
    );
    assert!(
        max_channel_diff(guessed, forced_601) >= 20,
        "HD guess {guessed:?} vs Force(601) {forced_601:?} must diverge >=20"
    );
    assert!(
        guessed[1] > 180 && guessed[0] < 80 && guessed[2] < 80,
        "green stays green: {guessed:?}"
    );
}

#[test]
fn resolution_guess_never_overrides_tags() {
    // A 1080p fixture EXPLICITLY tagged BT.601 (smpte170m): the resolution
    // guess says 709 for this height, but real tags must always win — the
    // guess policy has to match Force(601) and diverge from Force(709).
    let s = Scratch::new("guess_tagged");
    let p = s.path("green601_hd.mkv");
    generate_sized(&p, "0x00FF00", "smpte170m", "mpeg2video", 1920, 1080);
    let guessed = center_rgb(&p, GUESS);
    let forced_601 = center_rgb(&p, LIMITED_601);
    let forced_709 = center_rgb(&p, LIMITED_709);
    assert!(
        max_channel_diff(guessed, forced_601) <= 4,
        "tagged-601 HD under the guess {guessed:?} must follow the TAG, \
         matching Force(601) {forced_601:?}"
    );
    assert!(
        max_channel_diff(guessed, forced_709) >= 20,
        "tagged-601 HD under the guess {guessed:?} vs Force(709) {forced_709:?} \
         must diverge >=20 (a resolution guess overriding tags would collapse this)"
    );
}

#[test]
fn resolution_guess_preserves_full_range_tag() {
    // A REAL full-range tag with an untagged matrix: the guess may fill the
    // matrix but must NOT clobber the range — the limited pin applies only to
    // UNSPECIFIED range. If the guard overwrote the tag, the guess extraction
    // would collapse onto the limited reference. The carrier is rawvideo in
    // MKV: the range rides the container's colour metadata over plain yuv420p,
    // a range-AGNOSTIC pixel format, so the forced references actually differ
    // (an mjpeg/yuvj carrier would bake full-range into the pixel format and
    // make `in_range` moot).
    let s = Scratch::new("guess_range");
    let p = s.path("green_full.mkv");
    let input = Input::from("color=c=0x00FF00:s=64x64:r=10:d=0.3").set_format("lavfi");
    let output = Output::from(p.to_str().unwrap()).set_video_codec("rawvideo");
    let ctx = FfmpegContext::builder()
        .input(input)
        // Tag range FULL, clear everything else — matrix stays untagged so the
        // guess runs on this fixture too.
        .filter_desc(
            "format=yuv420p,setparams=range=pc:colorspace=unknown:\
             color_primaries=unknown:color_trc=unknown",
        )
        .output(output)
        .build()
        .expect("full-range fixture build");
    FfmpegScheduler::new(ctx)
        .start()
        .and_then(|s| s.wait())
        .expect("full-range fixture encode (rawvideo/mkv)");

    const FULL_601: ColorPolicy = ColorPolicy::Force {
        matrix: YuvMatrix::Bt601,
        range: YuvRange::Full,
    };
    let guessed = center_rgb(&p, GUESS);
    let full_601 = center_rgb(&p, FULL_601);
    let limited_601 = center_rgb(&p, LIMITED_601);
    assert!(
        max_channel_diff(guessed, full_601) <= 4,
        "guess {guessed:?} must keep the full-range tag, matching \
         Force(601, Full) {full_601:?}"
    );
    assert!(
        max_channel_diff(guessed, limited_601) >= 10,
        "guess {guessed:?} vs Force(601, Limited) {limited_601:?} must diverge \
         (a guard that clobbers the range tag collapses this)"
    );
}

/// Encodes one mpeg2video ELEMENTARY-STREAM segment (`.m2v`, no container) so
/// its colour tags live in the bitstream's sequence display extension — the
/// only place that survives a byte-level splice.
fn generate_m2v_segment(path: &Path, hex: &str, size: &str, setparams: &str) {
    let input = Input::from(format!("color=c={hex}:s={size}:r=10:d=0.5")).set_format("lavfi");
    let output = Output::from(path.to_str().unwrap())
        .set_format("mpeg2video")
        .set_video_codec("mpeg2video");
    let ctx = FfmpegContext::builder()
        .input(input)
        .filter_desc(setparams.to_string())
        .output(output)
        .build()
        .expect("m2v segment build");
    FfmpegScheduler::new(ctx)
        .start()
        .and_then(|s| s.wait())
        .expect("m2v segment encode");
}

/// Byte-concatenates two elementary-stream segments into `out`.
fn splice(seg1: &Path, seg2: &Path, out: &Path) {
    let mut bytes = std::fs::read(seg1).expect("read first segment");
    bytes.extend(std::fs::read(seg2).expect("read second segment"));
    std::fs::write(out, bytes).expect("write spliced stream");
}

#[test]
fn resolution_guess_tracks_mid_stream_tagging() {
    // The no-freezing contract: an UNTAGGED 720p head is guessed BT.709, and
    // when the stream turns TAGGED BT.601 mid-decode (new sequence header
    // after a byte splice), the tag must win from that frame on. A guard that
    // cached its first guess — or a graph that never re-reads the changed
    // colorspace — would keep converting the tail as 709 and go red on the
    // last-frame assertions.
    let s = Scratch::new("guess_transition");
    let untagged = s.path("untagged.m2v");
    let tagged = s.path("tagged601.m2v");
    generate_m2v_segment(
        &untagged,
        "0x00FF00",
        "1280x720",
        "setparams=range=unknown:colorspace=unknown:color_primaries=unknown:color_trc=unknown",
    );
    generate_m2v_segment(
        &tagged,
        "0x00FF00",
        "1280x720",
        "setparams=colorspace=smpte170m:color_primaries=bt709:color_trc=bt709",
    );
    let spliced = s.path("spliced.m2v");
    splice(&untagged, &tagged, &spliced);

    let extract = |policy: ColorPolicy| -> Vec<[u8; 3]> {
        let frames = FrameExtractor::new(spliced.to_str().unwrap())
            .color(policy)
            .pixel(PixelLayout::Rgb24)
            .collect_frames()
            .expect("spliced extraction");
        frames
            .iter()
            .map(|f| {
                let (w, h) = (f.width() as usize, f.height() as usize);
                let b = f.as_bytes();
                let i = ((h / 2) * w + w / 2) * 3;
                [b[i], b[i + 1], b[i + 2]]
            })
            .collect()
    };
    let guessed = extract(GUESS);
    let as_709 = extract(LIMITED_709);
    let as_601 = extract(LIMITED_601);
    assert_eq!(guessed.len(), as_709.len());
    assert_eq!(guessed.len(), as_601.len());
    assert!(
        guessed.len() >= 2,
        "need frames on both sides of the splice"
    );

    let first = 0;
    let last = guessed.len() - 1;
    // Head: untagged 720p => guessed BT.709.
    assert!(
        max_channel_diff(guessed[first], as_709[first]) <= 4,
        "untagged head {:?} must be guessed 709 {:?}",
        guessed[first],
        as_709[first]
    );
    assert!(
        max_channel_diff(guessed[first], as_601[first]) >= 20,
        "untagged head {:?} vs 601 {:?} must diverge",
        guessed[first],
        as_601[first]
    );
    // Tail: tagged BT.601 => the tag wins over the earlier guess.
    assert!(
        max_channel_diff(guessed[last], as_601[last]) <= 4,
        "tagged tail {:?} must follow the 601 tag {:?} (guess must not freeze)",
        guessed[last],
        as_601[last]
    );
    assert!(
        max_channel_diff(guessed[last], as_709[last]) >= 20,
        "tagged tail {:?} vs 709 {:?} must diverge (a frozen first guess \
         collapses this)",
        guessed[last],
        as_709[last]
    );
}

#[test]
fn mid_stream_hdr_splice_is_typed_runtime_error() {
    // The SDR→HDR splice scenario (MPEG-TS-style ad insertion): the file's
    // DECLARED parameters are SDR (the probe reads the leading segment), so
    // the open-time check passes — only the per-frame runtime guard can catch
    // the switch. Two mpeg2 elementary streams are byte-concatenated; the
    // decoder re-reads the second sequence header mid-stream and starts
    // emitting BT.2020/PQ-tagged frames, which must surface as the typed HDR
    // error in EVERY sampling mode (the guard sits before mode-specific
    // handling). Reverting the guard makes these runs complete cleanly, which
    // fails the expect below.
    let s = Scratch::new("splice");
    let sdr = s.path("sdr.m2v");
    let hdr = s.path("hdr.m2v");
    generate_m2v_segment(
        &sdr,
        "0x808080",
        "64x64",
        "setparams=colorspace=bt709:color_primaries=bt709:color_trc=bt709",
    );
    generate_m2v_segment(
        &hdr,
        "0x808080",
        "64x64",
        "setparams=colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
    );
    let spliced = s.path("spliced.m2v");
    splice(&sdr, &hdr, &spliced);

    let modes: [Sampling; 5] = [
        Sampling::All,
        Sampling::EveryNth(2),
        Sampling::EverySec(0.2),
        Sampling::KeyframesOnly,
        // Elementary streams expose no container duration; the hint also keeps
        // the run bounded.
        Sampling::UniformN(4),
    ];
    for mode in modes {
        let mut extractor = FrameExtractor::new(spliced.to_str().unwrap()).sampling(mode);
        if matches!(mode, Sampling::UniformN(_)) {
            extractor = extractor.duration_hint_us(1_000_000);
        }
        // Open-time must SUCCEED (declared parameters are SDR) — the error has
        // to come from the runtime guard, through the iterator.
        let iter = extractor
            .frames()
            .unwrap_or_else(|e| panic!("open must see the SDR head ({mode:?}): {e:?}"));
        let mut delivered = 0usize;
        let mut terminal = None;
        for item in iter {
            match item {
                Ok(_) => delivered += 1,
                Err(e) => terminal = Some(e),
            }
        }
        let err = terminal.unwrap_or_else(|| {
            panic!(
                "run completed cleanly ({mode:?}, {delivered} frames) — \
                 the mid-stream HDR splice was not detected"
            )
        });
        assert!(
            matches!(
                err,
                crate::error::Error::FrameExport(FrameExportError::HdrRequiresToneMapping)
            ),
            "expected typed HdrRequiresToneMapping for {mode:?}, got {err:?}"
        );
    }
}

#[test]
fn high_precision_preserves_matrix_attribution() {
    // The opt-in High tier changes rounding/interpolation, never color
    // interpretation: the self-relative matrix wedge must hold under it
    // exactly as it does under the default tier, and on flat color (no chroma
    // edges, where the tiers' chroma reconstruction cannot differ) the two
    // tiers must agree within the same rounding tolerance.
    let s = Scratch::new("high_tier");
    let p = s.path("red709.mkv");
    generate(&p, "0xFF0000", "bt709", "mpeg2video");
    let high_tagged = center_rgb_at(&p, ColorPolicy::Tagged, ConversionPrecision::High);
    let high_709 = center_rgb_at(&p, LIMITED_709, ConversionPrecision::High);
    let high_601 = center_rgb_at(&p, LIMITED_601, ConversionPrecision::High);
    assert!(
        max_channel_diff(high_tagged, high_709) <= 4,
        "High-tier Tagged {high_tagged:?} should match High-tier Force(709) {high_709:?}"
    );
    assert!(
        max_channel_diff(high_tagged, high_601) >= 20,
        "High-tier Tagged {high_tagged:?} vs Force(601) {high_601:?} must diverge >=20"
    );
    // Cross-tier pin: the tiers may differ by rounding, nothing more, on flat
    // color — a High tier that changed the MATRIX would blow this tolerance.
    let default_tagged = center_rgb(&p, ColorPolicy::Tagged);
    assert!(
        max_channel_diff(high_tagged, default_tagged) <= 4,
        "High tier {high_tagged:?} vs default tier {default_tagged:?} must agree \
         within rounding on flat color"
    );
}

/// Runtime pin for the High-tier plumbing: on gradient-heavy content the two
/// tiers must produce DIFFERENT bytes somewhere. The descriptor unit test
/// proves the flag string is constructed; this proves the builder value
/// actually reaches the graph — a regression that dropped
/// `conversion_precision` on the way to `build_filter_desc` would make the
/// tiers byte-identical here. Self-relative (tier vs tier on one fixture), so
/// no dependency on a particular swscale build.
#[test]
fn high_tier_diverges_from_standard_at_runtime() {
    let s = Scratch::new("tier_divergence");
    let p = s.path("moving709.mkv");
    let input = Input::from("testsrc2=s=320x240:r=10:d=0.5").set_format("lavfi");
    let output = Output::from(p.to_str().unwrap()).set_video_codec("mpeg2video");
    let ctx = FfmpegContext::builder()
        .input(input)
        .filter_desc("setparams=colorspace=bt709:color_primaries=bt709:color_trc=bt709")
        .output(output)
        .build()
        .expect("divergence fixture build");
    FfmpegScheduler::new(ctx)
        .start()
        .and_then(|sch| sch.wait())
        .expect("divergence fixture encode");

    let bytes_at = |precision| {
        FrameExtractor::new(p.to_str().unwrap())
            .conversion_precision(precision)
            .pixel(PixelLayout::Rgb24)
            .collect_frames()
            .expect("extraction")
            .iter()
            .flat_map(|f| f.as_bytes().to_vec())
            .collect::<Vec<u8>>()
    };
    let standard = bytes_at(ConversionPrecision::Standard);
    let high = bytes_at(ConversionPrecision::High);
    assert!(!standard.is_empty(), "fixture produced no frames");
    assert_eq!(standard.len(), high.len(), "same geometry and frame count");
    assert_ne!(
        standard, high,
        "High must change bytes on chroma edges — identical output means the \
         precision knob never reached the filter graph"
    );
}

/// How the CLI byte-parity golden resolves its reference binary.
///
/// Byte parity is only meaningful when the CLI links the exact libswscale
/// build the crate links — a different build legitimately produces different
/// bytes, and asserting against it would test version drift, not our default.
/// The gate has two lanes:
///
/// * `EZ_FFMPEG_CLI=<path>` (the CI lane): STRICT. Every failure — missing
///   binary, unparsable `-version` banner, libswscale mismatch, failing
///   reference run — fails the test. This is the enforcing configuration.
/// * Unset (developer machines): `ffmpeg` from PATH is probed. Exactly two
///   conditions skip (with a stderr note): the binary is absent, or its
///   libswscale differs from the crate's. Anything else — a probe error, a
///   nonzero `-version` exit, an unparsable banner, a failing reference
///   run — is a harness defect and fails loudly.
enum CliGate {
    Run { bin: String },
    Skip(String),
}

fn cli_parity_gate() -> CliGate {
    use std::io::ErrorKind;
    let pinned = std::env::var("EZ_FFMPEG_CLI").ok();
    let strict = pinned.is_some();
    let bin = pinned.unwrap_or_else(|| "ffmpeg".to_string());
    let out = match Command::new(&bin).arg("-version").output() {
        Err(e) if e.kind() == ErrorKind::NotFound && !strict => {
            return CliGate::Skip(format!("no `{bin}` on PATH"));
        }
        Err(e) => panic!("ffmpeg CLI probe `{bin} -version` failed: {e}"),
        Ok(o) if !o.status.success() => panic!("`{bin} -version` exited with {}", o.status),
        Ok(o) => o,
    };
    let banner = String::from_utf8_lossy(&out.stdout).into_owned();
    let cli = parse_swscale_version(&banner).unwrap_or_else(|| {
        panic!("could not parse a libswscale version out of `{bin} -version` output")
    });
    let ours = linked_swscale_version();
    if cli != ours {
        let msg = format!(
            "`{bin}` links libswscale {}.{}.{}, the crate links {}.{}.{} — \
             byte parity is only defined on the same build",
            cli.0, cli.1, cli.2, ours.0, ours.1, ours.2
        );
        if strict {
            panic!("{msg} (point EZ_FFMPEG_CLI at a matching binary)");
        }
        return CliGate::Skip(msg);
    }
    CliGate::Run { bin }
}

/// The libswscale version the crate itself links, as (major, minor, micro).
fn linked_swscale_version() -> (u32, u32, u32) {
    // SAFETY: plain version query, no preconditions.
    let v = unsafe { ffmpeg_sys_next::swscale_version() };
    (v >> 16, (v >> 8) & 0xff, v & 0xff)
}

/// Parses the `libswscale  a. b.  c /  a. b.  c` line out of `ffmpeg
/// -version` output. The triple RIGHT of the `/` is the version the binary
/// actually loads at run time (left is its compile-time header version);
/// parity is defined against the run-time one, matching our own
/// `swscale_version()` query. A line without `/` falls back to its only
/// triple.
fn parse_swscale_version(banner: &str) -> Option<(u32, u32, u32)> {
    let line = banner
        .lines()
        .find(|l| l.trim_start().starts_with("libswscale"))?;
    let runtime = line.rsplit('/').next()?;
    let digits: String = runtime
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.')
        .collect();
    let mut it = digits.split('.').filter(|s| !s.is_empty());
    let major = it.next()?.parse().ok()?;
    let minor = it.next()?.parse().ok()?;
    let micro = it.next()?.parse().ok()?;
    Some((major, minor, micro))
}

#[test]
fn parse_swscale_version_prefers_the_runtime_triple() {
    // Header (left of `/`) and run-time (right) triples deliberately
    // differ everywhere so a parser reading the wrong side cannot pass.
    let banner = "ffmpeg version 7.1.3\nlibswscale      8.  1.100 /  9. 13.  2\n";
    assert_eq!(parse_swscale_version(banner), Some((9, 13, 2)));
}

#[test]
fn parse_swscale_version_falls_back_without_a_slash() {
    let banner = "configuration: --enable-shared\nlibswscale 7.5.100\n";
    assert_eq!(parse_swscale_version(banner), Some((7, 5, 100)));
}

#[test]
fn parse_swscale_version_rejects_garbage() {
    // No libswscale line at all.
    assert_eq!(parse_swscale_version("ffmpeg version 7.1.3"), None);
    // A line with no digits on either side of the slash.
    assert_eq!(parse_swscale_version("libswscale garbage / nonsense"), None);
    // Slash present but nothing after it.
    assert_eq!(parse_swscale_version("libswscale 8.1.100 /"), None);
    // Incomplete triple: major.minor only.
    assert_eq!(parse_swscale_version("libswscale 8.1 / 8.1"), None);
}

#[test]
fn default_tier_matches_cli_bytes() {
    // The strongest pin on the CLI-consistent default: over a moving,
    // gradient-heavy clip (testsrc2 — solid color would pass trivially), the
    // default tier must produce byte-for-byte the output of the ffmpeg CLI
    // running the equivalent chain with DEFAULT scaler flags (no flags token
    // at all). This holds because an explicit `flags=bicubic` is
    // byte-identical to swscale's CLI default configuration. `cli_parity_gate`
    // only lets the comparison run against a CLI linking the crate's own
    // libswscale build (see its docs); under `EZ_FFMPEG_CLI` it never skips.
    // Once the gate passes, EVERY failure below asserts — a broken reference
    // run is a harness defect, not a skip.
    let bin = match cli_parity_gate() {
        CliGate::Run { bin } => bin,
        CliGate::Skip(why) => {
            eprintln!("skipping default_tier_matches_cli_bytes: {why}");
            return;
        }
    };
    let s = Scratch::new("cli_parity");
    let p = s.path("moving709.mkv");
    let input = Input::from("testsrc2=s=320x240:r=10:d=1").set_format("lavfi");
    let output = Output::from(p.to_str().unwrap()).set_video_codec("mpeg2video");
    let ctx = FfmpegContext::builder()
        .input(input)
        .filter_desc("setparams=colorspace=bt709:color_primaries=bt709:color_trc=bt709")
        .output(output)
        .build()
        .expect("parity fixture build");
    FfmpegScheduler::new(ctx)
        .start()
        .and_then(|sch| sch.wait())
        .expect("parity fixture encode");

    // Reference: the CLI with default scaler flags, vsync passthrough (the
    // extractor's own frame-delivery discipline), rawvideo out.
    let raw = s.path("cli.raw");
    let reference = Command::new(&bin)
        .args([
            "-y",
            "-v",
            "error",
            "-i",
            p.to_str().unwrap(),
            "-vf",
            "scale=iw:ih:in_color_matrix=auto:in_range=auto:out_range=full,format=rgb24",
            "-fps_mode",
            "passthrough",
            "-f",
            "rawvideo",
            raw.to_str().unwrap(),
        ])
        .output()
        .expect("spawn the reference ffmpeg run");
    assert!(
        reference.status.success(),
        "reference ffmpeg run failed with {}:\n{}",
        reference.status,
        String::from_utf8_lossy(&reference.stderr)
    );
    let cli_bytes = std::fs::read(&raw).expect("read CLI rawvideo output");

    let frames = FrameExtractor::new(p.to_str().unwrap())
        .pixel(PixelLayout::Rgb24)
        .collect_frames()
        .expect("default-tier extraction");
    assert!(!frames.is_empty(), "parity fixture produced no frames");
    let ez_total: usize = frames.iter().map(|f| f.as_bytes().len()).sum();
    assert_eq!(
        ez_total,
        cli_bytes.len(),
        "frame count / geometry must match the CLI ({} frames of {} bytes)",
        frames.len(),
        frames[0].as_bytes().len()
    );
    let mut offset = 0usize;
    for f in &frames {
        let bytes = f.as_bytes();
        assert_eq!(
            bytes,
            &cli_bytes[offset..offset + bytes.len()],
            "frame {} diverges from the CLI reference bytes",
            f.index()
        );
        offset += bytes.len();
    }
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
