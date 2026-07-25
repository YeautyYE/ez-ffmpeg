//! Color-tag detection and routing for the `hdr_to_sdr` cookbook recipe.
//!
//! Two layers are pinned here:
//!
//! - **Routing logic** (`routing.rs`, shared with the example via `#[path]`):
//!   the transfer-first classifier and the capability-aware backend choice,
//!   tested as pure functions — including the **T5 regression** (a wide-gamut
//!   SDR clip must not be tone-mapped) and the fix for wide-gamut SDR not
//!   needing `tonemap`.
//! - **Library metadata + end-to-end routing**: the new [`StreamInfo::Video`]
//!   color fields are populated from real fixtures, and the classifier is run
//!   on those read-back tags so a primaries-first regression fails here, not
//!   just in the example's own unit tests.
//!
//! The `zscale` tone-map happy-path is asserted only when this FFmpeg build has
//! the filters compiled in (they need libzimg); otherwise it is skipped, the
//! same way the crate's other optional-filter tests skip. All fixtures use the
//! native `mpeg2video` encoder so they build on the minimal CI FFmpeg (no
//! libx264/libx265 dependency), matching the `color_goldens` fixture pattern.

mod common;

#[path = "../examples/hdr_to_sdr/routing.rs"]
mod routing;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::hwaccel::is_filter_available;
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use ffmpeg_sys_next::{AVColorPrimaries, AVColorSpace, AVColorTransferCharacteristic};
use routing::{backend_from_caps, build_chain, classify, tone_map_chain, Backend, ColorKind};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_hdr_sdr_tests", name)
}

/// Encodes a short fixture whose frames carry the given `setparams` color tags
/// with the native `mpeg2video` encoder, whose sequence-display-extension color
/// description survives into the container for probing. No external encoder
/// (libx264/libx265) is needed, so this runs on the minimal CI FFmpeg build.
fn color_fixture(name: &str, setparams: &str) -> String {
    let path = tmp_path(name);
    let context = FfmpegContext::builder()
        .input(Input::from("testsrc2=size=64x64:rate=25").set_format("lavfi"))
        .filter_desc(format!("format=yuv420p,setparams={setparams}"))
        .output(
            Output::from(path.as_str())
                .set_video_codec("mpeg2video")
                .set_recording_time_us(200_000),
        )
        .build()
        .unwrap();
    wait_with_watchdog(context.start().unwrap(), 60, name).unwrap();
    path
}

/// Reads the three color fields off the first video stream.
fn video_color(path: &str) -> (i32, i32, i32) {
    match find_video_stream_info(path)
        .expect("probe")
        .expect("has video stream")
    {
        StreamInfo::Video {
            color_transfer,
            color_primaries,
            color_space,
            ..
        } => (color_transfer, color_primaries, color_space),
        other => panic!("expected video stream, got {other:?}"),
    }
}

const PQ: i32 = AVColorTransferCharacteristic::AVCOL_TRC_SMPTE2084 as i32;
const HLG: i32 = AVColorTransferCharacteristic::AVCOL_TRC_ARIB_STD_B67 as i32;
const TRC_709: i32 = AVColorTransferCharacteristic::AVCOL_TRC_BT709 as i32;
const PRI_2020: i32 = AVColorPrimaries::AVCOL_PRI_BT2020 as i32;
const PRI_709: i32 = AVColorPrimaries::AVCOL_PRI_BT709 as i32;
const SPC_2020: i32 = AVColorSpace::AVCOL_SPC_BT2020_NCL as i32;
const SPC_709: i32 = AVColorSpace::AVCOL_SPC_BT709 as i32;

// ---------------------------------------------------------------------------
// Pure routing logic (no FFmpeg execution)
// ---------------------------------------------------------------------------

#[test]
fn classify_routes_on_transfer_first() {
    assert_eq!(classify(PQ, PRI_2020, SPC_2020), ColorKind::Pq);
    assert_eq!(classify(HLG, PRI_2020, SPC_2020), ColorKind::Hlg);
    assert_eq!(classify(TRC_709, PRI_709, SPC_709), ColorKind::Sdr);
    // Untagged (unspecified everything) is treated as SDR.
    assert_eq!(classify(2, 2, 2), ColorKind::Sdr);
}

/// T5 regression (pure): a wide-gamut SDR clip (BT.709 transfer + BT.2020
/// primaries) must classify as wide-gamut SDR, NOT HDR, and its chain must not
/// tone-map. Routing on primaries instead of transfer would break this.
#[test]
fn wide_gamut_sdr_is_not_tone_mapped() {
    let kind = classify(TRC_709, PRI_2020, SPC_2020);
    assert_eq!(kind, ColorKind::WideGamutSdr, "must not be HDR");
    for backend in [Backend::Zscale, Backend::Libplacebo] {
        let chain = build_chain(kind, backend, 10).expect("wide-gamut has a chain");
        assert!(
            !chain.contains("tonemap"),
            "{backend:?} wide-gamut chain must not tone-map: {chain}"
        );
    }
}

/// Fix for "wide-gamut SDR unnecessarily requires tonemap": on a build with
/// `zscale` but no `tonemap`, wide-gamut SDR must still resolve to a backend,
/// while PQ (which needs the tone-map curve) must not.
#[test]
fn gamut_only_does_not_require_tonemap_capability() {
    // zscale present, tonemap absent, libplacebo absent.
    assert_eq!(
        backend_from_caps(ColorKind::WideGamutSdr, false, true, false, false),
        Some(Backend::Zscale),
        "wide-gamut SDR needs only zscale"
    );
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, true, false, false),
        None,
        "PQ needs tonemap, which is absent"
    );
    // With tonemap present, PQ resolves.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, true, true, false),
        Some(Backend::Zscale)
    );
    // No filters at all: nothing usable.
    assert_eq!(
        backend_from_caps(ColorKind::WideGamutSdr, false, false, false, false),
        None
    );
    // prefer_gpu picks libplacebo when present.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, true, true, true, true),
        Some(Backend::Libplacebo)
    );
}

#[test]
fn tone_map_chain_carries_the_anti_gray_parameters() {
    let cpu = tone_map_chain(Backend::Zscale, 10);
    assert!(cpu.contains("zscale=t=linear"), "must linearize: {cpu}");
    assert!(cpu.contains("desat=0"), "must disable desat: {cpu}");
    assert!(cpu.contains("peak=10"), "must set explicit peak: {cpu}");
    assert!(cpu.contains("r=tv"), "must re-tag limited range: {cpu}");

    let gpu = tone_map_chain(Backend::Libplacebo, 10);
    assert!(gpu.contains("tonemapping=bt.2390"), "bt.2390: {gpu}");
    assert!(gpu.contains("tonemapping_param=0.5"), "report knee: {gpu}");
    assert!(gpu.contains("range=tv"), "limited range: {gpu}");
}

#[test]
fn peak_scales_into_the_chain() {
    // A 4000-nit master normalizes to peak=40.
    assert!(tone_map_chain(Backend::Zscale, 40).contains("peak=40"));
}

// ---------------------------------------------------------------------------
// Library metadata + end-to-end routing on real fixtures
// ---------------------------------------------------------------------------

#[test]
fn detects_pq_and_routes_to_tone_map() {
    let f = color_fixture(
        "pq.mkv",
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
    );
    let (trc, pri, spc) = video_color(&f);
    assert_eq!(trc, PQ, "PQ transfer must be detected");
    assert_eq!(pri, PRI_2020, "PQ fixture carries BT.2020 primaries");
    // Route on the actual read-back tags.
    assert_eq!(classify(trc, pri, spc), ColorKind::Pq);
}

#[test]
fn detects_hlg_and_routes_to_tone_map() {
    let f = color_fixture(
        "hlg.mkv",
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=arib-std-b67",
    );
    let (trc, pri, spc) = video_color(&f);
    assert_eq!(trc, HLG, "HLG transfer must be detected");
    assert_eq!(classify(trc, pri, spc), ColorKind::Hlg);
}

/// T5 regression (end-to-end): a wide-gamut SDR fixture must probe as an SDR
/// transfer AND route to a non-tone-mapping chain. This runs the real
/// classifier on the tags read back from a real file, so a primaries-first
/// regression fails here.
#[test]
fn wide_gamut_sdr_fixture_routes_gamut_only() {
    let f = color_fixture(
        "sdr_bt2020.mkv",
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=bt709",
    );
    let (trc, pri, spc) = video_color(&f);
    assert_ne!(trc, PQ, "must not look like PQ HDR");
    assert_ne!(trc, HLG, "must not look like HLG HDR");
    assert_eq!(trc, TRC_709, "transfer is SDR BT.709");
    assert_eq!(pri, PRI_2020, "primaries ARE wide-gamut (the trap)");

    let kind = classify(trc, pri, spc);
    assert_eq!(kind, ColorKind::WideGamutSdr, "must route as wide-gamut SDR");
    for backend in [Backend::Zscale, Backend::Libplacebo] {
        let chain = build_chain(kind, backend, 10).expect("wide-gamut has a chain");
        assert!(!chain.contains("tonemap"), "must not tone-map: {chain}");
    }
}

/// End-to-end tone-map happy-path: run the CPU chain through the real
/// ez-ffmpeg pipeline and confirm the output is re-tagged BT.709. Skipped when
/// the linked FFmpeg has no `zscale`/`tonemap` (they need libzimg).
#[test]
fn zscale_tone_map_retags_output_bt709() {
    if !(is_filter_available("zscale") && is_filter_available("tonemap")) {
        eprintln!(
            "skipping zscale_tone_map_retags_output_bt709: linked FFmpeg lacks \
             zscale/tonemap (needs libzimg)"
        );
        return;
    }

    let hdr = color_fixture(
        "pq_for_tonemap.mkv",
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
    );
    let out = tmp_path("tonemapped_sdr.mkv");

    // The canonical CPU chain, built from the shared routing logic.
    let chain = tone_map_chain(Backend::Zscale, 10);

    let context = FfmpegContext::builder()
        .input(hdr.as_str())
        .output(
            Output::from(out.as_str())
                .set_video_filter(chain)
                .set_video_codec("mpeg2video")
                .set_recording_time_us(200_000),
        )
        .build()
        .unwrap();
    wait_with_watchdog(context.start().unwrap(), 60, "zscale tone-map").unwrap();

    let (trc, pri, spc) = video_color(&out);
    assert_eq!(trc, TRC_709, "output transfer must be BT.709");
    assert_eq!(pri, PRI_709, "output primaries must be BT.709");
    assert_eq!(spc, SPC_709, "output matrix must be BT.709");
}
