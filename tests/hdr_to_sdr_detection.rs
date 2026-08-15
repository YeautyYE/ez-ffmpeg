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
use ez_ffmpeg::capabilities::{is_filter_available, is_filter_option_available};
use ez_ffmpeg::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use ez_ffmpeg::filter::frame_filter_context::FrameFilterContext;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::util::ffmpeg_utils::{frame_is_eof_marker, make_frame_writable};
use ez_ffmpeg::{AVMediaType, FfmpegContext, Frame, Input, Output};
use ffmpeg_sys_next::{
    av_frame_new_side_data, AVColorPrimaries, AVColorSpace, AVColorTransferCharacteristic,
    AVFrameSideDataType,
};
use routing::{
    append_sidedata_delete, backend_from_caps, build_chain, build_chain_with_sidedata, classify,
    gamut_only_chain, graph_configures, preflight_chain, probe_hdr_caps, route,
    route_with_preflight, swscale_perceptual_options_available, tone_map_chain,
    wgpu_auto_route_allowed, Backend, ColorKind, HdrFilterCapabilities, RoutingPolicy,
    COLORSPACE_GAMUT_CHAIN, HDR_SIDEDATA_DELETE, PREFLIGHT_REJECTION, SWSCALE_PERCEPTUAL_CHAIN,
};
use std::sync::{Arc, Mutex};

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

fn caps(
    zscale: bool,
    tonemap: bool,
    libplacebo: bool,
    colorspace: bool,
    swscale_perceptual: bool,
) -> HdrFilterCapabilities {
    HdrFilterCapabilities {
        has_libplacebo: libplacebo,
        has_zscale: zscale,
        has_tonemap: tonemap,
        has_colorspace: colorspace,
        has_swscale_perceptual: swscale_perceptual,
    }
}

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
    // BT.2020 container with unspecified transfer is not guessed as SDR or HDR.
    assert_eq!(classify(2, PRI_2020, SPC_2020), ColorKind::AmbiguousHdr);
    let err = route(
        ColorKind::AmbiguousHdr,
        RoutingPolicy::Compatible,
        caps(true, true, true, true, true),
    )
    .unwrap_err();
    assert!(err.contains("unspecified"), "{err}");
    assert!(err.contains("No output was written"), "{err}");
    assert_eq!(
        build_chain(ColorKind::AmbiguousHdr, Backend::Zscale, 10),
        None
    );
}

/// T5 regression (pure): a wide-gamut SDR clip (BT.709 transfer + BT.2020
/// primaries) must classify as wide-gamut SDR, NOT HDR, and its chain must not
/// tone-map. Routing on primaries instead of transfer would break this.
#[test]
fn wide_gamut_sdr_is_not_tone_mapped() {
    let kind = classify(TRC_709, PRI_2020, SPC_2020);
    assert_eq!(kind, ColorKind::WideGamutSdr, "must not be HDR");
    for backend in [
        Backend::Zscale,
        Backend::Libplacebo,
        Backend::Colorspace,
        Backend::SwscalePerceptual,
    ] {
        let chain = build_chain(kind, backend, 10).expect("wide-gamut has a chain");
        assert!(
            !chain.contains("tonemap"),
            "{backend:?} wide-gamut chain must not tone-map: {chain}"
        );
        assert!(
            !chain.contains("intent=perceptual"),
            "{backend:?} wide-gamut chain must not perceptual-tone-map: {chain}",
        );
    }
}

/// Fix for "wide-gamut SDR unnecessarily requires tonemap": on a build with
/// `zscale` but no `tonemap`, wide-gamut SDR must still resolve to a backend,
/// while PQ (which needs the tone-map curve) must not unless another fallback
/// is present.
#[test]
fn gamut_only_does_not_require_tonemap_capability() {
    // zscale present, tonemap absent, libplacebo absent, no FFmpeg 8 scale.
    assert_eq!(
        backend_from_caps(
            ColorKind::WideGamutSdr,
            false,
            caps(true, false, false, false, false)
        ),
        Some(Backend::Zscale),
        "wide-gamut SDR needs only zscale"
    );
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(true, false, false, false, false)),
        None,
        "PQ needs tonemap (or another HDR backend), which is absent"
    );
    // With tonemap present, PQ resolves to the historical zscale default.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(true, true, false, false, false)),
        Some(Backend::Zscale)
    );
    // No filters at all, and no colorspace: nothing usable.
    assert_eq!(
        backend_from_caps(
            ColorKind::WideGamutSdr,
            false,
            caps(false, false, false, false, false)
        ),
        None
    );
    // prefer_gpu picks libplacebo when present.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, true, caps(true, true, true, true, true)),
        Some(Backend::Libplacebo)
    );
}

#[test]
fn ffmpeg8_scale_is_hdr_fallback_after_zscale_and_libplacebo() {
    let ffmpeg8_only = caps(false, false, false, true, true);
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, ffmpeg8_only),
        Some(Backend::SwscalePerceptual)
    );
    assert_eq!(
        backend_from_caps(ColorKind::Hlg, false, ffmpeg8_only),
        Some(Backend::SwscalePerceptual)
    );

    // Existing zscale+tonemap users keep the historical default even when the
    // FFmpeg 8 options are also present.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(true, true, false, true, true)),
        Some(Backend::Zscale)
    );
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(true, true, true, true, true)),
        Some(Backend::Zscale)
    );

    // tonemap without zscale is not a valid FFmpeg 7 backend.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(false, true, false, true, false)),
        None
    );

    // Partial scale options (the combined bit is false) are not a fallback.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(false, false, false, true, false)),
        None
    );
}

#[test]
fn wide_gamut_sdr_uses_builtin_colorspace_without_tonemap() {
    let colorspace_only = caps(false, false, false, true, true);
    assert_eq!(
        backend_from_caps(ColorKind::WideGamutSdr, false, colorspace_only),
        Some(Backend::Colorspace)
    );
    // HDR must not take the colorspace path even when it is the only
    // built-in filter; FFmpeg 7.1 colorspace has no PQ/HLG EOTF.
    assert_eq!(
        backend_from_caps(ColorKind::Pq, false, caps(false, false, false, true, false)),
        None
    );
}

#[test]
fn exact_backend_does_not_silently_fallback() {
    let zscale_only = caps(true, true, false, true, true);
    let err = route(
        ColorKind::Pq,
        RoutingPolicy::Exact(Backend::Libplacebo),
        zscale_only,
    )
    .expect_err("exact libplacebo must not fall back to zscale");
    assert!(
        err.contains("libplacebo filter is not compiled in"),
        "exact miss must keep the backend rejection: {err}"
    );
    assert!(
        err.contains("no usable backend"),
        "exact miss must fail closed: {err}"
    );

    let ok = route(
        ColorKind::Pq,
        RoutingPolicy::Exact(Backend::Zscale),
        zscale_only,
    )
    .expect("exact zscale when present");
    assert_eq!(ok.selected, Backend::Zscale);
}

/// Exact + PQ + colorspace must name why that backend is invalid for PQ,
/// not only the generic multi-backend checklist.
#[test]
fn exact_colorspace_on_pq_keeps_rejection_reason() {
    let colorspace_only = caps(false, false, false, true, false);
    let err = route(
        ColorKind::Pq,
        RoutingPolicy::Exact(Backend::Colorspace),
        colorspace_only,
    )
    .expect_err("exact colorspace must not be used for PQ");
    assert!(
        err.contains("colorspace is gamut-only") && err.contains("cannot tone-map PQ"),
        "exact miss must say why colorspace is invalid for PQ, not only the generic list: {err}"
    );
}

#[test]
fn quality_first_prefers_libplacebo_then_zscale_then_scale() {
    assert_eq!(
        route(
            ColorKind::Pq,
            RoutingPolicy::QualityFirst,
            caps(true, true, true, true, true)
        )
        .unwrap()
        .selected,
        Backend::Libplacebo
    );
    assert_eq!(
        route(
            ColorKind::Pq,
            RoutingPolicy::QualityFirst,
            caps(true, true, false, true, true)
        )
        .unwrap()
        .selected,
        Backend::Zscale
    );
    assert_eq!(
        route(
            ColorKind::Pq,
            RoutingPolicy::QualityFirst,
            caps(false, false, false, true, true)
        )
        .unwrap()
        .selected,
        Backend::SwscalePerceptual
    );
}

#[test]
fn wgpu_is_not_an_hdr_auto_backend() {
    assert!(!wgpu_auto_route_allowed(ColorKind::Pq));
    assert!(!wgpu_auto_route_allowed(ColorKind::Hlg));
    assert!(!wgpu_auto_route_allowed(ColorKind::WideGamutSdr));
    assert!(!wgpu_auto_route_allowed(ColorKind::AmbiguousHdr));
    // 8-bit SDR is the only wgpu-eligible kind, and SDR selects no conversion.
    assert!(wgpu_auto_route_allowed(ColorKind::Sdr));
    assert_eq!(
        backend_from_caps(ColorKind::Sdr, false, caps(true, true, true, true, true)),
        None
    );
}

#[test]
fn fail_closed_error_is_actionable_on_ffmpeg_71_minimal() {
    let none = caps(false, false, false, false, false);
    let err = route(ColorKind::Pq, RoutingPolicy::Compatible, none).unwrap_err();
    assert!(err.contains("PQ"), "{err}");
    assert!(err.contains("--enable-libzimg"), "{err}");
    assert!(err.contains("--enable-libplacebo"), "{err}");
    assert!(err.contains("FFmpeg 8"), "{err}");
    assert!(err.contains("ffmpeg -h filter=scale"), "{err}");
    assert!(err.contains("No output was written"), "{err}");
    assert!(
        err.contains("Tried:"),
        "Compatible miss must list the attempted backends: {err}"
    );
    assert!(
        err.contains("zscale filter is not compiled in"),
        "must surface the zscale rejection, not only the checklist: {err}"
    );
    assert!(
        err.contains("8-bit BT.601/709"),
        "must not advertise wgpu as an HDR fix: {err}"
    );
    assert!(
        !err.contains("use the swscale chain manually"),
        "must not recommend a blind scale on FFmpeg 7.1: {err}"
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

    let sws = tone_map_chain(Backend::SwscalePerceptual, 10);
    assert_eq!(sws, SWSCALE_PERCEPTUAL_CHAIN);
    assert!(
        sws.contains("intent=perceptual"),
        "must request perceptual mapping: {sws}"
    );
    assert!(
        sws.contains("out_primaries=bt709") && sws.contains("out_transfer=bt709"),
        "must convert, not only retag: {sws}"
    );
}

#[test]
fn peak_scales_into_the_chain() {
    // A 4000-nit master normalizes to peak=40.
    assert!(tone_map_chain(Backend::Zscale, 40).contains("peak=40"));
}

#[test]
fn colorspace_gamut_chain_is_not_a_tone_map() {
    assert_eq!(
        gamut_only_chain(Backend::Colorspace),
        COLORSPACE_GAMUT_CHAIN
    );
    assert!(!COLORSPACE_GAMUT_CHAIN.contains("tonemap"));
    assert!(!COLORSPACE_GAMUT_CHAIN.contains("intent="));
}

/// Preflight (pure): a backend that passes the static registry check but
/// whose graph would not configure must be skipped with the preflight
/// rejection, and the next candidate must win. This is the
/// "libplacebo compiled in, Vulkan missing" scenario.
#[test]
fn preflight_skips_registered_but_unconfigurable_backend() {
    let all = caps(true, true, true, true, true);

    // Fake preflight: only the libplacebo chain fails to configure.
    let vulkan_broken = |_kind: ColorKind, chain: &str| !chain.contains("libplacebo");
    let decision = route_with_preflight(
        ColorKind::Pq,
        RoutingPolicy::QualityFirst,
        all,
        Some(&vulkan_broken),
    )
    .expect("zscale must win after libplacebo fails preflight");
    assert_eq!(decision.selected, Backend::Zscale);
    let placebo = decision
        .attempts
        .iter()
        .find(|a| a.backend == Backend::Libplacebo)
        .expect("libplacebo must appear in the attempts");
    assert!(!placebo.available, "failed preflight means unavailable");
    assert_eq!(placebo.rejection, Some(PREFLIGHT_REJECTION));

    // Every graph failing must fail closed with the preflight reason listed.
    let nothing_configures = |_kind: ColorKind, _chain: &str| false;
    let err = route_with_preflight(
        ColorKind::Pq,
        RoutingPolicy::Compatible,
        all,
        Some(&nothing_configures),
    )
    .expect_err("no configurable backend must fail closed");
    assert!(err.contains("would not configure"), "{err}");
    assert!(err.contains("No output was written"), "{err}");

    // Exact must not silently substitute when its graph fails preflight.
    let err = route_with_preflight(
        ColorKind::Pq,
        RoutingPolicy::Exact(Backend::Libplacebo),
        all,
        Some(&vulkan_broken),
    )
    .expect_err("exact libplacebo failing preflight must error");
    assert!(err.contains("would not configure"), "{err}");
}

/// `route` (no preflight hook) must stay purely static: identical to
/// `route_with_preflight(.., None)`, so the existing pure tests keep meaning.
#[test]
fn route_without_hook_is_static_only() {
    let all = caps(true, true, true, true, true);
    assert_eq!(
        route(ColorKind::Pq, RoutingPolicy::Compatible, all),
        route_with_preflight(ColorKind::Pq, RoutingPolicy::Compatible, all, None)
    );
}

/// Side-data cleanup (pure): tone-map and gamut chains append HDR-typed
/// `sidedata` deletes when the filter exists, and stay untouched when not.
#[test]
fn sidedata_delete_is_appended_when_filter_exists() {
    let with = build_chain_with_sidedata(ColorKind::Pq, Backend::Zscale, 10, true).unwrap();
    assert!(
        with.contains(HDR_SIDEDATA_DELETE) && !with.contains("sidedata=delete,"),
        "{with}"
    );
    let without = build_chain_with_sidedata(ColorKind::Pq, Backend::Zscale, 10, false).unwrap();
    assert!(!without.contains("sidedata"), "{without}");

    // Wide-gamut SDR conversion also leaves stale HDR side data behind.
    let gamut =
        build_chain_with_sidedata(ColorKind::WideGamutSdr, Backend::Colorspace, 10, true).unwrap();
    assert!(gamut.contains(HDR_SIDEDATA_DELETE), "{gamut}");

    assert_eq!(
        append_sidedata_delete("null".to_string(), false),
        "null",
        "missing filter must not fail or alter the chain"
    );
}

/// Side-data cleanup (live): `build_chain` mirrors the linked FFmpeg's
/// `sidedata` availability.
#[test]
fn build_chain_carries_sidedata_delete_matching_live_probe() {
    let chain = build_chain(ColorKind::Pq, Backend::Zscale, 10).unwrap();
    if is_filter_available("sidedata") {
        assert!(chain.contains(HDR_SIDEDATA_DELETE), "{chain}");
    } else {
        assert!(!chain.contains("sidedata"), "{chain}");
    }
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
    assert_eq!(
        kind,
        ColorKind::WideGamutSdr,
        "must route as wide-gamut SDR"
    );
    for backend in [Backend::Zscale, Backend::Libplacebo, Backend::Colorspace] {
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
        if std::env::var_os("EZ_FFMPEG_ZSCALE_MUST_RUN").is_some() {
            panic!(
                "EZ_FFMPEG_ZSCALE_MUST_RUN is set but the linked FFmpeg lacks \
                 zscale/tonemap (needs libzimg)"
            );
        }
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

/// Live graph preflight: a trivially valid chain configures, a nonexistent
/// filter does not. This is the extracted, cheap-to-test core of the
/// "registered is not the same as runnable" check.
#[test]
fn graph_configures_reflects_the_live_build() {
    assert!(graph_configures("null"), "the null filter must configure");
    assert!(
        graph_configures("format=yuv420p"),
        "a plain format conversion must configure"
    );
    assert!(
        !graph_configures("thisfilterdoesnotexist"),
        "an unknown filter must be reported as not configuring"
    );
}

/// Live preflight of the real chains: when zscale/tonemap are compiled in,
/// the PQ tone-map chain must survive the preflight graph (so the preflight
/// cannot reject the backend the rest of this suite proves works E2E).
#[test]
fn preflight_accepts_working_zscale_chain() {
    if !(is_filter_available("zscale") && is_filter_available("tonemap")) {
        if std::env::var_os("EZ_FFMPEG_ZSCALE_MUST_RUN").is_some() {
            panic!(
                "EZ_FFMPEG_ZSCALE_MUST_RUN is set but the linked FFmpeg lacks \
                 zscale/tonemap (needs libzimg)"
            );
        }
        eprintln!(
            "skipping preflight_accepts_working_zscale_chain: linked FFmpeg \
             lacks zscale/tonemap (needs libzimg)"
        );
        return;
    }
    let chain = build_chain(ColorKind::Pq, Backend::Zscale, 10).unwrap();
    assert!(
        preflight_chain(ColorKind::Pq, &chain),
        "a compiled-in zscale/tonemap chain must pass preflight: {chain}"
    );
}

/// Live FFmpeg 7.1 fail-closed: when the linked build has no HDR backend,
/// Compatible routing of PQ must refuse before writing output. Skipped when
/// zscale, libplacebo, or FFmpeg 8 scale/perceptual is present so those CI
/// lanes stay green.
#[test]
fn live_ffmpeg_71_pq_fails_closed_when_no_hdr_backend() {
    if swscale_perceptual_options_available()
        || is_filter_available("zscale")
        || is_filter_available("libplacebo")
    {
        eprintln!(
            "skipping live_ffmpeg_71_pq_fails_closed_when_no_hdr_backend: \
             linked FFmpeg has an HDR backend (zscale, libplacebo, or \
             FFmpeg 8 scale/perceptual); this assertion is for minimal 7.1"
        );
        return;
    }

    let live_caps = probe_hdr_caps();
    let err = route(ColorKind::Pq, RoutingPolicy::Compatible, live_caps)
        .expect_err("PQ must fail closed on a build with no HDR backend");
    assert!(
        err.contains("No output was written"),
        "fail-closed message must say no output was written: {err}"
    );
}

/// Live option probe: FFmpeg 8 perceptual mapping is all-or-nothing. A 7.1
/// `scale` that only has `out_color_matrix` must not be treated as a fallback.
#[test]
fn swscale_perceptual_probe_requires_all_color_options() {
    let has_primaries = is_filter_option_available("scale", "out_primaries");
    let has_transfer = is_filter_option_available("scale", "out_transfer");
    let has_intent = is_filter_option_available("scale", "intent");
    let combined = swscale_perceptual_options_available();
    assert_eq!(
        combined,
        has_primaries && has_transfer && has_intent,
        "partial scale color options must not count as FFmpeg 8 perceptual"
    );
    // `out_color_matrix` exists on FFmpeg 7.1; it is not sufficient.
    if is_filter_option_available("scale", "out_color_matrix") && !has_intent {
        assert!(
            !combined,
            "FFmpeg 7.1-style scale must fail closed for HDR fallback"
        );
    }
}

/// FFmpeg 8 built-in fallback E2E. Skipped when the linked FFmpeg does not
/// expose the required scale options (typical FFmpeg 7.1).
#[test]
fn swscale_perceptual_retags_output_bt709() {
    if !swscale_perceptual_options_available() {
        if std::env::var_os("EZ_FFMPEG_SWSCALE_PERCEPTUAL_MUST_RUN").is_some() {
            panic!(
                "EZ_FFMPEG_SWSCALE_PERCEPTUAL_MUST_RUN is set but the linked \
                 FFmpeg lacks scale out_primaries/out_transfer/intent"
            );
        }
        eprintln!(
            "skipping swscale_perceptual_retags_output_bt709: linked FFmpeg \
             lacks scale out_primaries/out_transfer/intent (needs FFmpeg 8+)"
        );
        return;
    }

    let hdr = color_fixture(
        "pq_for_swscale.mkv",
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
    );
    let out = tmp_path("swscale_sdr.mkv");
    let chain = tone_map_chain(Backend::SwscalePerceptual, 10);
    assert_eq!(chain, SWSCALE_PERCEPTUAL_CHAIN);

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
    wait_with_watchdog(context.start().unwrap(), 60, "swscale perceptual").unwrap();

    let (trc, pri, spc) = video_color(&out);
    assert_eq!(trc, TRC_709, "output transfer must be BT.709");
    assert_eq!(pri, PRI_709, "output primaries must be BT.709");
    assert_eq!(spc, SPC_709, "output matrix must be BT.709");
}

/// Wide-gamut SDR on the built-in `colorspace` filter (FFmpeg 7.1, no zimg).
#[test]
fn colorspace_gamut_only_retags_output_bt709() {
    if !is_filter_available("colorspace") {
        eprintln!(
            "skipping colorspace_gamut_only_retags_output_bt709: linked \
             FFmpeg lacks the colorspace filter"
        );
        return;
    }

    let src = color_fixture(
        "sdr_bt2020_for_colorspace.mkv",
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=bt709",
    );
    let out = tmp_path("colorspace_bt709.mkv");
    let chain = gamut_only_chain(Backend::Colorspace);
    assert_eq!(chain, COLORSPACE_GAMUT_CHAIN);

    let context = FfmpegContext::builder()
        .input(src.as_str())
        .output(
            Output::from(out.as_str())
                .set_video_filter(chain)
                .set_video_codec("mpeg2video")
                .set_recording_time_us(200_000),
        )
        .build()
        .unwrap();
    wait_with_watchdog(context.start().unwrap(), 60, "colorspace gamut").unwrap();

    let (trc, pri, spc) = video_color(&out);
    assert_eq!(trc, TRC_709, "output transfer must be BT.709");
    assert_eq!(pri, PRI_709, "output primaries must be BT.709");
    assert_eq!(spc, SPC_709, "output matrix must be BT.709");
}

/// 10-bit PQ / HLG pixel oracle. Hosts without zscale+tonemap and without
/// FFmpeg 8 `scale`/`intent=perceptual` skip unless
/// `EZ_FFMPEG_HDR_PIXEL_ORACLE_MUST_RUN` is set (the FFmpeg 8.1.2 LGPL
/// contract lane). When a backend exists, the graph is forced through
/// `yuv420p10le` and the converted luma of a bright patch must sit above a
/// dark patch, both inside limited-range SDR.
fn hdr_cpu_backend() -> Option<Backend> {
    if is_filter_available("zscale") && is_filter_available("tonemap") {
        return Some(Backend::Zscale);
    }
    if swscale_perceptual_options_available() {
        return Some(Backend::SwscalePerceptual);
    }
    None
}

const RAW10_W: usize = 32;
const RAW10_H: usize = 32;
const RAW10_FRAMES: usize = 3;

fn write_yuv420p10le_solid(path: &str, luma: u16) {
    let y = luma.to_le_bytes();
    let chroma = 512u16.to_le_bytes();
    let mut buf = Vec::with_capacity(RAW10_FRAMES * RAW10_W * RAW10_H * 3);
    for _ in 0..RAW10_FRAMES {
        for _ in 0..(RAW10_W * RAW10_H) {
            buf.extend_from_slice(&y);
        }
        // U then V, 16x16 each.
        for _ in 0..(RAW10_W * RAW10_H / 2) {
            buf.extend_from_slice(&chroma);
        }
    }
    std::fs::write(path, buf).expect("write 10-bit raw");
}

fn raw10_input(path: &str) -> Input {
    Input::from(path)
        .set_format("rawvideo")
        .set_format_opt("video_size", "32x32")
        .set_format_opt("pixel_format", "yuv420p10le")
        .set_format_opt("framerate", "25")
}

fn raw8_center_luma(path: &str, name: &str) -> u8 {
    let bytes = std::fs::read(path).expect(name);
    assert!(
        bytes.len() >= RAW10_W * RAW10_H,
        "{name}: expected at least one 32x32 Y plane, got {} bytes",
        bytes.len()
    );
    bytes[16 * 32 + 16]
}

fn run_raw10_filter(src: &str, filter: &str, out_name: &str) -> u8 {
    let out = tmp_path(out_name);
    let context = FfmpegContext::builder()
        .input(raw10_input(src))
        .output(
            Output::from(out.as_str())
                .set_video_filter(filter)
                .set_format("rawvideo")
                .set_video_codec("rawvideo")
                .set_recording_time_us(80_000),
        )
        .build()
        .unwrap();
    wait_with_watchdog(context.start().unwrap(), 60, out_name).unwrap();
    raw8_center_luma(&out, out_name)
}

fn ten_bit_hdr_center_luma(kind: ColorKind, luma: u16, name: &str) -> u8 {
    let backend = hdr_cpu_backend().expect("caller skipped when no HDR backend");
    let chain = build_chain(kind, backend, 10).expect("PQ/HLG have a tone-map chain");
    let setparams = match kind {
        ColorKind::Pq => "colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
        ColorKind::Hlg => "colorspace=bt2020nc:color_primaries=bt2020:color_trc=arib-std-b67",
        other => panic!("oracle is PQ/HLG only, got {other:?}"),
    };
    let src = tmp_path(&format!("{name}.src.yuv"));
    write_yuv420p10le_solid(&src, luma);
    run_raw10_filter(&src, &format!("setparams={setparams},{chain}"), name)
}

#[test]
fn lavfi_white_yuv420p10le_is_actually_ten_bit() {
    let out = tmp_path("white10.yuv");
    let context = FfmpegContext::builder()
        .input(Input::from("color=c=white:s=32x32:rate=25:duration=0.08").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .set_video_filter("format=yuv420p10le")
                .set_format("rawvideo")
                .set_video_codec("rawvideo")
                .set_recording_time_us(40_000),
        )
        .build()
        .unwrap();
    wait_with_watchdog(context.start().unwrap(), 60, "white10 raw").unwrap();
    let bytes = std::fs::read(&out).expect("10-bit rawvideo");
    assert!(
        bytes.len() >= 32 * 32 * 2,
        "yuv420p10le Y plane is 2048 bytes, got {}",
        bytes.len()
    );
    let y0 = u16::from_le_bytes([bytes[0], bytes[1]]);
    assert!(
        y0 > 255,
        "10-bit white luma must not fit in 8 bits, got {y0}"
    );
}

#[test]
fn ten_bit_pq_and_hlg_oracles_map_to_sdr_luma() {
    if hdr_cpu_backend().is_none() {
        if std::env::var_os("EZ_FFMPEG_HDR_PIXEL_ORACLE_MUST_RUN").is_some() {
            panic!(
                "EZ_FFMPEG_HDR_PIXEL_ORACLE_MUST_RUN is set but the linked \
                 FFmpeg has neither zscale+tonemap nor FFmpeg 8 scale/perceptual"
            );
        }
        eprintln!(
            "skipping ten_bit_pq_and_hlg_oracles_map_to_sdr_luma: linked \
             FFmpeg has neither zscale+tonemap nor FFmpeg 8 scale/perceptual"
        );
        return;
    }

    // 10-bit codes: 64 ≈ black+offset, 900 ≈ a bright PQ/HLG highlight,
    // 512 ≈ mid (naive 8-bit reinterpret is 128).
    let pq_bright = ten_bit_hdr_center_luma(ColorKind::Pq, 900, "pq10_bright.yuv");
    let pq_dark = ten_bit_hdr_center_luma(ColorKind::Pq, 64, "pq10_dark.yuv");
    let hlg_bright = ten_bit_hdr_center_luma(ColorKind::Hlg, 900, "hlg10_bright.yuv");
    let hlg_dark = ten_bit_hdr_center_luma(ColorKind::Hlg, 64, "hlg10_dark.yuv");

    for (label, y) in [
        ("pq bright", pq_bright),
        ("pq dark", pq_dark),
        ("hlg bright", hlg_bright),
        ("hlg dark", hlg_dark),
    ] {
        assert!(
            (16..=255).contains(&y),
            "{label} luma {y} is outside 8-bit SDR"
        );
    }
    assert!(
        pq_bright > pq_dark.saturating_add(20),
        "10-bit PQ 900 ({pq_bright}) must stay brighter than 64 ({pq_dark})"
    );
    assert!(
        hlg_bright > hlg_dark.saturating_add(20),
        "10-bit HLG 900 ({hlg_bright}) must stay brighter than 64 ({hlg_dark})"
    );

    // Anti-retag: a 10→8 bit shift of code 512 is 128. A real PQ curve must
    // not land on that exact naive reinterpret (a retag-only `format=yuv420p`
    // would).
    let src_mid = tmp_path("pq10_mid.src.yuv");
    write_yuv420p10le_solid(&src_mid, 512);
    let naive = run_raw10_filter(&src_mid, "format=yuv420p", "pq10_mid_naive.yuv");
    let pq_mid = ten_bit_hdr_center_luma(ColorKind::Pq, 512, "pq10_mid.yuv");
    assert_eq!(naive, 128, "10-bit 512 >> 2 must be 128, got {naive}");
    assert_ne!(
        pq_mid, naive,
        "PQ tone-map of 10-bit 512 must not be a retag-only >>2 ({pq_mid})"
    );

    let hlg_mid = ten_bit_hdr_center_luma(ColorKind::Hlg, 512, "hlg10_mid.yuv");
    assert_ne!(
        hlg_mid, naive,
        "HLG tone-map of 10-bit 512 must not be a retag-only >>2 ({hlg_mid})"
    );
}

extern "C" {
    fn av_mastering_display_metadata_create_side_data(
        frame: *mut ffmpeg_sys_next::AVFrame,
    ) -> *mut std::ffi::c_void;
    fn av_content_light_metadata_create_side_data(
        frame: *mut ffmpeg_sys_next::AVFrame,
    ) -> *mut std::ffi::c_void;
}

struct SideDataInject;

impl FrameFilter for SideDataInject {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }

    fn filter_frame(
        &mut self,
        mut frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if frame_is_eof_marker(&frame) {
            return Ok(Some(frame));
        }
        make_frame_writable(&mut frame).map_err(|e| format!("make writable: {e}"))?;
        unsafe {
            let p = frame.as_mut_ptr();
            if av_mastering_display_metadata_create_side_data(p).is_null() {
                return Err("failed to attach MASTERING_DISPLAY_METADATA".into());
            }
            if av_content_light_metadata_create_side_data(p).is_null() {
                return Err("failed to attach CONTENT_LIGHT_LEVEL".into());
            }
            if av_frame_new_side_data(p, AVFrameSideDataType::AV_FRAME_DATA_A53_CC, 8).is_null() {
                return Err("failed to attach A53_CC".into());
            }
        }
        Ok(Some(frame))
    }
}

struct SideDataTap {
    kinds: Arc<Mutex<Vec<i32>>>,
}

impl FrameFilter for SideDataTap {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if !frame_is_eof_marker(&frame) {
            unsafe {
                let p = frame.as_ptr();
                if !p.is_null() {
                    let n = (*p).nb_side_data;
                    let arr = (*p).side_data;
                    if !arr.is_null() {
                        for i in 0..n {
                            let sd = *arr.add(i as usize);
                            if !sd.is_null() {
                                self.kinds.lock().unwrap().push((*sd).type_ as i32);
                            }
                        }
                    }
                }
            }
        }
        Ok(Some(frame))
    }
}

fn is_hdr_side_data(kind: i32) -> bool {
    kind == AVFrameSideDataType::AV_FRAME_DATA_MASTERING_DISPLAY_METADATA as i32
        || kind == AVFrameSideDataType::AV_FRAME_DATA_CONTENT_LIGHT_LEVEL as i32
        || kind == AVFrameSideDataType::AV_FRAME_DATA_DYNAMIC_HDR_PLUS as i32
}

fn is_a53_side_data(kind: i32) -> bool {
    kind == AVFrameSideDataType::AV_FRAME_DATA_A53_CC as i32
}

fn collect_injected_side_data(video_filter: Option<&str>, name: &str) -> Vec<i32> {
    let kinds = Arc::new(Mutex::new(Vec::new()));
    let inject = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO)
        .filter("hdr-side-data-inject", Box::new(SideDataInject));
    let tap = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "hdr-side-data-tap",
        Box::new(SideDataTap {
            kinds: kinds.clone(),
        }),
    );
    let out = tmp_path(&format!("{name}.mpg"));
    let mut output = Output::from(out.as_str())
        .add_frame_pipeline(tap)
        .set_video_codec("mpeg2video")
        .set_recording_time_us(80_000);
    if let Some(filter) = video_filter {
        output = output.set_video_filter(filter);
    }
    let src = color_fixture(
        &format!("{name}.src.mkv"),
        "colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
    );
    let context = FfmpegContext::builder()
        .input(Input::from(src.as_str()).add_frame_pipeline(inject))
        .output(output)
        .build()
        .unwrap();
    wait_with_watchdog(context.start().unwrap(), 60, name).unwrap();
    let collected = kinds.lock().unwrap().clone();
    collected
}

/// Decoded-frame oracle: mastering/CLL attached after decode must survive a
/// passthrough and disappear after HDR-typed `sidedata` deletes and after
/// the cookbook chain. A53 captions must survive the typed cleanup.
/// Injection is a Rust `FrameFilter` because FFmpeg 7.1 `sidedata` has no
/// `mode=add`.
#[test]
fn cookbook_chain_strips_hdr_keeps_non_hdr_side_data() {
    let injected = collect_injected_side_data(None, "hdr_side_inject");
    assert!(
        injected.iter().copied().any(is_hdr_side_data),
        "input inject must attach mastering/CLL on decoded frames, got {injected:?}"
    );
    assert!(
        injected.iter().copied().any(is_a53_side_data),
        "input inject must attach A53_CC as a non-HDR control, got {injected:?}"
    );

    if !is_filter_available("sidedata") {
        eprintln!(
            "skipping sidedata typed-delete / cookbook half: linked FFmpeg has no \
             sidedata filter"
        );
        return;
    }

    let deleted = collect_injected_side_data(Some(HDR_SIDEDATA_DELETE), "hdr_side_delete");
    assert!(
        deleted.iter().copied().all(|kind| !is_hdr_side_data(kind)),
        "HDR-typed sidedata delete must drop HDR side data, got {deleted:?}"
    );
    assert!(
        deleted.iter().copied().any(is_a53_side_data),
        "HDR-typed sidedata delete must keep A53_CC, got {deleted:?}"
    );

    if let Some(backend) = hdr_cpu_backend() {
        let chain = build_chain(ColorKind::Pq, backend, 10).expect("PQ has a tone-map chain");
        assert!(
            chain.contains(HDR_SIDEDATA_DELETE),
            "cookbook chain must append HDR-typed sidedata deletes when the filter exists: {chain}"
        );
        let cooked = collect_injected_side_data(Some(&chain), "hdr_side_cookbook");
        assert!(
            cooked.iter().copied().all(|kind| !is_hdr_side_data(kind)),
            "cookbook chain must strip HDR side data from decoded frames, got {cooked:?}"
        );
        assert!(
            cooked.iter().copied().any(is_a53_side_data),
            "cookbook chain must keep non-HDR A53_CC, got {cooked:?}"
        );
    }
}
