//! HlsLadder video-encoder selection, prescriptions, and injectable registry.

use std::sync::OnceLock;

use ffmpeg_sys_next::AVCodecID;

use crate::error::{HlsEncoderAttempt, HlsEncoderAttemptOutcome, HlsEncoderSelectionError, Result};

use super::{Rendition, DEFAULT_VIDEO_CODEC as LIBX264};

/// How the caller asked for a video encoder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum HlsVideoCodecSelection {
    /// `HlsLadder::new` default: historical `libx264` path.
    HistoricalDefault,
    /// `.video_codec(name)` — `name` is a literal encoder, including `"auto"`.
    Explicit(String),
    /// `.video_codec_auto()` — opt-in runtime selection.
    Auto,
}

/// Auto-admitted fallback wrappers, in selection priority. AMF is not in v1.
pub(super) const AUTO_PRIORITY: [&str; 4] =
    ["h264_videotoolbox", "h264_nvenc", "h264_qsv", "libopenh264"];

const PIX_YUV420P: &str = "yuv420p";
const PIX_NV12: &str = "nv12";

/// Resolved encoder used to wire every rendition of one ladder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ResolvedHlsEncoder {
    pub name: String,
    pub pixel_format: &'static str,
    pub is_fallback: bool,
    kind: ResolvedKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolvedKind {
    HistoricalX264,
    Explicit,
    Fallback(FallbackKind),
    /// `.video_codec("h264_qsv")` and the other AUTO_PRIORITY names: the
    /// caller pinned the wrapper, so this is not a host-dependent fallback
    /// and must not warn that libx264 is missing. The HLS-safe option set
    /// (pixel format, `bf=0`, periodic IDR) still applies — copying a name
    /// from the historical-default error into `.video_codec(...)` has to
    /// produce an aligned ladder, not generic `g`/`keyint_min` options the
    /// wrapper ignores.
    ExplicitAdmitted(FallbackKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FallbackKind {
    VideoToolbox,
    Nvenc,
    Qsv,
    OpenH264,
}

impl FallbackKind {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "h264_videotoolbox" => Some(Self::VideoToolbox),
            "h264_nvenc" => Some(Self::Nvenc),
            "h264_qsv" => Some(Self::Qsv),
            "libopenh264" => Some(Self::OpenH264),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::VideoToolbox => "h264_videotoolbox",
            Self::Nvenc => "h264_nvenc",
            Self::Qsv => "h264_qsv",
            Self::OpenH264 => "libopenh264",
        }
    }

    fn pixel_format(self) -> &'static str {
        match self {
            Self::Qsv => PIX_NV12,
            _ => PIX_YUV420P,
        }
    }

    fn omit_bufsize(self) -> bool {
        matches!(self, Self::VideoToolbox | Self::OpenH264)
    }
}

impl ResolvedHlsEncoder {
    pub(super) fn historical() -> Self {
        Self {
            name: LIBX264.to_string(),
            pixel_format: PIX_YUV420P,
            is_fallback: false,
            kind: ResolvedKind::HistoricalX264,
        }
    }

    pub(super) fn explicit(name: impl Into<String>) -> Self {
        let name = name.into();
        if let Some(kind) = FallbackKind::from_name(&name) {
            return Self {
                name: kind.name().to_string(),
                pixel_format: kind.pixel_format(),
                is_fallback: false,
                kind: ResolvedKind::ExplicitAdmitted(kind),
            };
        }
        Self {
            name,
            pixel_format: PIX_YUV420P,
            is_fallback: false,
            kind: ResolvedKind::Explicit,
        }
    }

    pub(super) fn fallback(kind: FallbackKind) -> Self {
        Self {
            name: kind.name().to_string(),
            pixel_format: kind.pixel_format(),
            is_fallback: true,
            kind: ResolvedKind::Fallback(kind),
        }
    }

    pub(super) fn wants_periodic_force(&self) -> bool {
        matches!(
            self.kind,
            ResolvedKind::Fallback(_) | ResolvedKind::ExplicitAdmitted(_)
        )
    }

    /// VideoToolbox / NVENC / QSV / OpenH264 omit a proven `bufsize`, so
    /// master `BANDWIDTH` must be measured from completed segments whether
    /// the wrapper was auto-selected or pinned by name.
    pub(super) fn wants_measured_bandwidth(&self) -> bool {
        matches!(
            self.kind,
            ResolvedKind::Fallback(_) | ResolvedKind::ExplicitAdmitted(_)
        )
    }

    /// Codec AVOptions excluding `b` (applied via `set_video_bitrate`).
    pub(super) fn codec_opts(
        &self,
        gop_frames: &str,
        video_bitrate: &str,
        bufsize: &str,
    ) -> Vec<(String, String)> {
        match self.kind {
            ResolvedKind::HistoricalX264 => {
                historical_x264_opts(gop_frames, video_bitrate, bufsize)
            }
            ResolvedKind::Explicit => explicit_opts(&self.name, gop_frames, video_bitrate, bufsize),
            ResolvedKind::Fallback(kind) | ResolvedKind::ExplicitAdmitted(kind) => {
                fallback_opts(kind, gop_frames, video_bitrate, bufsize)
            }
        }
    }
}

fn historical_x264_opts(
    gop_frames: &str,
    video_bitrate: &str,
    bufsize: &str,
) -> Vec<(String, String)> {
    vec![
        ("g".into(), gop_frames.into()),
        ("keyint_min".into(), gop_frames.into()),
        ("sc_threshold".into(), "0".into()),
        ("maxrate".into(), video_bitrate.into()),
        ("bufsize".into(), bufsize.into()),
        ("x264-params".into(), "scenecut=0:open-gop=0".into()),
    ]
}

fn explicit_opts(
    name: &str,
    gop_frames: &str,
    video_bitrate: &str,
    bufsize: &str,
) -> Vec<(String, String)> {
    let mut opts = vec![
        ("g".into(), gop_frames.into()),
        ("keyint_min".into(), gop_frames.into()),
        ("sc_threshold".into(), "0".into()),
        ("maxrate".into(), video_bitrate.into()),
        ("bufsize".into(), bufsize.into()),
    ];
    if name == "libx264" {
        opts.push(("x264-params".into(), "scenecut=0:open-gop=0".into()));
    } else if name == "libx265" {
        opts.push(("x265-params".into(), "scenecut=0:open-gop=0".into()));
    }
    opts
}

fn fallback_opts(
    kind: FallbackKind,
    gop_frames: &str,
    video_bitrate: &str,
    bufsize: &str,
) -> Vec<(String, String)> {
    let mut opts: Vec<(String, String)> =
        vec![("g".into(), gop_frames.into()), ("bf".into(), "0".into())];
    match kind {
        FallbackKind::VideoToolbox => {
            opts.push(("flags".into(), "+cgop".into()));
            opts.push(("maxrate".into(), video_bitrate.into()));
        }
        FallbackKind::Nvenc => {
            opts.push(("rc-lookahead".into(), "0".into()));
            opts.push(("no-scenecut".into(), "1".into()));
            opts.push(("forced-idr".into(), "1".into()));
            opts.push(("intra-refresh".into(), "0".into()));
            opts.push(("maxrate".into(), video_bitrate.into()));
            opts.push(("bufsize".into(), bufsize.into()));
        }
        FallbackKind::Qsv => {
            opts.push(("flags".into(), "-cgop".into()));
            opts.push(("adaptive_i".into(), "0".into()));
            opts.push(("adaptive_b".into(), "0".into()));
            opts.push(("extbrc".into(), "0".into()));
            opts.push(("look_ahead".into(), "0".into()));
            opts.push(("idr_interval".into(), "0".into()));
            opts.push(("forced_idr".into(), "1".into()));
            opts.push(("maxrate".into(), video_bitrate.into()));
            opts.push(("bufsize".into(), bufsize.into()));
        }
        FallbackKind::OpenH264 => {
            opts.push(("allow_skip_frames".into(), "0".into()));
            opts.push(("rc_mode".into(), "bitrate".into()));
            opts.push(("maxrate".into(), video_bitrate.into()));
        }
    }
    opts
}

/// Registry of encoder *names* (registration, not runtime-ready).
pub(super) trait EncoderRegistry {
    fn is_registered(&self, name: &str) -> bool;
    /// Registered H.264 encoder names (exact wrapper names).
    fn registered_h264_names(&self) -> Vec<String>;
}

pub(super) struct FfmpegRegistry;

#[derive(Clone, Copy)]
struct FallbackPresence {
    videotoolbox: bool,
    nvenc: bool,
    qsv: bool,
    openh264: bool,
}

static FALLBACK_PRESENCE: OnceLock<FallbackPresence> = OnceLock::new();

fn probe_fallback_presence() -> FallbackPresence {
    FallbackPresence {
        videotoolbox: crate::capabilities::is_encoder_available("h264_videotoolbox"),
        nvenc: crate::capabilities::is_encoder_available("h264_nvenc"),
        qsv: crate::capabilities::is_encoder_available("h264_qsv"),
        openh264: crate::capabilities::is_encoder_available("libopenh264"),
    }
}

impl EncoderRegistry for FfmpegRegistry {
    fn is_registered(&self, name: &str) -> bool {
        match name {
            "h264_videotoolbox" => {
                FALLBACK_PRESENCE
                    .get_or_init(probe_fallback_presence)
                    .videotoolbox
            }
            "h264_nvenc" => FALLBACK_PRESENCE.get_or_init(probe_fallback_presence).nvenc,
            "h264_qsv" => FALLBACK_PRESENCE.get_or_init(probe_fallback_presence).qsv,
            "libopenh264" => {
                FALLBACK_PRESENCE
                    .get_or_init(probe_fallback_presence)
                    .openh264
            }
            _ => crate::capabilities::is_encoder_available(name),
        }
    }

    fn registered_h264_names(&self) -> Vec<String> {
        crate::core::codec::get_encoders()
            .into_iter()
            .filter(|info| info.codec_id == AVCodecID::AV_CODEC_ID_H264)
            .map(|info| info.codec_name)
            .collect()
    }
}

/// Trial-open backend. Production uses FFmpeg; tests inject a script.
pub(super) trait EncoderOpener {
    fn try_open(&self, plan: &EncoderProbePlan<'_>) -> std::result::Result<(), OpenFail>;
}

#[derive(Debug, Clone)]
pub(super) struct OpenFail {
    pub width: u32,
    pub height: u32,
    pub raw_code: i32,
    pub message: String,
}

/// Per-rung parameters for a trial `avcodec_open2`.
#[derive(Debug, Clone)]
pub(super) struct RenditionProbe {
    pub width: u32,
    pub height: u32,
    pub bit_rate: i64,
    pub max_rate: i64,
    pub buffer_size: i64,
    /// Original bitrate string for AVOption `b` (e.g. `"2800k"`), matching
    /// [`Output::set_video_bitrate`](crate::Output::set_video_bitrate).
    pub b_opt: String,
    pub options: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub(super) struct EncoderProbePlan<'a> {
    pub encoder_name: &'a str,
    pub fps: (i64, i64),
    pub pixel_format: &'static str,
    pub renditions: Vec<RenditionProbe>,
}

#[derive(Debug)]
pub(super) enum Decision {
    Ready(ResolvedHlsEncoder),
    NeedTrial,
    /// Registered AUTO_PRIORITY name pinned with `.video_codec(...)`.
    NeedTrialExplicit(FallbackKind),
}

pub(super) fn decide(
    selection: &HlsVideoCodecSelection,
    registry: &dyn EncoderRegistry,
) -> Result<Decision> {
    match selection {
        HlsVideoCodecSelection::HistoricalDefault => {
            if registry.is_registered(LIBX264) {
                Ok(Decision::Ready(ResolvedHlsEncoder::historical()))
            } else {
                let (auto, explicit) = classify_registered_h264(registry);
                Err(HlsEncoderSelectionError::HistoricalDefaultUnavailable {
                    registered_auto_candidates: auto,
                    registered_explicit_h264_encoders: explicit,
                }
                .into())
            }
        }
        HlsVideoCodecSelection::Explicit(name) => {
            if let Some(kind) = FallbackKind::from_name(name) {
                if registry.is_registered(name) {
                    return Ok(Decision::NeedTrialExplicit(kind));
                }
            }
            Ok(Decision::Ready(ResolvedHlsEncoder::explicit(name.clone())))
        }
        HlsVideoCodecSelection::Auto => {
            if registry.is_registered(LIBX264) {
                Ok(Decision::Ready(ResolvedHlsEncoder::historical()))
            } else if AUTO_PRIORITY
                .iter()
                .any(|name| registry.is_registered(name))
            {
                Ok(Decision::NeedTrial)
            } else {
                Err(auto_failed(not_registered_attempts()).into())
            }
        }
    }
}

pub(super) fn select_auto_fallback(
    registry: &dyn EncoderRegistry,
    opener: &dyn EncoderOpener,
    renditions: &[Rendition],
    fps_num: i64,
    fps_den: i64,
    gop_frames: u64,
    parse_bitrate_bps: impl Fn(&str) -> Result<u64>,
) -> Result<ResolvedHlsEncoder> {
    let gop_frames_s = gop_frames.to_string();
    let mut attempts = Vec::with_capacity(AUTO_PRIORITY.len());

    for name in AUTO_PRIORITY {
        if !registry.is_registered(name) {
            attempts.push(HlsEncoderAttempt {
                encoder: name.to_string(),
                outcome: HlsEncoderAttemptOutcome::NotRegistered,
            });
            continue;
        }
        let kind = FallbackKind::from_name(name).expect("AUTO_PRIORITY names are admitted");
        let plan = build_probe_plan(
            kind,
            renditions,
            fps_num,
            fps_den,
            &gop_frames_s,
            &parse_bitrate_bps,
        )?;
        match opener.try_open(&plan) {
            Ok(()) => return Ok(ResolvedHlsEncoder::fallback(kind)),
            Err(fail) => {
                attempts.push(HlsEncoderAttempt {
                    encoder: name.to_string(),
                    outcome: HlsEncoderAttemptOutcome::OpenFailed {
                        width: fail.width,
                        height: fail.height,
                        raw_code: fail.raw_code,
                        message: fail.message,
                    },
                });
            }
        }
    }

    Err(auto_failed(attempts).into())
}

/// Trial-open every rendition of one pinned AUTO_PRIORITY encoder.
pub(super) fn trial_explicit_admitted(
    opener: &dyn EncoderOpener,
    kind: FallbackKind,
    renditions: &[Rendition],
    fps_num: i64,
    fps_den: i64,
    gop_frames: u64,
    parse_bitrate_bps: impl Fn(&str) -> Result<u64>,
) -> Result<ResolvedHlsEncoder> {
    let gop_frames_s = gop_frames.to_string();
    let plan = build_probe_plan(
        kind,
        renditions,
        fps_num,
        fps_den,
        &gop_frames_s,
        &parse_bitrate_bps,
    )?;
    match opener.try_open(&plan) {
        Ok(()) => Ok(ResolvedHlsEncoder::explicit(kind.name())),
        Err(fail) => Err(HlsEncoderSelectionError::ExplicitOpenFailed {
            encoder: kind.name().to_string(),
            width: fail.width,
            height: fail.height,
            raw_code: fail.raw_code,
            message: fail.message,
        }
        .into()),
    }
}

fn build_probe_plan(
    kind: FallbackKind,
    renditions: &[Rendition],
    fps_num: i64,
    fps_den: i64,
    gop_frames: &str,
    parse_bitrate_bps: &impl Fn(&str) -> Result<u64>,
) -> Result<EncoderProbePlan<'static>> {
    let mut rungs = Vec::with_capacity(renditions.len());
    for rendition in renditions {
        let video_bps = parse_bitrate_bps(&rendition.video_bitrate)?;
        let bufsize_bps = video_bps.saturating_mul(2);
        let bufsize = bufsize_bps.to_string();
        let buffer_size = if kind.omit_bufsize() {
            0
        } else {
            i64::try_from(bufsize_bps).unwrap_or(i64::MAX)
        };
        let bit_rate = i64::try_from(video_bps).unwrap_or(i64::MAX);
        rungs.push(RenditionProbe {
            width: rendition.width,
            height: rendition.height,
            bit_rate,
            max_rate: bit_rate,
            buffer_size,
            b_opt: rendition.video_bitrate.clone(),
            options: fallback_opts(kind, gop_frames, &rendition.video_bitrate, &bufsize),
        });
    }
    rungs.sort_by(|a, b| {
        (b.width as u64 * b.height as u64).cmp(&(a.width as u64 * a.height as u64))
    });
    Ok(EncoderProbePlan {
        encoder_name: kind.name(),
        fps: (fps_num, fps_den),
        pixel_format: kind.pixel_format(),
        renditions: rungs,
    })
}

fn not_registered_attempts() -> Vec<HlsEncoderAttempt> {
    AUTO_PRIORITY
        .iter()
        .map(|name| HlsEncoderAttempt {
            encoder: (*name).to_string(),
            outcome: HlsEncoderAttemptOutcome::NotRegistered,
        })
        .collect()
}

fn auto_failed(attempts: Vec<HlsEncoderAttempt>) -> HlsEncoderSelectionError {
    HlsEncoderSelectionError::AutoSelectionFailed { attempts }
}

/// Split registered H.264 names for error text: auto-admitted (priority
/// order) vs explicit-only (lexicographic). `libx264` is omitted.
pub(super) fn classify_registered_h264(
    registry: &dyn EncoderRegistry,
) -> (Vec<String>, Vec<String>) {
    let mut listed = registry.registered_h264_names();
    listed.sort();
    listed.dedup();

    let auto: Vec<String> = AUTO_PRIORITY
        .iter()
        .filter(|name| registry.is_registered(name))
        .map(|name| (*name).to_string())
        .collect();

    let mut explicit: Vec<String> = listed
        .into_iter()
        .filter(|n| n != LIBX264 && !AUTO_PRIORITY.contains(&n.as_str()))
        .collect();
    explicit.sort();
    explicit.dedup();
    (auto, explicit)
}

#[cfg(test)]
pub(super) use self::test_support::*;

#[cfg(test)]
mod test_support {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[derive(Clone, Default)]
    pub(crate) struct FakeRegistry {
        pub names: HashSet<String>,
    }

    impl FakeRegistry {
        pub(crate) fn new(names: &[&str]) -> Self {
            Self {
                names: names.iter().map(|s| (*s).to_string()).collect(),
            }
        }
    }

    impl EncoderRegistry for FakeRegistry {
        fn is_registered(&self, name: &str) -> bool {
            self.names.contains(name)
        }

        fn registered_h264_names(&self) -> Vec<String> {
            let mut v: Vec<String> = self.names.iter().cloned().collect();
            v.sort();
            v
        }
    }

    #[derive(Clone, Copy)]
    pub(crate) enum ScriptedOpen {
        Success,
        Fail {
            width: u32,
            height: u32,
            code: i32,
            msg: &'static str,
        },
        /// Succeed the first (largest) rung, fail the second.
        FailAfterFirst {
            width: u32,
            height: u32,
            code: i32,
            msg: &'static str,
        },
    }

    #[derive(Default)]
    pub(crate) struct ScriptedOpener {
        pub script: HashMap<String, ScriptedOpen>,
        pub opened: std::cell::RefCell<Vec<String>>,
    }

    impl ScriptedOpener {
        pub(crate) fn panic_if_called() -> PanicOpener {
            PanicOpener
        }
    }

    pub(crate) struct PanicOpener;

    impl EncoderOpener for PanicOpener {
        fn try_open(&self, plan: &EncoderProbePlan<'_>) -> std::result::Result<(), OpenFail> {
            panic!(
                "trial open must not run when libx264 is registered (got {})",
                plan.encoder_name
            );
        }
    }

    impl EncoderOpener for ScriptedOpener {
        fn try_open(&self, plan: &EncoderProbePlan<'_>) -> std::result::Result<(), OpenFail> {
            self.opened.borrow_mut().push(plan.encoder_name.to_string());
            match self.script.get(plan.encoder_name).copied() {
                None | Some(ScriptedOpen::Success) => {
                    if plan.renditions.is_empty() {
                        return Err(OpenFail {
                            width: 0,
                            height: 0,
                            raw_code: -1,
                            message: "no renditions".into(),
                        });
                    }
                    Ok(())
                }
                Some(ScriptedOpen::Fail {
                    width,
                    height,
                    code,
                    msg,
                }) => Err(OpenFail {
                    width,
                    height,
                    raw_code: code,
                    message: msg.into(),
                }),
                Some(ScriptedOpen::FailAfterFirst {
                    width,
                    height,
                    code,
                    msg,
                }) => {
                    if plan.renditions.len() < 2 {
                        return Ok(());
                    }
                    Err(OpenFail {
                        width,
                        height,
                        raw_code: code,
                        message: msg.into(),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{Error, HlsEncoderSelectionError};

    fn parse_ok(spec: &str) -> Result<u64> {
        // Tiny bitrate parser for selection tests (matches 800k / 2800k used below).
        let s = spec.trim();
        if let Some(rest) = s.strip_suffix('k').or_else(|| s.strip_suffix('K')) {
            return Ok(rest.parse::<u64>().unwrap() * 1000);
        }
        Ok(s.parse().unwrap())
    }

    fn two_rungs() -> Vec<Rendition> {
        vec![
            Rendition::new(1280, 720, "2800k"),
            Rendition::new(640, 360, "800k"),
        ]
    }

    #[test]
    fn x264_registered_default_and_auto_are_historical() {
        let registry = FakeRegistry::new(&[LIBX264, "h264_videotoolbox"]);
        match decide(&HlsVideoCodecSelection::HistoricalDefault, &registry).unwrap() {
            Decision::Ready(enc) => {
                assert_eq!(enc, ResolvedHlsEncoder::historical());
                assert!(!enc.is_fallback);
            }
            Decision::NeedTrial | Decision::NeedTrialExplicit(_) => {
                panic!("must not trial when libx264 is registered")
            }
        }
        match decide(&HlsVideoCodecSelection::Auto, &registry).unwrap() {
            Decision::Ready(enc) => assert_eq!(enc, ResolvedHlsEncoder::historical()),
            Decision::NeedTrial | Decision::NeedTrialExplicit(_) => {
                panic!("auto with x264 must be historical")
            }
        }
    }

    #[test]
    fn x264_registered_auto_never_calls_opener() {
        let registry = FakeRegistry::new(&[LIBX264]);
        match decide(&HlsVideoCodecSelection::Auto, &registry).unwrap() {
            Decision::Ready(_) => {}
            Decision::NeedTrial => {
                let _ = select_auto_fallback(
                    &registry,
                    &ScriptedOpener::panic_if_called(),
                    &two_rungs(),
                    30,
                    1,
                    180,
                    parse_ok,
                );
            }
            Decision::NeedTrialExplicit(_) => panic!("auto with x264 must not trial a fallback"),
        }
    }

    #[test]
    fn x264_missing_default_is_hls_error() {
        let registry = FakeRegistry::new(&["h264_videotoolbox", "libopenh264", "h264_amf"]);
        let err = decide(&HlsVideoCodecSelection::HistoricalDefault, &registry).unwrap_err();
        match err {
            Error::HlsEncoderSelection(inner) => match inner.as_ref() {
                HlsEncoderSelectionError::HistoricalDefaultUnavailable {
                    registered_auto_candidates,
                    registered_explicit_h264_encoders,
                } => {
                    assert_eq!(
                        registered_auto_candidates,
                        &vec!["h264_videotoolbox".to_string(), "libopenh264".to_string()]
                    );
                    assert_eq!(
                        registered_explicit_h264_encoders,
                        &vec!["h264_amf".to_string()]
                    );
                }
                other => panic!("unexpected {other:?}"),
            },
            other => panic!("expected HLS error, got {other}"),
        }
    }

    #[test]
    fn auto_skips_unregistered_and_open_failures() {
        let registry = FakeRegistry::new(&["h264_nvenc", "libopenh264"]);
        let mut opener = ScriptedOpener::default();
        opener.script.insert(
            "h264_nvenc".into(),
            ScriptedOpen::Fail {
                width: 1280,
                height: 720,
                code: -5,
                msg: "No such device",
            },
        );
        opener
            .script
            .insert("libopenh264".into(), ScriptedOpen::Success);
        let enc =
            select_auto_fallback(&registry, &opener, &two_rungs(), 30, 1, 180, parse_ok).unwrap();
        assert_eq!(enc.name, "libopenh264");
        assert!(enc.is_fallback);
        assert_eq!(
            opener.opened.borrow().as_slice(),
            &["h264_nvenc".to_string(), "libopenh264".to_string()]
        );
    }

    #[test]
    fn auto_requires_every_rendition_to_open() {
        let registry = FakeRegistry::new(&["h264_videotoolbox", "libopenh264"]);
        let mut opener = ScriptedOpener::default();
        opener.script.insert(
            "h264_videotoolbox".into(),
            ScriptedOpen::FailAfterFirst {
                width: 640,
                height: 360,
                code: -12,
                msg: "session quota",
            },
        );
        opener
            .script
            .insert("libopenh264".into(), ScriptedOpen::Success);
        let enc =
            select_auto_fallback(&registry, &opener, &two_rungs(), 30, 1, 180, parse_ok).unwrap();
        assert_eq!(enc.name, "libopenh264");
    }

    #[test]
    fn amf_registered_is_not_auto_admitted() {
        let registry = FakeRegistry::new(&["h264_amf"]);
        match decide(&HlsVideoCodecSelection::Auto, &registry).unwrap_err() {
            Error::HlsEncoderSelection(inner) => match inner.as_ref() {
                HlsEncoderSelectionError::AutoSelectionFailed { attempts } => {
                    assert_eq!(attempts.len(), 4);
                    assert!(attempts
                        .iter()
                        .all(|a| matches!(a.outcome, HlsEncoderAttemptOutcome::NotRegistered)));
                    assert!(!attempts.iter().any(|a| a.encoder == "h264_amf"));
                }
                other => panic!("unexpected {other:?}"),
            },
            other => panic!("expected auto failure, got {other}"),
        }
        let err = decide(&HlsVideoCodecSelection::HistoricalDefault, &registry).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("h264_amf"), "{text}");
        assert!(text.contains("explicit only"), "{text}");
        assert!(text.contains("Registered auto candidates"));
        assert!(text.contains(": none."), "{text}");
    }

    #[test]
    fn explicit_bypasses_new_resolver() {
        let registry = FakeRegistry::new(&[]);
        match decide(
            &HlsVideoCodecSelection::Explicit("libx264".into()),
            &registry,
        )
        .unwrap()
        {
            Decision::Ready(enc) => {
                assert_eq!(enc.name, "libx264");
                assert!(!enc.is_fallback);
            }
            Decision::NeedTrial | Decision::NeedTrialExplicit(_) => {
                panic!("explicit libx264 must not trial")
            }
        }
        match decide(&HlsVideoCodecSelection::Explicit("auto".into()), &registry).unwrap() {
            Decision::Ready(enc) => assert_eq!(enc.name, "auto"),
            Decision::NeedTrial | Decision::NeedTrialExplicit(_) => {
                panic!("literal auto is not Auto mode")
            }
        }
    }

    #[test]
    fn explicit_auto_priority_name_gets_hls_safe_prescription() {
        let enc = ResolvedHlsEncoder::explicit("h264_qsv");
        assert_eq!(enc.name, "h264_qsv");
        assert_eq!(enc.pixel_format, PIX_NV12);
        assert!(
            !enc.is_fallback,
            "pinning QSV is not a libx264-missing fallback"
        );
        assert!(
            enc.wants_measured_bandwidth(),
            "pinned admitted wrappers omit bufsize; master BANDWIDTH must be measured"
        );
        assert!(enc.wants_periodic_force());
        let opts = enc.codec_opts("180", "2800k", "5600000");
        let keys: Vec<&str> = opts.iter().map(|(k, _)| k.as_str()).collect();
        assert!(!keys.contains(&"keyint_min"), "{keys:?}");
        assert!(keys.contains(&"bf"), "{keys:?}");
        assert_eq!(
            ResolvedHlsEncoder::explicit("h264_qsv").codec_opts("180", "2800k", "5600000"),
            ResolvedHlsEncoder::fallback(FallbackKind::Qsv).codec_opts("180", "2800k", "5600000")
        );
    }

    #[test]
    fn pinned_admitted_encoder_measures_bandwidth_historical_does_not() {
        assert!(ResolvedHlsEncoder::explicit("h264_videotoolbox").wants_measured_bandwidth());
        assert!(ResolvedHlsEncoder::explicit("libopenh264").wants_measured_bandwidth());
        assert!(ResolvedHlsEncoder::fallback(FallbackKind::OpenH264).wants_measured_bandwidth());
        assert!(!ResolvedHlsEncoder::historical().wants_measured_bandwidth());
        assert!(!ResolvedHlsEncoder::explicit("libx264").wants_measured_bandwidth());
        assert!(!ResolvedHlsEncoder::explicit("mpeg4").wants_measured_bandwidth());
    }

    #[test]
    fn mixed_case_admitted_name_is_not_rewritten() {
        let enc = ResolvedHlsEncoder::explicit("H264_QSV");
        assert_eq!(enc.name, "H264_QSV");
        assert!(!enc.wants_periodic_force());
        let opts = enc.codec_opts("180", "2800k", "5600000");
        let keys: Vec<&str> = opts.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"keyint_min"), "{keys:?}");
        assert!(!keys.contains(&"bf"), "{keys:?}");
    }

    #[test]
    fn registered_admitted_name_trials_only_that_encoder() {
        let registry = FakeRegistry::new(&["h264_qsv", "h264_nvenc"]);
        match decide(
            &HlsVideoCodecSelection::Explicit("h264_qsv".into()),
            &registry,
        )
        .unwrap()
        {
            Decision::NeedTrialExplicit(FallbackKind::Qsv) => {}
            other => panic!("expected NeedTrialExplicit(Qsv), got {other:?}"),
        }
        match decide(
            &HlsVideoCodecSelection::Explicit("h264_qsv".into()),
            &FakeRegistry::new(&[]),
        )
        .unwrap()
        {
            Decision::Ready(enc) => {
                assert_eq!(enc.name, "h264_qsv");
                assert!(enc.wants_periodic_force());
            }
            other => panic!("unregistered pin must not trial, got {other:?}"),
        }
    }

    #[test]
    fn explicit_trial_open_failure_is_typed_and_does_not_fall_through() {
        let mut opener = ScriptedOpener::default();
        opener.script.insert(
            "h264_qsv".into(),
            ScriptedOpen::Fail {
                width: 1280,
                height: 720,
                code: -1,
                msg: "device busy",
            },
        );
        let err = trial_explicit_admitted(
            &opener,
            FallbackKind::Qsv,
            &two_rungs(),
            30,
            1,
            180,
            parse_ok,
        )
        .unwrap_err();
        match err {
            Error::HlsEncoderSelection(inner) => match inner.as_ref() {
                HlsEncoderSelectionError::ExplicitOpenFailed {
                    encoder,
                    width,
                    height,
                    message,
                    ..
                } => {
                    assert_eq!(encoder, "h264_qsv");
                    assert_eq!(*width, 1280);
                    assert_eq!(*height, 720);
                    assert_eq!(message, "device busy");
                    let text = inner.to_string();
                    assert!(text.contains("pinned encoder 'h264_qsv'"), "{text}");
                    assert!(
                        text.contains("Output directories were not created"),
                        "{text}"
                    );
                    assert!(!text.contains("h264_nvenc"), "{text}");
                }
                other => panic!("expected ExplicitOpenFailed, got {other:?}"),
            },
            other => panic!("expected HlsEncoderSelection, got {other}"),
        }
    }

    #[test]
    fn explicit_unknown_hardware_keeps_generic_opts() {
        let enc = ResolvedHlsEncoder::explicit("h264_amf");
        assert_eq!(enc.name, "h264_amf");
        assert_eq!(enc.pixel_format, PIX_YUV420P);
        assert!(!enc.is_fallback);
        assert!(!enc.wants_periodic_force());
        let opts = enc.codec_opts("180", "2800k", "5600000");
        let keys: Vec<&str> = opts.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"keyint_min"), "{keys:?}");
        assert!(!keys.contains(&"bf"), "{keys:?}");
    }

    #[test]
    fn error_attempt_order_is_stable() {
        let registry = FakeRegistry::new(&["h264_qsv"]);
        let mut opener = ScriptedOpener::default();
        opener.script.insert(
            "h264_qsv".into(),
            ScriptedOpen::Fail {
                width: 1920,
                height: 1080,
                code: -1,
                msg: "External library error",
            },
        );
        let err = select_auto_fallback(&registry, &opener, &two_rungs(), 30, 1, 180, parse_ok)
            .unwrap_err();
        match err {
            Error::HlsEncoderSelection(inner) => match inner.as_ref() {
                HlsEncoderSelectionError::AutoSelectionFailed { attempts } => {
                    let names: Vec<&str> = attempts.iter().map(|a| a.encoder.as_str()).collect();
                    assert_eq!(
                        names,
                        ["h264_videotoolbox", "h264_nvenc", "h264_qsv", "libopenh264"]
                    );
                    assert!(matches!(
                        attempts[0].outcome,
                        HlsEncoderAttemptOutcome::NotRegistered
                    ));
                    assert!(matches!(
                        attempts[2].outcome,
                        HlsEncoderAttemptOutcome::OpenFailed {
                            width: 1920,
                            height: 1080,
                            ..
                        }
                    ));
                    let text = inner.to_string();
                    assert!(text.contains("h264_videotoolbox (not registered)"));
                    assert!(text.contains("1920x1080 encoder open failed: External library error"));
                    assert!(!text.contains("OutOfMemory"), "{text}");
                }
                other => panic!("unexpected {other:?}"),
            },
            other => panic!("expected auto failure, got {other}"),
        }
    }

    #[test]
    fn qsv_uses_nv12_others_yuv420p() {
        assert_eq!(
            ResolvedHlsEncoder::fallback(FallbackKind::Qsv).pixel_format,
            PIX_NV12
        );
        assert_eq!(
            ResolvedHlsEncoder::fallback(FallbackKind::VideoToolbox).pixel_format,
            PIX_YUV420P
        );
        assert_eq!(
            ResolvedHlsEncoder::fallback(FallbackKind::Nvenc).pixel_format,
            PIX_YUV420P
        );
        assert_eq!(
            ResolvedHlsEncoder::fallback(FallbackKind::OpenH264).pixel_format,
            PIX_YUV420P
        );
        assert_eq!(ResolvedHlsEncoder::historical().pixel_format, PIX_YUV420P);
    }

    #[test]
    fn fallback_opts_omit_keyint_and_sc_threshold() {
        for kind in [
            FallbackKind::VideoToolbox,
            FallbackKind::Nvenc,
            FallbackKind::Qsv,
            FallbackKind::OpenH264,
        ] {
            let opts = ResolvedHlsEncoder::fallback(kind).codec_opts("180", "2800k", "5600000");
            let keys: Vec<&str> = opts.iter().map(|(k, _)| k.as_str()).collect();
            assert!(!keys.contains(&"keyint_min"), "{kind:?} {keys:?}");
            assert!(!keys.contains(&"sc_threshold"), "{kind:?} {keys:?}");
            assert!(keys.contains(&"bf"), "{kind:?} {keys:?}");
            assert!(keys.contains(&"g"), "{kind:?} {keys:?}");
        }
    }

    #[test]
    fn x264_opts_do_not_gain_bf_or_flags() {
        let opts = ResolvedHlsEncoder::historical().codec_opts("180", "2800k", "5600000");
        let keys: Vec<&str> = opts.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            [
                "g",
                "keyint_min",
                "sc_threshold",
                "maxrate",
                "bufsize",
                "x264-params"
            ]
        );
        assert!(!keys.contains(&"bf"));
        assert!(!keys.contains(&"flags"));
        assert!(!ResolvedHlsEncoder::historical().wants_periodic_force());
    }

    #[test]
    fn auto_priority_and_packet_sink_strict_tier_stay_distinct() {
        // Two encoder-name contracts must stay distinct:
        // - HLS AUTO_PRIORITY is the opt-in auto fallback order.
        // - packet_sink STRICT_TIER_VIDEO_ENCODERS is the delivery whitelist.
        // QSV is HLS-auto only. libx264 is packet_sink + the historical HLS
        // default, not an auto fallback.
        use crate::core::packet_sink::registry::STRICT_TIER_VIDEO_ENCODERS;

        assert_eq!(
            AUTO_PRIORITY,
            ["h264_videotoolbox", "h264_nvenc", "h264_qsv", "libopenh264"]
        );
        assert_eq!(
            STRICT_TIER_VIDEO_ENCODERS,
            &["libx264", "h264_nvenc", "h264_videotoolbox", "libopenh264"]
        );
        assert!(AUTO_PRIORITY.contains(&"h264_qsv"), "QSV is HLS-auto only");
        assert!(
            !STRICT_TIER_VIDEO_ENCODERS.contains(&"h264_qsv"),
            "QSV is HLS-auto only, not packet_sink strict-tier"
        );
        assert!(
            !AUTO_PRIORITY.contains(&"libx264"),
            "libx264 is packet_sink + historical HLS default, not an auto fallback"
        );
        assert!(
            STRICT_TIER_VIDEO_ENCODERS.contains(&"libx264"),
            "libx264 remains on the packet_sink strict-tier list"
        );
    }

    #[test]
    fn historical_error_mentions_lgpl_and_api() {
        let registry = FakeRegistry::new(&[]);
        let err = decide(&HlsVideoCodecSelection::HistoricalDefault, &registry).unwrap_err();
        let text = err.to_string();
        assert!(text.contains("LGPL"));
        assert!(text.contains("libx264"));
        assert!(text.contains(".video_codec_auto()"));
        assert!(text.contains(".video_codec("));
        let auto_at = text.find(".video_codec_auto()").expect("auto API");
        let pin_at = text.find(".video_codec(").expect("pin API");
        assert!(
            auto_at < pin_at,
            "the one-liner must be named before pinning: {text}"
        );
        assert!(text.contains("docs/INSTALL.md#ffmpeg-capability-and-licensing-matrix"));
    }
}
