//! VOD adaptive-bitrate (ABR) HLS ladder recipe.
//!
//! [`HlsLadder`] decodes a single input once, fans it out through a `split` +
//! per-rendition `scale` filtergraph, and encodes each branch into its own
//! HLS media playlist (`{name}/index.m3u8` + segments). On success it writes a
//! conservative master playlist (`master.m3u8`) via
//! [`crate::core::recipes::hls_master`].
//!
//! # MVP boundary (read this first)
//!
//! This recipe implements **CFR VOD ABR only**:
//! - Constant-frame-rate (CFR) input is **required**. FFmpeg's
//!   `avg_frame_rate` cannot prove CFR, so the caller must guarantee it; the
//!   frame rate is used verbatim to derive a fixed GOP.
//! - Single video stream plus a **single optional** audio stream
//!   (`0:a:0?`). No audio groups, no per-rendition audio.
//! - Keyframe alignment across renditions is pursued with **fixed-GOP codec
//!   AVOptions** (`g` / `keyint_min` / `sc_threshold`, plus closed-GOP
//!   `x264-params` for libx264) rather than
//!   [`Output::set_force_key_frames`](crate::Output::set_force_key_frames),
//!   which remains available for hand-built pipelines. With the same CFR
//!   input and GOP length, renditions' keyframe PTS sequences coincide, so
//!   segments split at the same PTS and are cross-switchable — as long as the
//!   chosen encoder honors those options. The explicit fixed-GOP/no-scenecut
//!   parameters are wired for libx264/libx265 (and encoders honoring the
//!   generic `g`/`sc_threshold` AVOptions); an arbitrary encoder passed to
//!   [`HlsLadder::video_codec`] may keep inserting scene-cut keyframes, and
//!   this recipe cannot verify alignment after the fact.
//!
//! Segments are MPEG-TS by default; [`HlsLadder::segment_type`] switches the
//! whole ladder to fragmented-MP4 segments (see [fMP4
//! segments](#fmp4-segments)).
//!
//! Out of scope for the MVP (future work): VFR input, exact cross-rendition
//! alignment guarantees, subtitles, audio groups (`EXT-X-MEDIA`), encryption
//! (AES-128 / SAMPLE-AES), and live/event playlists.
//!
//! # fMP4 segments
//!
//! [`HlsSegmentType::Fmp4`] writes fragmented-MP4 ("fMP4") HLS: each
//! rendition's media playlist references `.m4s` media segments plus an
//! `init.mp4` initialization segment announced via `EXT-X-MAP`, and the
//! master playlist declares `#EXT-X-VERSION:7` (matching what FFmpeg's own
//! `hls` muxer writes for fMP4 output).
//!
//! Expectations worth knowing up front:
//! - **One init segment per rendition.** Every rendition is its own FFmpeg
//!   `hls` muxer instance, so each rendition directory holds its own
//!   `init.mp4` next to its `index.m3u8`. That N-output layout is valid HLS —
//!   each media playlist references only its own init segment — but it
//!   differs from single-muxer `var_stream_map` setups where one muxer
//!   writes all variants.
//! - **"fMP4 HLS" is the claim, not CMAF or Apple compliance.** The output is
//!   verified structurally (init segment + `EXT-X-MAP` + `.m4s` segments,
//!   probe-able end to end); no Apple validation is performed.
//! - **HEVC:** Apple requires the `hvc1` sample-entry tag for HEVC, but
//!   libx265 typically emits `hev1` and this crate has no per-stream
//!   `codec_tag` option to rewrite it — prefer H.264 for Apple targets.
//!   [`HlsLadder::codecs`] only changes master-playlist metadata, never the
//!   encoded samples.
//!
//! # Example
//!
//! ```rust,ignore
//! use ez_ffmpeg::core::recipes::hls::{HlsLadder, HlsSegmentType};
//!
//! HlsLadder::new("input.mp4", "out/")
//!     .rendition(1920, 1080, "5000k")
//!     .rendition(1280, 720, "2800k")
//!     .rendition(640, 360, "800k")
//!     .segment_duration(6.0)
//!     .run()?;
//! // Produces out/master.m3u8 + out/1080p/, out/720p/, out/360p/.
//!
//! // Fragmented-MP4 segments instead of MPEG-TS:
//! HlsLadder::new("input.mp4", "out_fmp4/")
//!     .rendition(1280, 720, "2800k")
//!     .rendition(640, 360, "800k")
//!     .segment_type(HlsSegmentType::Fmp4)
//!     .run()?;
//! // Produces out_fmp4/master.m3u8 + per-rendition init.mp4 and .m4s segments.
//! # Ok::<(), ez_ffmpeg::error::Error>(())
//! ```
//!
//! [`Output::set_force_key_frames`]: crate::core::context::output::Output::set_force_key_frames

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::core::recipes::hls_master::{generate_master_playlist, MasterVariant};
use crate::core::stream_info::{find_audio_stream_info, find_video_stream_info, StreamInfo};
use crate::error::{Error, Result};

const DEFAULT_SEGMENT_DURATION: f64 = 6.0;
const DEFAULT_VIDEO_CODEC: &str = "libx264";
const DEFAULT_AUDIO_CODEC: &str = "aac";
const DEFAULT_AUDIO_BITRATE: &str = "128k";
const DEFAULT_MASTER_NAME: &str = "master.m3u8";
/// MPEG-TS segment filename template, resolved to a full path inside each
/// rendition dir.
const SEGMENT_TEMPLATE_TS: &str = "seg_%05d.ts";
/// fMP4 media-segment filename template, resolved to a full path inside each
/// rendition dir.
const SEGMENT_TEMPLATE_FMP4: &str = "seg_%05d.m4s";
/// fMP4 initialization-segment name. Deliberately a bare basename: FFmpeg's
/// `hls` muxer resolves it against each rendition playlist's directory, so
/// every rendition gets its own `init.mp4` without any path plumbing here.
const FMP4_INIT_FILENAME: &str = "init.mp4";
/// Muxing/overhead headroom folded into the reported `BANDWIDTH` (`+10%`),
/// expressed as an integer ratio so the result is deterministic (`f64 * 1.1`
/// can drift a bit above the true value and round the wrong way).
const BANDWIDTH_OVERHEAD_NUM: u64 = 11;
const BANDWIDTH_OVERHEAD_DEN: u64 = 10;
/// VBV `bufsize` as a multiple of the target video bitrate.
const BUFSIZE_MULTIPLIER: u64 = 2;

/// Container format for the ladder's HLS media segments.
///
/// Passed to [`HlsLadder::segment_type`]. The default is
/// [`MpegTs`](HlsSegmentType::MpegTs), which keeps the recipe's historical
/// output byte-identical; see the [module docs](self#fmp4-segments) for what
/// [`Fmp4`](HlsSegmentType::Fmp4) changes on disk.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HlsSegmentType {
    /// MPEG-TS segments (`seg_%05d.ts`) — the classic HLS container and the
    /// FFmpeg `hls` muxer default. The master playlist keeps the
    /// conservative `#EXT-X-VERSION:3`.
    #[default]
    MpegTs,
    /// Fragmented-MP4 segments (`seg_%05d.m4s`) plus one `init.mp4`
    /// initialization segment per rendition, announced in each media
    /// playlist via `EXT-X-MAP` (FFmpeg `hls_segment_type=fmp4`). The master
    /// playlist declares `#EXT-X-VERSION:7`.
    Fmp4,
}

impl HlsSegmentType {
    /// Segment filename template joined onto each rendition directory.
    fn segment_template(self) -> &'static str {
        match self {
            HlsSegmentType::MpegTs => SEGMENT_TEMPLATE_TS,
            HlsSegmentType::Fmp4 => SEGMENT_TEMPLATE_FMP4,
        }
    }

    /// `EXT-X-VERSION` declared by the generated master playlist.
    ///
    /// TS ladders keep the conservative `3`. fMP4 ladders write `7`:
    /// `EXT-X-MAP` requires protocol version 6+, and FFmpeg's own fMP4
    /// master playlists declare 7, so aligning avoids a version mismatch
    /// between our master and the FFmpeg-written media playlists.
    fn master_playlist_version(self) -> u32 {
        match self {
            HlsSegmentType::MpegTs => 3,
            HlsSegmentType::Fmp4 => 7,
        }
    }
}

/// A single quality level of the ladder.
#[derive(Debug, Clone)]
pub struct Rendition {
    /// Output width in pixels. Must be positive and even (yuv420p requirement).
    pub width: u32,
    /// Output height in pixels. Must be positive and even (yuv420p requirement).
    pub height: u32,
    /// Target video bitrate in FFmpeg syntax, e.g. `"5000k"` or `"5M"`.
    pub video_bitrate: String,
    /// Directory / variant name. Defaults to `"{height}p"` when `None`.
    pub name: Option<String>,
}

impl Rendition {
    /// Creates a rendition with the default `"{height}p"` directory name.
    pub fn new(width: u32, height: u32, video_bitrate: impl Into<String>) -> Self {
        Self {
            width,
            height,
            video_bitrate: video_bitrate.into(),
            name: None,
        }
    }

    /// Overrides the directory / variant name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Resolved directory name for this rendition (`name` or `"{height}p"`).
    fn dir_name(&self) -> String {
        match &self.name {
            Some(name) => name.clone(),
            None => format!("{}p", self.height),
        }
    }
}

/// Builder for a VOD ABR HLS ladder. See the [module docs](self) for the MVP
/// boundary and keyframe-alignment strategy.
pub struct HlsLadder {
    input: Input,
    out_dir: PathBuf,
    renditions: Vec<Rendition>,
    segment_duration_s: f64,
    /// GOP length in seconds; `None` tracks `segment_duration_s`.
    gop_seconds: Option<f64>,
    video_codec: String,
    audio_codec: String,
    audio_bitrate: String,
    master_name: String,
    /// Explicit CFR frame rate override `(num, den)`; required for inputs with
    /// no probe-able URL (e.g. callback inputs).
    fps: Option<(i32, i32)>,
    /// Optional `CODECS` attribute applied to every master variant. `None`
    /// omits the attribute (the conservative default).
    master_codecs: Option<String>,
    /// Media-segment container for every rendition (MPEG-TS or fMP4).
    segment_type: HlsSegmentType,
}

impl HlsLadder {
    /// Starts a new ladder for `input`, writing into `out_dir`.
    pub fn new(input: impl Into<Input>, out_dir: impl Into<PathBuf>) -> Self {
        Self {
            input: input.into(),
            out_dir: out_dir.into(),
            renditions: Vec::new(),
            segment_duration_s: DEFAULT_SEGMENT_DURATION,
            gop_seconds: None,
            video_codec: DEFAULT_VIDEO_CODEC.to_string(),
            audio_codec: DEFAULT_AUDIO_CODEC.to_string(),
            audio_bitrate: DEFAULT_AUDIO_BITRATE.to_string(),
            master_name: DEFAULT_MASTER_NAME.to_string(),
            fps: None,
            master_codecs: None,
            segment_type: HlsSegmentType::default(),
        }
    }

    /// Appends a rendition with the default `"{height}p"` name.
    pub fn rendition(self, width: u32, height: u32, video_bitrate: impl Into<String>) -> Self {
        self.rendition_named(Rendition::new(width, height, video_bitrate))
    }

    /// Appends a fully-specified [`Rendition`].
    pub fn rendition_named(mut self, rendition: Rendition) -> Self {
        self.renditions.push(rendition);
        self
    }

    /// Sets the target HLS segment duration in seconds (default `6.0`).
    ///
    /// Must be a positive integer multiple of the effective GOP length so that
    /// every segment boundary lands on a keyframe.
    pub fn segment_duration(mut self, seconds: f64) -> Self {
        self.segment_duration_s = seconds;
        self
    }

    /// Overrides the GOP length in seconds (default: equal to the segment
    /// duration). The segment duration must be an integer multiple of this.
    pub fn gop_seconds(mut self, seconds: f64) -> Self {
        self.gop_seconds = Some(seconds);
        self
    }

    /// Sets the video encoder (default `"libx264"`).
    pub fn video_codec(mut self, codec: impl Into<String>) -> Self {
        self.video_codec = codec.into();
        self
    }

    /// Sets the audio encoder (default `"aac"`).
    pub fn audio_codec(mut self, codec: impl Into<String>) -> Self {
        self.audio_codec = codec.into();
        self
    }

    /// Sets the audio bitrate (default `"128k"`).
    pub fn audio_bitrate(mut self, bitrate: impl Into<String>) -> Self {
        self.audio_bitrate = bitrate.into();
        self
    }

    /// Sets the master playlist filename (default `"master.m3u8"`).
    pub fn master(mut self, name: impl Into<String>) -> Self {
        self.master_name = name.into();
        self
    }

    /// Provides an explicit CFR frame rate `num/den`, bypassing input probing.
    ///
    /// Required when the input has no URL to probe (callback inputs). Both
    /// components must be positive.
    pub fn fps(mut self, num: i32, den: i32) -> Self {
        self.fps = Some((num, den));
        self
    }

    /// Sets the `CODECS` attribute string written for every master variant
    /// (e.g. `"avc1.640028,mp4a.40.2"`). Omitted by default because a precise
    /// codec string depends on the encoder's chosen profile/level.
    pub fn codecs(mut self, codecs: impl Into<String>) -> Self {
        self.master_codecs = Some(codecs.into());
        self
    }

    /// Sets the media-segment container (default
    /// [`HlsSegmentType::MpegTs`]).
    ///
    /// [`HlsSegmentType::Fmp4`] switches every rendition to fragmented-MP4
    /// segments with a per-rendition `init.mp4` initialization segment and
    /// bumps the master playlist to `#EXT-X-VERSION:7`. See the
    /// [module docs](self#fmp4-segments) for the exact on-disk layout.
    pub fn segment_type(mut self, segment_type: HlsSegmentType) -> Self {
        self.segment_type = segment_type;
        self
    }

    /// Builds the underlying [`FfmpegContext`] (consuming `self`, since [`Input`]
    /// is not `Clone`).
    ///
    /// This wires one HLS [`Output`] per rendition and creates the output
    /// directory tree — but the directories are created only **after** the
    /// context has been built, so a configuration rejected at build time (for
    /// example a video encoder the linked FFmpeg build lacks) leaves no empty
    /// rendition directories behind. It does **not** run the job or write the
    /// master playlist — that is [`run`](Self::run)'s job. Use this when you
    /// want to drive the [`crate::FfmpegScheduler`] yourself.
    pub fn build_context(self) -> Result<FfmpegContext> {
        self.validate()?;

        let (fps_num, fps_den) = self.resolve_fps()?;
        let gop_us = seconds_to_us(self.effective_gop_seconds());
        let gop_frames = compute_gop_frames(fps_num, fps_den, gop_us).to_string();
        let hls_time = format_seconds(self.segment_duration_s);
        let filter_desc = build_split_desc(&self.renditions);

        // Wire every rendition Output first (pure configuration, no
        // filesystem access), keeping directory creation in one block below.
        let mut outputs = Vec::with_capacity(self.renditions.len());
        for (i, rendition) in self.renditions.iter().enumerate() {
            outputs.push(self.rendition_output(i, rendition, &gop_frames, &hls_time)?);
        }
        let rendition_dirs: Vec<PathBuf> = self
            .renditions
            .iter()
            .map(|rendition| self.out_dir.join(rendition.dir_name()))
            .collect();

        // Consume `self` into owned locals so the move of `input` into the
        // builder does not conflict with the later use of `out_dir`.
        let HlsLadder { input, out_dir, .. } = self;

        let mut builder = FfmpegContext::builder()
            .input(input)
            .filter_desc(filter_desc);
        for output in outputs {
            builder = builder.output(output);
        }
        // Build first: encoder resolution happens inside build(), while
        // output-file I/O is deferred to start(), so the directories are only
        // needed before start(). Creating them after build() succeeds means a
        // rejected configuration (e.g. an unavailable encoder) leaves no empty
        // directory tree behind.
        let context = builder.build()?;

        create_dir(&out_dir)?;
        for rendition_dir in &rendition_dirs {
            create_dir(rendition_dir)?;
        }

        Ok(context)
    }

    /// Wires one rendition's HLS [`Output`]: playlist path, segment template,
    /// codecs, fixed-GOP options, and the segment-type-specific muxer options.
    /// Pure configuration — no filesystem access — so tests can assert the
    /// exact option wiring without touching disk.
    fn rendition_output(
        &self,
        index: usize,
        rendition: &Rendition,
        gop_frames: &str,
        hls_time: &str,
    ) -> Result<Output> {
        let rendition_dir = self.out_dir.join(rendition.dir_name());
        let playlist_path = path_to_utf8(&rendition_dir.join("index.m3u8"))?;
        let segment_template =
            path_to_utf8(&rendition_dir.join(self.segment_type.segment_template()))?;

        // Validated already; recompute the numeric bitrate for VBV bufsize.
        let video_bps = parse_bitrate_bps(&rendition.video_bitrate)?;
        let bufsize = video_bps.saturating_mul(BUFSIZE_MULTIPLIER).to_string();

        let mut output = Output::from(playlist_path)
            .set_format("hls")
            // Map this rendition's scaled video pad, then the (optional)
            // first audio stream shared across renditions.
            .add_stream_map(format!("[v{index}]"))
            .add_stream_map("0:a:0?")
            .set_video_codec(self.video_codec.as_str())
            .set_video_bitrate(rendition.video_bitrate.clone())
            .set_audio_codec(self.audio_codec.as_str())
            .set_audio_bitrate(self.audio_bitrate.clone())
            .set_pix_fmt("yuv420p")
            // Fixed GOP == segment for cross-rendition keyframe alignment.
            .set_video_codec_opt("g", gop_frames)
            .set_video_codec_opt("keyint_min", gop_frames)
            .set_video_codec_opt("sc_threshold", "0")
            // VBV constraint so the reported BANDWIDTH is meaningful.
            .set_video_codec_opt("maxrate", rendition.video_bitrate.clone())
            .set_video_codec_opt("bufsize", bufsize)
            .set_format_opt("hls_time", hls_time)
            .set_format_opt("hls_playlist_type", "vod")
            .set_format_opt("hls_segment_filename", segment_template);

        if self.segment_type == HlsSegmentType::Fmp4 {
            // `hls_segment_type=fmp4` switches the muxer to `.m4s` media
            // segments plus an initialization segment. The init filename is
            // pinned explicitly (rather than relying on the muxer default)
            // so the on-disk contract documented on `HlsSegmentType::Fmp4`
            // cannot drift with FFmpeg versions.
            output = output
                .set_format_opt("hls_segment_type", "fmp4")
                .set_format_opt("hls_fmp4_init_filename", FMP4_INIT_FILENAME);
        }

        // Closed GOP (scenecut disabled, open-gop off) so each segment is
        // independently decodable. libx264/libx265 take their `*-params`
        // here; for other encoders the fixed GOP above still applies but
        // closed-GOP/scenecut can't be guaranteed (documented limitation).
        if self.video_codec.eq_ignore_ascii_case("libx264") {
            output = output.set_video_codec_opt("x264-params", "scenecut=0:open-gop=0");
        } else if self.video_codec.eq_ignore_ascii_case("libx265") {
            output = output.set_video_codec_opt("x265-params", "scenecut=0:open-gop=0");
        }

        Ok(output)
    }

    /// Runs the ladder to completion, then writes the master playlist.
    ///
    /// The master playlist text is computed up front but only written to disk
    /// **after** the transcode succeeds, so a failed run leaves no dangling
    /// `master.m3u8`. Transcode errors propagate from
    /// [`start`](FfmpegContext::start) / [`wait`](crate::FfmpegScheduler::wait).
    pub fn run(self) -> Result<()> {
        self.validate()?;

        // Gather everything the master playlist needs before `self` is consumed.
        let has_audio = self.resolve_has_audio()?;
        let variants = self.build_master_variants(has_audio)?;
        let master_text =
            generate_master_playlist(&variants, self.segment_type.master_playlist_version());
        let master_path = self.out_dir.join(&self.master_name);

        let context = self.build_context()?;
        let scheduler = context.start()?;
        scheduler.wait()?;

        std::fs::write(&master_path, master_text).map_err(|e| {
            Error::InvalidRecipeArg(format!(
                "failed to write master playlist '{}': {e}",
                master_path.display()
            ))
        })?;
        Ok(())
    }

    /// Effective GOP length in seconds (`gop_seconds` or the segment duration).
    fn effective_gop_seconds(&self) -> f64 {
        self.gop_seconds.unwrap_or(self.segment_duration_s)
    }

    /// Validates renditions, durations, dimensions, and names. Performs no I/O.
    fn validate(&self) -> Result<()> {
        if self.renditions.is_empty() {
            return Err(Error::InvalidRecipeArg(
                "HLS ladder requires at least one rendition".to_string(),
            ));
        }

        // The master filename is joined onto `out_dir` and written to disk, so
        // it must be a safe single segment just like the rendition names.
        validate_path_segment(&self.master_name, "master playlist name")?;

        // A CODECS override is written verbatim inside a quoted playlist
        // attribute; reject characters that would break out of or inject into it.
        if let Some(codecs) = &self.master_codecs {
            if codecs.chars().any(|c| c == '"' || c.is_control()) {
                return Err(Error::InvalidRecipeArg(
                    "codecs must not contain quotes or control characters".to_string(),
                ));
            }
        }

        if !self.segment_duration_s.is_finite() || self.segment_duration_s <= 0.0 {
            return Err(Error::InvalidRecipeArg(format!(
                "segment_duration must be finite and positive, got {}",
                self.segment_duration_s
            )));
        }

        let gop = self.effective_gop_seconds();
        if !gop.is_finite() || gop <= 0.0 {
            return Err(Error::InvalidRecipeArg(format!(
                "gop_seconds must be finite and positive, got {gop}"
            )));
        }

        // Segment boundaries only land on keyframes when the segment is an
        // integer multiple of the GOP length.
        let ratio = self.segment_duration_s / gop;
        if !ratio.is_finite() || ratio.round() < 1.0 || (ratio - ratio.round()).abs() > 1e-9 {
            return Err(Error::InvalidRecipeArg(format!(
                "segment_duration ({}) must be a positive integer multiple of gop_seconds ({gop})",
                self.segment_duration_s
            )));
        }

        // Reject a malformed audio bitrate even if the source turns out to have
        // no audio; the setting is otherwise silently unused.
        parse_bitrate_bps(&self.audio_bitrate)?;

        if let Some((num, den)) = self.fps {
            if num <= 0 || den <= 0 {
                return Err(Error::InvalidRecipeArg(format!(
                    "fps override must be positive, got {num}/{den}"
                )));
            }
        }

        let mut names = HashSet::new();
        for rendition in &self.renditions {
            if rendition.width == 0
                || rendition.height == 0
                || rendition.width % 2 != 0
                || rendition.height % 2 != 0
            {
                return Err(Error::InvalidRecipeArg(format!(
                    "rendition dimensions must be positive and even, got {}x{}",
                    rendition.width, rendition.height
                )));
            }
            parse_bitrate_bps(&rendition.video_bitrate)?;

            let name = rendition.dir_name();
            validate_rendition_name(&name)?;
            if !names.insert(name.clone()) {
                return Err(Error::InvalidRecipeArg(format!(
                    "duplicate rendition name '{name}'"
                )));
            }
        }

        // The master is written into `out_dir`; a name equal to a rendition
        // directory would make `fs::write` target a directory and fail only
        // after a successful transcode.
        if names.contains(&self.master_name) {
            return Err(Error::InvalidRecipeArg(format!(
                "master playlist name '{}' collides with a rendition directory name",
                self.master_name
            )));
        }

        Ok(())
    }

    /// Resolves the CFR frame rate as a rational `(num, den)`.
    ///
    /// Uses the explicit [`fps`](Self::fps) override when set; otherwise probes
    /// the input URL via [`find_video_stream_info`]. Errors when the frame rate
    /// is missing/zero (unusable for a fixed GOP) or the input cannot be probed.
    fn resolve_fps(&self) -> Result<(i64, i64)> {
        if let Some((num, den)) = self.fps {
            if num <= 0 || den <= 0 {
                return Err(Error::InvalidRecipeArg(format!(
                    "fps override must be positive, got {num}/{den}"
                )));
            }
            return Ok((num as i64, den as i64));
        }

        let url = self.input.url.as_ref().ok_or_else(|| {
            Error::InvalidRecipeArg(
                "cannot probe frame rate: input has no URL. Provide an explicit fps() override \
                 for callback inputs (CFR is required)"
                    .to_string(),
            )
        })?;

        let info = find_video_stream_info(url.as_str())?.ok_or_else(|| {
            Error::InvalidRecipeArg(format!("no video stream found in input '{url}'"))
        })?;

        match info {
            StreamInfo::Video { avg_frame_rate, .. } => {
                let (num, den) = (avg_frame_rate.num, avg_frame_rate.den);
                if num <= 0 || den <= 0 {
                    return Err(Error::InvalidRecipeArg(format!(
                        "input '{url}' has an unusable frame rate ({num}/{den}); ABR ladders \
                         require CFR — provide an explicit fps() override"
                    )));
                }
                Ok((num as i64, den as i64))
            }
            _ => Err(Error::InvalidRecipeArg(format!(
                "no video stream found in input '{url}'"
            ))),
        }
    }

    /// Determines whether the master playlist should declare audio.
    ///
    /// Probes the input URL for an audio stream. Callback inputs (no URL)
    /// cannot be probed and are treated as audio-less for the master; the
    /// `0:a:0?` map still passes any real audio into the media playlists.
    fn resolve_has_audio(&self) -> Result<bool> {
        match self.input.url.as_ref() {
            Some(url) => Ok(find_audio_stream_info(url.as_str())?.is_some()),
            None => Ok(false),
        }
    }

    /// Builds the master playlist variant descriptors from the renditions.
    fn build_master_variants(&self, has_audio: bool) -> Result<Vec<MasterVariant>> {
        let audio_bps = if has_audio {
            Some(parse_bitrate_bps(&self.audio_bitrate)?)
        } else {
            None
        };

        let mut variants = Vec::with_capacity(self.renditions.len());
        for rendition in &self.renditions {
            let video_bps = parse_bitrate_bps(&rendition.video_bitrate)?;
            variants.push(MasterVariant {
                bandwidth: compute_bandwidth(video_bps, audio_bps),
                width: rendition.width,
                height: rendition.height,
                uri: format!("{}/index.m3u8", rendition.dir_name()),
                codecs: self.master_codecs.clone(),
            });
        }
        Ok(variants)
    }
}

/// Builds the `split` + per-branch `scale`/`format` filtergraph description.
///
/// For N renditions:
/// `"[0:v]split=N[s0][s1]...;[s0]scale=w0:h0,format=yuv420p[v0];..."`.
fn build_split_desc(renditions: &[Rendition]) -> String {
    let n = renditions.len();
    let mut desc = format!("[0:v]split={n}");
    for i in 0..n {
        desc.push_str(&format!("[s{i}]"));
    }
    for (i, rendition) in renditions.iter().enumerate() {
        desc.push_str(&format!(
            ";[s{i}]scale={}:{},setsar=1,format=yuv420p[v{i}]",
            rendition.width, rendition.height
        ));
    }
    desc
}

/// Frames per GOP for a rational frame rate over `dur_us` microseconds,
/// rounded **up** so the GOP never falls short of the interval.
///
/// `gop = ceil(fps_num * dur_us / (fps_den * 1_000_000))`, computed with
/// integer math so rates like `30000/1001` are not corrupted by float
/// rounding. The result is clamped to at least 1.
fn compute_gop_frames(fps_num: i64, fps_den: i64, dur_us: i64) -> u64 {
    if fps_num <= 0 || fps_den <= 0 || dur_us <= 0 {
        return 1;
    }
    let numerator = (fps_num as i128) * (dur_us as i128);
    let denominator = (fps_den as i128) * 1_000_000i128;
    // Ceiling division of two positive integers.
    let gop = (numerator + denominator - 1) / denominator;
    u64::try_from(gop.max(1)).unwrap_or(u64::MAX)
}

/// Reported peak `BANDWIDTH` (bps): video + optional audio plus muxing overhead.
fn compute_bandwidth(video_bps: u64, audio_bps: Option<u64>) -> u64 {
    // Widen to u128 so the overhead multiply can't wrap, then saturate to u64.
    let base = u128::from(video_bps.saturating_add(audio_bps.unwrap_or(0)));
    let scaled = base * u128::from(BANDWIDTH_OVERHEAD_NUM) / u128::from(BANDWIDTH_OVERHEAD_DEN);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

/// Parses an FFmpeg-style bitrate (`"5000k"`, `"5M"`, `"800000"`) into bits/sec.
fn parse_bitrate_bps(spec: &str) -> Result<u64> {
    let trimmed = spec.trim();
    if trimmed.is_empty() {
        return Err(Error::InvalidRecipeArg(
            "bitrate must not be empty".to_string(),
        ));
    }

    // Split off a single-letter SI-ish suffix without byte-slicing the string.
    let (number, multiplier) = if let Some(rest) = trimmed.strip_suffix(|c| c == 'k' || c == 'K') {
        (rest, 1_000f64)
    } else if let Some(rest) = trimmed.strip_suffix(|c| c == 'm' || c == 'M') {
        (rest, 1_000_000f64)
    } else if let Some(rest) = trimmed.strip_suffix(|c| c == 'g' || c == 'G') {
        (rest, 1_000_000_000f64)
    } else {
        (trimmed, 1f64)
    };

    let value: f64 = number
        .trim()
        .parse()
        .map_err(|_| Error::InvalidRecipeArg(format!("invalid bitrate '{spec}'")))?;
    if !value.is_finite() || value <= 0.0 {
        return Err(Error::InvalidRecipeArg(format!(
            "bitrate must be positive, got '{spec}'"
        )));
    }
    Ok((value * multiplier).round() as u64)
}

/// Rejects a path segment (a rendition directory name or the master playlist
/// filename) that could escape the output directory, or inject a line into a
/// generated `.m3u8` via an embedded newline / control character.
fn validate_path_segment(name: &str, what: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidRecipeArg(format!("{what} must not be empty")));
    }
    if name == "." || name == ".." || name.contains("..") {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} '{name}' must not contain '..'"
        )));
    }
    if name.contains('/') || name.contains('\\') {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} '{name}' must not contain a path separator"
        )));
    }
    if name.chars().any(char::is_control) {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} must not contain control characters"
        )));
    }
    if name.starts_with('#') {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} '{name}' must not start with '#' (it would break the HLS playlist URI line)"
        )));
    }
    if Path::new(name).is_absolute() {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} '{name}' must be a relative path"
        )));
    }
    Ok(())
}

/// Rejects rendition names that could escape the output directory.
fn validate_rendition_name(name: &str) -> Result<()> {
    validate_path_segment(name, "rendition name")
}

/// Converts a path to an owned UTF-8 `String`, erroring on non-UTF-8 paths
/// (FFmpeg option strings must be UTF-8). On Windows, separators are
/// normalized to forward slashes: the hls muxer derives the fMP4 init
/// segment's directory with a forward-slash-only `strrchr` on the playlist
/// path (hlsenc has no DOS-path handling there), so a `\`-separated path
/// silently drops `init.mp4` outside the rendition directory. Windows file
/// APIs accept `/` throughout, so normalizing is lossless.
fn path_to_utf8(path: &Path) -> Result<String> {
    let utf8 = path.to_str().map(str::to_string).ok_or_else(|| {
        Error::InvalidRecipeArg(format!("path is not valid UTF-8: {}", path.display()))
    })?;
    // Verbatim (`\\?\`) paths are prefix-sensitive: rewriting their
    // separators would change which object the path names, so they pass
    // through untouched (prefer regular paths for fMP4 output on Windows).
    #[cfg(windows)]
    let utf8 = if utf8.starts_with(r"\\?\") {
        utf8
    } else {
        utf8.replace('\\', "/")
    };
    Ok(utf8)
}

/// Creates `dir` (and parents), mapping I/O failures to a recipe argument error.
fn create_dir(dir: &Path) -> Result<()> {
    std::fs::create_dir_all(dir).map_err(|e| {
        Error::InvalidRecipeArg(format!(
            "failed to create output directory {}: {e}",
            dir.display()
        ))
    })
}

/// Whole microseconds for a duration in seconds.
fn seconds_to_us(seconds: f64) -> i64 {
    (seconds * 1_000_000.0).round() as i64
}

/// Formats a seconds value for FFmpeg's `hls_time` (drops a redundant `.0`).
fn format_seconds(seconds: f64) -> String {
    seconds.to_string()
}

#[cfg(test)]
mod tests;
