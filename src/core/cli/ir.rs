//! Typed intermediate representation shared by the run and emit paths.
//!
//! One command parses into exactly one [`CliIr`]; both `from_cli_args`
//! (execution) and `emit_rust_code` (code generation) lower the SAME value
//! through [`super::lower`], so the two can never drift apart.
//!
//! Round 1 is single-input/single-output by construction, so the IR holds one
//! `InputIr` and one `OutputIr` instead of vectors — a second `-i` or output
//! path is rejected during parsing, before an IR exists.

/// Which of `-t` / `-to` produced a duration bound. The two are distinct in
/// the CLI (`-t` = duration, `-to` = absolute end position) and lower to
/// different builder calls; same-scope coexistence is rejected at parse time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DurationKind {
    /// `-t`: duration in microseconds.
    Duration,
    /// `-to`: absolute end position in microseconds.
    StopAt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DurationBound {
    pub(crate) kind: DurationKind,
    pub(crate) us: i64,
    /// Argv index of the option token that set this bound (diagnostic
    /// anchor for the same-scope -t/-to conflict).
    pub(crate) token_index: usize,
}

/// A recognized no-op global (`-hide_banner`, `-loglevel …`, …): legal in the
/// subset, no in-process equivalent, carried through so the emitter can note
/// it instead of silently swallowing it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Noop {
    pub(crate) flag: String,
    pub(crate) value: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InputIr {
    pub(crate) url: String,
    /// Input `-f` (demuxer name).
    pub(crate) format: Option<String>,
    /// Input-side `-ss`, microseconds.
    pub(crate) start_time_us: Option<i64>,
    /// Input-side `-t` or `-to`.
    pub(crate) duration: Option<DurationBound>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OutputIr {
    pub(crate) url: String,
    /// Output `-f` (muxer name).
    pub(crate) format: Option<String>,
    /// Output-side `-ss`, microseconds (decode-then-discard trim).
    pub(crate) start_time_us: Option<i64>,
    /// Output-side `-t` or `-to`.
    pub(crate) duration: Option<DurationBound>,
    /// `-vn`.
    pub(crate) video_disabled: bool,
    /// `-an`.
    pub(crate) audio_disabled: bool,
    /// `-c:v` (never "copy" here — `-c:v copy` sets `video_copy`).
    pub(crate) video_codec: Option<String>,
    /// `-c:a` (never "copy" here — `-c:a copy` sets `audio_copy`).
    pub(crate) audio_codec: Option<String>,
    /// `-c:v copy`.
    pub(crate) video_copy: bool,
    /// `-c:a copy`.
    pub(crate) audio_copy: bool,
    /// `-b:v`, kept in CLI spelling (`2M`, `2600k`, `500000`).
    pub(crate) video_bitrate: Option<String>,
    /// `-b:a`, kept in CLI spelling.
    pub(crate) audio_bitrate: Option<String>,
    /// `-crf`, validated integer kept as spelled.
    pub(crate) crf: Option<String>,
    /// `-preset`, whitelisted x264 preset name.
    pub(crate) preset: Option<String>,
    /// `-pix_fmt`.
    pub(crate) pix_fmt: Option<String>,
    /// `-ar`.
    pub(crate) audio_sample_rate: Option<i32>,
    /// `-ac`.
    pub(crate) audio_channels: Option<i32>,
    /// `-frames:v` (Round 1 accepts only the value 1).
    pub(crate) frames_v: Option<i64>,
    /// `-vf` (Round 1: a single scale filter).
    pub(crate) video_filter: Option<String>,
    /// `-map` values, basic index form only (`0`, `0:v`, `0:a:1`, …).
    pub(crate) maps: Vec<String>,
    /// Exact `-movflags +faststart`.
    pub(crate) movflags_faststart: bool,
    /// `-hls_time`, seconds as spelled.
    pub(crate) hls_time: Option<String>,
    /// `-hls_playlist_type` (Round 1: exactly `vod`).
    pub(crate) hls_playlist_type: Option<String>,
    /// `-hls_list_size` (Round 1: exactly `0`).
    pub(crate) hls_list_size: Option<String>,
    /// `-hls_segment_filename`.
    pub(crate) hls_segment_filename: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GlobalsIr {
    /// `-y` (mandatory in the subset).
    pub(crate) overwrite: bool,
    /// Recognized no-op globals, in command order.
    pub(crate) noops: Vec<Noop>,
}

/// The parsed command. Fields mirror the manifest's accept surface; anything
/// not representable here was rejected before construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CliIr {
    pub(crate) globals: GlobalsIr,
    pub(crate) input: InputIr,
    pub(crate) output: OutputIr,
    /// Argv anchor per consumed option: `(scope-qualified canonical key,
    /// index of the option token)`, first occurrence. Post-parse combination
    /// diagnostics pull their positions from here.
    pub(crate) spans: Vec<(String, usize)>,
}

impl CliIr {
    /// First recorded argv index of a scope-qualified option key.
    pub(crate) fn span(&self, key: &str) -> Option<usize> {
        self.spans
            .iter()
            .find(|(recorded, _)| recorded == key)
            .map(|(_, index)| *index)
    }
}

impl CliIr {
    /// The command's semantic option fingerprint: the sorted list of
    /// scope-qualified canonical option keys present in the IR. Shape
    /// verification compares this against the manifest's verified shapes.
    /// No-op globals and the mandatory `-y` are excluded — they never change
    /// execution semantics.
    pub(crate) fn fingerprint(&self) -> Vec<&'static str> {
        let mut keys: Vec<&'static str> = Vec::new();
        let input = &self.input;
        let output = &self.output;
        if input.format.is_some() {
            keys.push("in:-f");
        }
        if input.start_time_us.is_some() {
            keys.push("in:-ss");
        }
        match input.duration.map(|d| d.kind) {
            Some(DurationKind::Duration) => keys.push("in:-t"),
            Some(DurationKind::StopAt) => keys.push("in:-to"),
            None => {}
        }
        if output.format.is_some() {
            keys.push("out:-f");
        }
        if output.start_time_us.is_some() {
            keys.push("out:-ss");
        }
        match output.duration.map(|d| d.kind) {
            Some(DurationKind::Duration) => keys.push("out:-t"),
            Some(DurationKind::StopAt) => keys.push("out:-to"),
            None => {}
        }
        if output.video_disabled {
            keys.push("out:-vn");
        }
        if output.audio_disabled {
            keys.push("out:-an");
        }
        if output.video_codec.is_some() {
            keys.push("out:-c:v");
        }
        if output.audio_codec.is_some() {
            keys.push("out:-c:a");
        }
        if output.video_copy {
            keys.push("out:-c:v copy");
        }
        if output.audio_copy {
            keys.push("out:-c:a copy");
        }
        if output.video_bitrate.is_some() {
            keys.push("out:-b:v");
        }
        if output.audio_bitrate.is_some() {
            keys.push("out:-b:a");
        }
        if output.crf.is_some() {
            keys.push("out:-crf");
        }
        if output.preset.is_some() {
            keys.push("out:-preset");
        }
        if output.pix_fmt.is_some() {
            keys.push("out:-pix_fmt");
        }
        if output.audio_sample_rate.is_some() {
            keys.push("out:-ar");
        }
        if output.audio_channels.is_some() {
            keys.push("out:-ac");
        }
        if output.frames_v.is_some() {
            keys.push("out:-frames:v");
        }
        if output.video_filter.is_some() {
            keys.push("out:-vf");
        }
        if !output.maps.is_empty() {
            keys.push("out:-map");
        }
        if output.movflags_faststart {
            keys.push("out:-movflags");
        }
        if output.hls_time.is_some() {
            keys.push("out:-hls_time");
        }
        if output.hls_playlist_type.is_some() {
            keys.push("out:-hls_playlist_type");
        }
        if output.hls_list_size.is_some() {
            keys.push("out:-hls_list_size");
        }
        if output.hls_segment_filename.is_some() {
            keys.push("out:-hls_segment_filename");
        }
        keys.sort_unstable();
        keys
    }
}
