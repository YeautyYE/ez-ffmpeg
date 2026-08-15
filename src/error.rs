use ffmpeg_next::ffi::AVERROR;
use ffmpeg_sys_next::*;
use std::ffi::NulError;
use std::{io, result};

// The `opengl` module path is deprecated as a whole (superseded by
// `wgpu_filter`), but the crate error enum must still name its typed error;
// importing it here, with the module-path deprecation silenced, keeps the
// variant and thiserror's generated `From` impl warning-free.
#[cfg(feature = "opengl")]
#[allow(deprecated)]
use crate::opengl::OpenGLFilterError;

/// Result type of all ez-ffmpeg library calls.
pub type Result<T, E = Error> = result::Result<T, E>;

/// Top-level error type for all ez-ffmpeg operations.
///
/// Most variants wrap a stage-specific error enum (opening inputs and
/// outputs, demuxing, decoding, filtering, encoding, muxing, ...), so
/// callers can match on the pipeline stage first and inspect the typed
/// cause when they need to.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Returned when an operation requires a scheduler that has been
    /// started, but it has not been.
    #[error("Scheduler is not started")]
    NotStarted,

    /// A URL or path string could not be converted into a C string for
    /// FFmpeg (see [`UrlError`]).
    #[error("URL error: {0}")]
    Url(#[from] UrlError),

    /// Opening an input file, stream, device, or custom input source failed.
    #[error("Open input stream error: {0}")]
    OpenInputStream(#[from] OpenInputError),

    /// Probing stream information from an opened input failed.
    #[error("Find stream info error: {0}")]
    FindStream(#[from] FindStreamError),

    /// Resolving a decoder failed (see [`DecoderError`]).
    #[error("Decoder error: {0}")]
    Decoder(#[from] DecoderError),

    /// Parsing a filtergraph description failed.
    #[error("Filter graph parse error: {0}")]
    FilterGraphParse(#[from] FilterGraphParseError),

    /// Returned when a filtergraph link label could not be converted to a
    /// UTF-8 string.
    #[error("Filter description converted to utf8 string error")]
    FilterDescUtf8,

    /// Returned when a filter name could not be converted to a UTF-8 string.
    #[error("Filter name converted to utf8 string error")]
    FilterNameUtf8,

    /// Returned when a filtergraph declares zero outputs, which is not
    /// supported.
    #[error("A filtergraph has zero outputs, this is not supported")]
    FilterZeroOutputs,

    /// Returned when a filtergraph declares zero inputs, which is not
    /// supported.
    #[error("A filtergraph has zero inputs, this is not supported")]
    FilterZeroInputs,

    /// Returned when a numeric field — such as a file index in a stream
    /// specifier or link label — could not be parsed as an integer.
    #[error("Input is not a valid number")]
    ParseInteger,

    /// Allocating an output format context failed.
    #[error("Alloc output context error: {0}")]
    AllocOutputContext(#[from] AllocOutputContextError),

    /// Opening or configuring an output failed.
    #[error("Open output error: {0}")]
    OpenOutput(#[from] OpenOutputError),

    /// Returned when an output URL is identical to one of the input URLs;
    /// the payload is the offending path. In-place editing is not supported.
    #[error("Output file '{0}' is the same as an input file")]
    FileSameAsInput(String),

    /// Enumerating capture devices failed.
    #[error("Find devices error: {0}")]
    FindDevices(#[from] FindDevicesError),

    /// Allocating an `AVFrame` failed.
    #[error("Alloc frame error: {0}")]
    AllocFrame(#[from] AllocFrameError),

    /// Allocating an `AVPacket` failed.
    #[error("Alloc packet error: {0}")]
    AllocPacket(#[from] AllocPacketError),

    /// Making a frame's data buffers writable failed (see
    /// [`FrameWritableError`]).
    #[error("Frame writable error: {0}")]
    FrameWritable(#[from] FrameWritableError),

    // ---- Muxing ----
    /// A muxing operation failed while writing the output container.
    #[error("Muxing operation failed {0}")]
    Muxing(#[from] MuxingOperationError),

    // ---- Open Encoder ----
    /// Opening or configuring an encoder failed.
    #[error("Open encoder operation failed {0}")]
    OpenEncoder(#[from] OpenEncoderOperationError),

    // ---- Encoding ----
    /// An encoding operation failed.
    #[error("Encoding operation failed {0}")]
    Encoding(#[from] EncodingOperationError),

    // ---- FilterGraph ----
    /// A filtergraph runtime operation failed.
    #[error("Filter graph operation failed {0}")]
    FilterGraph(#[from] FilterGraphOperationError),

    // ---- Open Decoder ----
    /// Opening or configuring a decoder failed.
    #[error("Open decoder operation failed {0}")]
    OpenDecoder(#[from] OpenDecoderOperationError),

    // ---- Decoding ----
    /// A decoding operation failed.
    #[error("Decoding operation failed {0}")]
    Decoding(#[from] DecodingOperationError),

    // ---- Demuxing ----
    /// A demuxing operation failed.
    #[error("Demuxing operation failed {0}")]
    Demuxing(#[from] DemuxingOperationError),

    // ---- Packet Scanner ----
    /// A packet-scanning operation failed (see [`PacketScannerError`]).
    #[error("Packet scanner error: {0}")]
    PacketScanner(#[from] PacketScannerError),

    // ---- Frame Filter ----
    /// A frame filter failed to initialize; carries the error returned by
    /// the filter's `init`.
    #[error("Frame filter init failed: {0}")]
    FrameFilterInit(Box<dyn std::error::Error + Send + Sync>),

    /// A frame filter failed while processing a frame; carries the error
    /// returned by the filter's `filter_frame`.
    #[error("Frame filter process failed: {0}")]
    FrameFilterProcess(Box<dyn std::error::Error + Send + Sync>),

    /// A frame filter failed while generating a frame; carries the error
    /// returned by the filter's `request_frame`.
    #[error("Frame filter request failed: {0}")]
    FrameFilterRequest(Box<dyn std::error::Error + Send + Sync>),

    /// Returned while building a frame pipeline when no stream of the
    /// required media type exists at the named pipeline end; fields are the
    /// pipeline end (input/output) and the media type.
    #[error("No {0} stream of the type:{1} were found while build frame pipeline")]
    FrameFilterTypeNoMatched(String, String),

    /// Returned while building a frame pipeline when no stream at the named
    /// pipeline end matches both the requested stream index and media type;
    /// fields are the pipeline end, the stream index, and the media type.
    #[error("{0} stream:{1} of the type:{2} were mismatched while build frame pipeline")]
    FrameFilterStreamTypeNoMatched(String, usize, String),

    /// Returned when a frame pipeline tries to deliver a frame to a
    /// destination that has already finished.
    #[error("Frame filter pipeline destination already finished")]
    FrameFilterDstFinished,

    /// Returned when a frame pipeline fails to duplicate a frame required
    /// for an additional destination.
    #[error("Frame filter pipeline failed to duplicate a frame for an additional destination")]
    FrameFilterFrameDuplicateFailed,

    /// Returned when spawning a frame pipeline's worker thread fails, so
    /// the pipeline never ran.
    #[error("Frame filter pipeline thread exited")]
    FrameFilterThreadExited,

    /// A worker thread panicked; the payload is the worker's thread name.
    /// Output may be incomplete.
    #[error("Worker thread '{0}' panicked; output may be incomplete")]
    WorkerPanicked(String),

    /// Recorded as the scheduler result when `start()` fails after some
    /// worker threads were already launched. `start()` itself returns the
    /// actual init error to its caller; this recorded value is what
    /// concurrent observers (packet-sink terminal callbacks) report, so a
    /// sink can never mistake a torn-down startup for a settled-Ok job.
    #[error("Scheduler start failed; the job was torn down during startup")]
    StartFailed,

    /// Returned when publishing to the embedded RTMP server with a stream
    /// key that is already in use; the payload is the key.
    #[cfg(feature = "rtmp")]
    #[error("Rtmp stream already exists with key: {0}")]
    RtmpStreamAlreadyExists(String),

    /// Returned when a stream could not be created on the embedded RTMP
    /// server, typically because the server has stopped.
    #[cfg(feature = "rtmp")]
    #[error("Rtmp create stream failed. Check whether the server is stopped.")]
    RtmpCreateStream,

    /// Returned when too many streams are waiting to be registered on the
    /// embedded RTMP server.
    #[cfg(feature = "rtmp")]
    #[error("Rtmp registration queue is full: too many streams are waiting to be registered")]
    RtmpRegistrationQueueFull,

    /// Returned when the embedded RTMP server's thread has exited.
    #[cfg(feature = "rtmp")]
    #[error("Rtmp server thread exited")]
    RtmpThreadExited,

    /// Returned when the embedded RTMP server is no longer consuming a
    /// published stream.
    #[cfg(feature = "rtmp")]
    #[error("Rtmp stream closed: the server is no longer consuming this stream")]
    RtmpStreamClosed,

    /// Returned when starting an embedded RTMP server that was already
    /// started; clones of one server share a single lifecycle that can be
    /// started only once.
    #[cfg(feature = "rtmp")]
    #[error("Rtmp server already started: clones of one server share a single lifecycle, which can be started only once")]
    RtmpServerAlreadyStarted,

    /// A subtitle processing operation failed.
    #[cfg(feature = "subtitle")]
    #[error("Subtitle error: {0}")]
    Subtitle(#[from] crate::subtitle::SubtitleError),

    /// A wgpu GPU filter operation failed.
    #[cfg(feature = "wgpu")]
    #[error("Wgpu filter error: {0}")]
    WgpuFilter(#[from] crate::wgpu_filter::WgpuFilterError),

    // The allow covers the deprecation that OpenGLFilterError inherits from
    // the deprecated `opengl` module; the variant must still carry the type.
    // From is hand-written below the enum (a derived #[from] would re-name
    // the type in generated code that no #[allow] on the variant reaches).
    /// An OpenGL filter operation failed (deprecated `opengl` feature).
    #[cfg(feature = "opengl")]
    #[allow(deprecated)]
    #[error("OpenGL filter error: {0}")]
    OpenGLFilter(#[source] OpenGLFilterError),

    /// An I/O error from the standard library.
    #[error("IO error:{0}")]
    IO(#[from] io::Error),

    /// Internal end-of-stream marker passed between pipeline stages; a
    /// normal end of input is consumed internally rather than reported as
    /// a job failure.
    #[error("EOF")]
    EOF,
    /// Internal control-flow marker instructing pipeline stages to shut
    /// down; normally consumed internally.
    #[error("Exit")]
    Exit,
    /// Internal invariant violation that should never occur; indicates a
    /// bug in this crate rather than a problem with user input.
    #[error("Bug")]
    Bug,

    /// Returned when a recipe or analysis option is invalid (out of range,
    /// malformed, or inconsistent); the payload describes the problem.
    #[error("Invalid recipe argument: {0}")]
    InvalidRecipeArg(String),

    /// A decoded frame could not be analyzed (unsupported pixel format,
    /// interlaced fields, or a hardware surface). This is a runtime frame
    /// condition, not a recipe/config error — those stay
    /// [`InvalidRecipeArg`](Error::InvalidRecipeArg). Boxed so [`Error`]
    /// stays within the 64-byte layout contract.
    #[error("analysis frame error: {0}")]
    AnalysisFrame(Box<str>),

    /// HLS ladder video-encoder selection failed. Boxed so the payload stays
    /// inside the crate-wide [`Error`] size contract.
    #[error("{0}")]
    HlsEncoderSelection(Box<HlsEncoderSelectionError>),

    /// HLS master playlist write failed after every rendition transcode
    /// succeeded. Boxed so the payload stays inside the crate-wide [`Error`]
    /// size contract.
    #[error("{0}")]
    HlsMasterWrite(Box<HlsMasterWriteError>),

    /// A container-info query was called with an out-of-range index.
    #[error("Container info error: {0}")]
    ContainerInfo(#[from] ContainerInfoError),

    /// A frame-export operation failed.
    #[error("Frame export error: {0}")]
    FrameExport(#[from] crate::core::frame_export::FrameExportError),

    /// Building or opening a video writer failed.
    #[error("Video writer error: {0}")]
    Writer(#[from] crate::core::writer::WriterError),

    /// Pushing a frame into a video writer failed.
    #[error("Video writer push error: {0}")]
    Push(#[from] crate::core::writer::PushError),

    /// Returned when a frame-source input's worker thread failed to start.
    #[error("Frame source thread failed to start")]
    FrameSourceThreadExited,

    /// A packet-sink output failed (see [`PacketSinkError`]).
    #[error("Packet sink error: {0}")]
    PacketSink(#[from] PacketSinkError),

    /// CLI-compat pipelines only: a `-vf` command was lowered onto an input
    /// whose OPENED demuxer does not carry exactly one video stream. The
    /// check runs on the demuxer instance the pipeline actually executes
    /// with (no separate probe opening, no TOCTOU window). The facade maps
    /// this to its public `AmbiguousFilterSource` diagnostic.
    #[cfg(feature = "cli")]
    #[error("the per-output video filter requires exactly one video stream in the input; the opened input has {video_streams}")]
    AmbiguousVideoSource {
        /// Number of video streams carried by the opened input.
        video_streams: usize,
    },

    /// Strict AVOption handling (CLI-compat pipelines): an option the caller
    /// supplied was not consumed by the component it targeted. The default
    /// builder path only WARNS about such leftovers; pipelines built through
    /// the `cli` feature's entry points fail instead, mirroring fftools'
    /// `check_avoptions` abort. Only exists with the `cli` feature — the
    /// feature-off API surface is unchanged.
    #[cfg(feature = "cli")]
    #[error("option '{option}' was not consumed by {site}; CLI-compat strict mode treats leftover AVOptions as errors")]
    UnconsumedCliOption {
        /// Human-readable description of the component that should have
        /// consumed the option (e.g. "the muxer of output 0").
        site: String,
        /// The option key that was left unconsumed.
        option: String,
    },

    // HTTP_INPUT_ERROR_VARIANT
    /// Boxed so [`Error`] stays within the 64-byte layout contract.
    #[cfg(feature = "http-input")]
    #[error("HTTP input error: {0}")]
    HttpInput(Box<crate::http_input::HttpInputError>),
}

// `Error` rides in every hot-path `Result` — the per-frame encoder and filter
// calls return `Result<(), Error>` / `Result<bool, Error>` — so its size is a
// layout contract, not an implementation detail: one oversized payload grows
// every such `Result` crate-wide. 64 bytes is the long-standing layout; keep
// new payloads inside it (use static labels for fixed vocabulary, or box a
// genuinely large variant). A const assertion rather than a #[test] so that
// merely compiling the crate enforces the bound for whichever feature-gated
// variants that build carries — including feature combinations whose tests
// are compiled but never run.
const _: () = assert!(
    std::mem::size_of::<Error>() <= 64,
    "Error grew past its 64-byte layout: shrink the new payload (static labels) or box the variant"
);

impl From<HlsEncoderSelectionError> for Error {
    fn from(err: HlsEncoderSelectionError) -> Self {
        Error::HlsEncoderSelection(Box::new(err))
    }
}

impl From<HlsMasterWriteError> for Error {
    fn from(err: HlsMasterWriteError) -> Self {
        Error::HlsMasterWrite(Box::new(err))
    }
}

/// Failure to select a video encoder for [`crate::recipes::HlsLadder`].
///
/// The historical default (`libx264`) is never silently replaced. Callers
/// either get this typed error or opt in with
/// [`HlsLadder::video_codec_auto`](crate::recipes::HlsLadder::video_codec_auto).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum HlsEncoderSelectionError {
    /// [`HlsLadder`](crate::recipes::HlsLadder) was left on its historical
    /// `libx264` default, and that encoder is not registered in the linked
    /// FFmpeg build. No fallback was selected.
    HistoricalDefaultUnavailable {
        /// Auto-admitted H.264 encoder names that are registered. Presence
        /// here is not runtime-ready and does not prove HLS alignment.
        registered_auto_candidates: Vec<String>,
        /// Other registered H.264 encoders that can only be chosen with
        /// [`.video_codec(...)`](crate::recipes::HlsLadder::video_codec);
        /// this recipe does not manage their alignment.
        registered_explicit_h264_encoders: Vec<String>,
    },
    /// [`HlsLadder::video_codec_auto`](crate::recipes::HlsLadder::video_codec_auto)
    /// ran after `libx264` was unavailable and no candidate could be opened
    /// for every rendition.
    AutoSelectionFailed {
        /// One entry per auto-priority encoder, in selection order.
        attempts: Vec<HlsEncoderAttempt>,
    },
    /// A pinned auto-admitted encoder (`.video_codec("h264_qsv")` and the
    /// other AUTO_PRIORITY names) failed trial-open (rungs are opened one by
    /// one; all sessions are held concurrently to prove the ladder's session
    /// count). No other encoder was tried, and output directories were not
    /// created.
    ExplicitOpenFailed {
        /// Encoder name the caller pinned.
        encoder: String,
        /// Width of the rendition that failed to open.
        width: u32,
        /// Height of the rendition that failed to open.
        height: u32,
        /// Raw FFmpeg `AVERROR` code from `avcodec_open2` (or setup).
        raw_code: i32,
        /// `av_strerror` text for [`raw_code`](Self::ExplicitOpenFailed::raw_code).
        message: String,
    },
}

/// One auto-selection attempt recorded in
/// [`HlsEncoderSelectionError::AutoSelectionFailed`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HlsEncoderAttempt {
    /// Encoder name that was considered.
    pub encoder: String,
    /// Why this encoder was not used.
    pub outcome: HlsEncoderAttemptOutcome,
}

/// Outcome of one auto-selection attempt.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum HlsEncoderAttemptOutcome {
    /// `avcodec_find_encoder_by_name` returned null.
    NotRegistered,
    /// Trial `avcodec_open2` failed for a rendition of this ladder.
    OpenFailed {
        /// Width of the rendition that failed to open.
        width: u32,
        /// Height of the rendition that failed to open.
        height: u32,
        /// Raw FFmpeg `AVERROR` code from `avcodec_open2` (or setup).
        raw_code: i32,
        /// `av_strerror` text for [`raw_code`](Self::OpenFailed::raw_code).
        message: String,
    },
}

fn format_encoder_name_list(names: &[String]) -> String {
    if names.is_empty() {
        "none".to_string()
    } else {
        names.join(", ")
    }
}

impl std::fmt::Display for HlsEncoderSelectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HlsEncoderSelectionError::HistoricalDefaultUnavailable {
                registered_auto_candidates,
                registered_explicit_h264_encoders,
            } => write!(
                f,
                "HlsLadder's historical default encoder 'libx264' is not available in the linked \
                 FFmpeg build. No fallback was selected automatically because encoder quality, \
                 hardware use, and HLS keyframe behavior differ. For an LGPL-only FFmpeg build, \
                 opt in to runtime selection with `.video_codec_auto()`, or pin a registered \
                 encoder with `.video_codec(\"...\")`. Named auto-admitted encoders \
                 (h264_videotoolbox, h264_nvenc, h264_qsv, libopenh264) use this recipe's \
                 HLS-safe option set; other explicit names do not. Registered auto candidates \
                 (runtime readiness and this host's output alignment not verified): {}. \
                 Other registered H.264 encoders (explicit only; HLS alignment unmanaged): {}. \
                 List all registered encoders with `codec::get_encoders()`. \
                 See docs/INSTALL.md#ffmpeg-capability-and-licensing-matrix.",
                format_encoder_name_list(registered_auto_candidates),
                format_encoder_name_list(registered_explicit_h264_encoders),
            ),
            HlsEncoderSelectionError::AutoSelectionFailed { attempts } => {
                let tried = attempts
                    .iter()
                    .map(|attempt| match &attempt.outcome {
                        HlsEncoderAttemptOutcome::NotRegistered => {
                            format!("{} (not registered)", attempt.encoder)
                        }
                        HlsEncoderAttemptOutcome::OpenFailed {
                            width,
                            height,
                            message,
                            ..
                        } => format!(
                            "{} ({}x{} encoder open failed: {message})",
                            attempt.encoder, width, height
                        ),
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "HlsLadder could not select a runtime-ready H.264 encoder after 'libx264' was \
                     unavailable. Tried: {tried}. Enable one of these encoders in the linked \
                     FFmpeg build, choose one explicitly with `.video_codec(\"...\")`, or see \
                     docs/INSTALL.md#ffmpeg-capability-and-licensing-matrix."
                )
            }
            HlsEncoderSelectionError::ExplicitOpenFailed {
                encoder,
                width,
                height,
                message,
                ..
            } => write!(
                f,
                "HlsLadder could not trial-open pinned encoder '{encoder}' for {width}x{height}: \
                 {message}. Output directories were not created. Pin a different encoder with \
                 `.video_codec(\"...\")`, use `.video_codec_auto()`, or see \
                 docs/INSTALL.md#ffmpeg-capability-and-licensing-matrix."
            ),
        }
    }
}

impl std::error::Error for HlsEncoderSelectionError {}

/// Master playlist write failed after every HLS rendition transcode succeeded.
///
/// The media playlists may already be on disk; only the master file (or the
/// BANDWIDTH measurement that feeds it) failed. Display always starts with
/// `transcode succeeded` so operators do not treat this as an encoder miss.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HlsMasterWriteError {
    /// Master playlist file name the recipe tried to write.
    pub master_name: String,
    /// Why the write, or the BANDWIDTH measurement that feeds it, failed.
    pub detail: String,
}

impl std::fmt::Display for HlsMasterWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "transcode succeeded but {} was not written: {}",
            self.master_name, self.detail
        )
    }
}

impl std::error::Error for HlsMasterWriteError {}

/// Builder/open-time validation errors for [`crate::VideoWriter`]. Exported here
/// (not from the crate root) to mirror the existing `OpenInputError` /
/// `OpenOutputError` organization; the root surface stays the settled writer
/// types (the writer itself, its builder, and the push error pair).
pub use crate::core::writer::WriterError;

#[cfg(feature = "http-input")]
impl From<crate::http_input::HttpInputError> for Error {
    fn from(err: crate::http_input::HttpInputError) -> Self {
        Error::HttpInput(Box::new(err))
    }
}

// Hand-written counterpart of the #[from] the sibling variants derive: the
// error type inherits deprecation from the deprecated `opengl` module, so
// the conversion is spelled out where the lint can be silenced.
#[cfg(feature = "opengl")]
#[allow(deprecated)]
impl From<OpenGLFilterError> for Error {
    fn from(err: OpenGLFilterError) -> Self {
        Error::OpenGLFilter(err)
    }
}

/// Errors from the `container_info` queries where the caller asked for an index
/// outside the container's range. These are caller/argument errors — a bad index
/// into an otherwise valid container — kept distinct from an open/probe failure
/// (`OpenInputError` / `FindStreamError`) so retry logic, telemetry, and user
/// messages can tell "you asked for chapter 5 of a 3-chapter file" apart from
/// "the file is corrupt or unreadable". Each variant carries the offending
/// `index` and the container's actual `count`.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ContainerInfoError {
    /// Returned when the requested chapter index exceeds the number of
    /// chapters in the container.
    #[error("chapter index {index} out of range: the container has {count} chapter(s)")]
    ChapterIndexOutOfRange {
        /// The chapter index that was requested.
        index: usize,
        /// Number of chapters the container actually has.
        count: usize,
    },

    /// Returned when the requested stream index exceeds the number of
    /// streams in the container.
    #[error("stream index {index} out of range: the container has {count} stream(s)")]
    StreamIndexOutOfRange {
        /// The stream index that was requested.
        index: usize,
        /// Number of streams the container actually has.
        count: usize,
    },
}

/// Error type for RTMP streaming operations using StreamBuilder
#[cfg(feature = "rtmp")]
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum StreamError {
    /// Returned when a required builder parameter was never set; the
    /// payload is the parameter name (e.g. "address", "stream_key").
    #[error("missing required parameter: {0}")]
    MissingParameter(&'static str),

    /// Returned when the configured input path does not point to an
    /// existing file.
    #[error("input path is not a valid file: {path}")]
    InputNotFound {
        /// The input path that failed validation.
        path: std::path::PathBuf,
    },

    /// An underlying ez-ffmpeg error raised while building or running the
    /// stream.
    #[error("ffmpeg error: {0}")]
    Ffmpeg(#[from] crate::error::Error),
}

impl PartialEq for Error {
    /// Structural equality for payload-less variants only. Variants carrying
    /// an inner error compare unequal even to themselves — use matches! on
    /// the variant when that is what you mean.
    fn eq(&self, other: &Self) -> bool {
        use Error::*;
        match (self, other) {
            (NotStarted, NotStarted)
            | (FilterDescUtf8, FilterDescUtf8)
            | (FilterNameUtf8, FilterNameUtf8)
            | (FilterZeroOutputs, FilterZeroOutputs)
            | (FilterZeroInputs, FilterZeroInputs)
            | (ParseInteger, ParseInteger)
            | (FrameFilterDstFinished, FrameFilterDstFinished)
            | (FrameFilterFrameDuplicateFailed, FrameFilterFrameDuplicateFailed)
            | (FrameFilterThreadExited, FrameFilterThreadExited)
            | (FrameSourceThreadExited, FrameSourceThreadExited)
            | (EOF, EOF)
            | (Exit, Exit)
            | (Bug, Bug) => true,
            #[cfg(feature = "rtmp")]
            (RtmpCreateStream, RtmpCreateStream)
            | (RtmpRegistrationQueueFull, RtmpRegistrationQueueFull)
            | (RtmpThreadExited, RtmpThreadExited)
            | (RtmpStreamClosed, RtmpStreamClosed)
            | (RtmpServerAlreadyStarted, RtmpServerAlreadyStarted) => true,
            _ => false,
        }
    }
}

// No Eq impl: variants carrying payloads are not equal to themselves, so
// the relation is not reflexive and claiming Eq would be a lie.

/// Errors from the demuxer stage while reading packets from an input.
/// Variants carrying a [`DemuxingError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DemuxingOperationError {
    /// Returned when reading the next packet from the input fails
    /// (`av_read_frame`).
    #[error("while reading frame: {0}")]
    ReadFrameError(DemuxingError),

    /// Returned when creating an additional reference to a demuxed packet
    /// fails (`av_packet_ref`).
    #[error("while referencing packet: {0}")]
    PacketRefError(DemuxingError),

    /// Returned when seeking in the input fails (`avformat_seek_file`).
    #[error("while seeking file: {0}")]
    SeekFileError(DemuxingError),

    /// Returned when spawning the demuxer thread fails, so demuxing never
    /// started.
    #[error("Thread exited")]
    ThreadExited,
}

/// Errors from the decoder stage while turning packets into frames.
/// Variants carrying a [`DecodingError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DecodingOperationError {
    /// Returned when creating a new reference to a decoded frame fails
    /// (`av_frame_ref`).
    #[error("during frame reference creation: {0}")]
    FrameRefError(DecodingError),

    /// Returned when copying frame metadata fails (`av_frame_copy_props`).
    #[error("during frame properties copy: {0}")]
    FrameCopyPropsError(DecodingError),

    /// Returned when decoding a subtitle packet fails
    /// (`avcodec_decode_subtitle2`).
    #[error("during subtitle decoding: {0}")]
    DecodeSubtitleError(DecodingError),

    /// Returned when copying a decoded subtitle for delivery fails.
    #[error("during subtitle copy: {0}")]
    CopySubtitleError(DecodingError),

    /// Returned when submitting a packet to the decoder fails
    /// (`avcodec_send_packet`).
    #[error("during packet submission to decoder: {0}")]
    SendPacketError(DecodingError),

    /// Returned when receiving a decoded frame from the decoder fails
    /// (`avcodec_receive_frame`).
    #[error("during frame reception from decoder: {0}")]
    ReceiveFrameError(DecodingError),

    /// Returned when allocating a frame during decoding fails.
    #[error("during frame allocation: {0}")]
    FrameAllocationError(DecodingError),

    /// Returned when allocating a packet during decoding fails.
    #[error("during packet allocation: {0}")]
    PacketAllocationError(DecodingError),

    /// Returned when allocating an `AVSubtitle` during decoding fails.
    #[error("during AVSubtitle allocation: {0}")]
    SubtitleAllocationError(DecodingError),

    /// Returned when the decoder emits a frame flagged as corrupt and
    /// corrupt frames are treated as errors.
    #[error("corrupt decoded frame")]
    CorruptFrame,

    /// Returned when the ratio of decode errors to decoded frames exceeds
    /// the maximum allowed rate.
    #[error("decode error rate exceeded the maximum allowed")]
    ErrorRateExceeded,

    /// Returned when downloading a hardware-decoded frame to system memory
    /// fails (`av_hwframe_transfer_data`).
    #[error("during retrieve data on hw: {0}")]
    HWRetrieveDataError(DecodingError),

    /// Returned when applying codec cropping metadata to a decoded frame
    /// fails.
    #[error("during cropping: {0}")]
    CroppingError(DecodingError),
}

/// Errors from opening and configuring a decoder.
/// Variants carrying an [`OpenDecoderError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenDecoderOperationError {
    /// Returned when allocating the decoder context fails
    /// (`avcodec_alloc_context3`).
    #[error("during context allocation: {0}")]
    ContextAllocationError(OpenDecoderError),

    /// Returned when applying the stream's codec parameters to the decoder
    /// context fails (`avcodec_parameters_to_context`).
    #[error("while applying parameters to context: {0}")]
    ParameterApplicationError(OpenDecoderError),

    /// Returned when opening the decoder fails (`avcodec_open2`).
    #[error("while opening decoder: {0}")]
    DecoderOpenError(OpenDecoderError),

    /// Returned when copying the audio channel layout into the decoder
    /// context fails.
    #[error("while copying channel layout: {0}")]
    ChannelLayoutCopyError(OpenDecoderError),

    /// Returned when setting up hardware acceleration for the decoder
    /// fails.
    #[error("while Hw setup: {0}")]
    HwSetupError(OpenDecoderError),

    /// Returned when the configured decoder name is invalid.
    #[error("Invalid decoder name")]
    InvalidName,

    /// Returned when spawning the decoder thread fails, so the decoder
    /// never opened.
    #[error("Thread exited")]
    ThreadExited,
}

/// Errors from running frames through a configured filtergraph.
/// Variants carrying a [`FilterGraphError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FilterGraphOperationError {
    /// Returned when requesting the next frame from the graph fails
    /// (`avfilter_graph_request_oldest`).
    #[error("during requesting oldest frame: {0}")]
    RequestOldestError(FilterGraphError),

    /// Returned when processing frames through the filtergraph fails.
    #[error("during process frames: {0}")]
    ProcessFramesError(FilterGraphError),

    /// Returned when sending frames into the filtergraph fails.
    #[error("during send frames: {0}")]
    SendFramesError(FilterGraphError),

    /// Returned when copying an audio channel layout while configuring the
    /// graph fails.
    #[error("during copying channel layout: {0}")]
    ChannelLayoutCopyError(FilterGraphError),

    /// Returned when pushing a frame into a graph input fails
    /// (`av_buffersrc_add_frame`).
    #[error("during buffer source add frame: {0}")]
    BufferSourceAddFrameError(FilterGraphError),

    /// Returned when closing a graph input at end of stream fails
    /// (`av_buffersrc_close`).
    #[error("during closing buffer source: {0}")]
    BufferSourceCloseError(FilterGraphError),

    /// Returned when replacing a frame's buffer reference fails
    /// (`av_buffer_replace`).
    #[error("during replace buffer: {0}")]
    BufferReplaceoseError(FilterGraphError),

    /// Returned when cloning frame side data for the graph fails.
    #[error("during cloning frame side data: {0}")]
    FrameSideDataCloneError(FilterGraphError),

    /// Returned when parsing or configuring the filtergraph description
    /// fails.
    #[error("during parse: {0}")]
    ParseError(FilterGraphParseError),

    /// Returned when a frame entering the graph carries invalid or
    /// corrupted data.
    #[error("The data in the frame is invalid or corrupted")]
    InvalidData,

    /// Returned before the graph is configured when one input has buffered
    /// frames past the admission limit while another input has not yet
    /// delivered its first frame; fields are the input label, the buffered
    /// frame count, and the estimated retained memory in bytes.
    #[error(
        "graph input '{0}' already holds {1} buffered frames and admitting the next \
         one would raise the best-effort retained-memory estimate to ~{2} bytes, \
         while another input has not yet delivered its first frame, so the filter \
         graph cannot be configured; check that every graph input actually produces \
         data (or produces it within the buffering window)"
    )]
    PreConfigQueueOverflow(String, usize, usize),

    // Only constructed on the FFmpeg 8+ buffersrc side-data clone path.
    /// Returned when a frame's combined side-data metadata is too large to
    /// deep-copy into the buffersrc parameters; fields are the input label
    /// and the estimated size in bytes.
    #[cfg_attr(not(ffmpeg_8_0), allow(dead_code))]
    #[error(
        "graph input '{0}' would deep-copy an estimated {1} bytes of side-data \
         metadata into the buffersrc parameters, exceeding the side-data clone \
         estimate threshold; the frame's combined side-data metadata (across its \
         global and downmix entries) is pathologically large"
    )]
    OversizedSideDataClone(String, usize),

    /// Returned when spawning the filtergraph thread fails, so the graph
    /// never ran.
    #[error("Thread exited")]
    ThreadExited,
}

/// Errors from the encoder stage while turning frames into packets.
/// Variants carrying an [`EncodingError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EncodingOperationError {
    /// Returned when submitting a frame to the encoder fails
    /// (`avcodec_send_frame`).
    #[error("during frame submission: {0}")]
    SendFrameError(EncodingError),

    /// Returned when receiving an encoded packet from the encoder fails
    /// (`avcodec_receive_packet`).
    #[error("during packet retrieval: {0}")]
    ReceivePacketError(EncodingError),

    /// Returned when re-chunking buffered audio samples into encoder-sized
    /// frames fails.
    #[error("during audio frame receive: {0}")]
    ReceiveAudioError(EncodingError),

    /// Returned when a subtitle packet reaches the encoder without a
    /// presentation timestamp.
    #[error(": Subtitle packets must have a pts")]
    SubtitleNotPts,

    /// Returned when an encoded packet cannot be delivered because the
    /// muxer has already finished.
    #[error(": Muxer already finished")]
    MuxerFinished,

    /// An output stream buffered more packets before the muxer started than the
    /// pre-mux queue admits (fftools `AVERROR_BUFFER_TOO_SMALL`, "Too many
    /// packets buffered for output stream"). Unlike `MuxerFinished` this is a
    /// hard failure — never a silent truncation — so it must reach the
    /// scheduler error, not the graceful stop path.
    #[error(": too many packets buffered for an output stream before the muxer started; raise Output::set_max_muxing_queue_size / Output::set_muxing_queue_data_threshold, or check that every mapped output stream receives data")]
    MuxQueueFull,

    /// Returned when encoding a subtitle fails (see
    /// [`EncodeSubtitleError`]).
    #[error("Encode subtitle error: {0}")]
    EncodeSubtitle(#[from] EncodeSubtitleError),

    /// Returned when allocating a packet for encoder output fails.
    #[error(": {0}")]
    AllocPacket(AllocPacketError),
}

/// Errors from the muxer stage while writing the output container.
/// Variants carrying a [`MuxingError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MuxingOperationError {
    /// Returned when writing the container header fails (see
    /// [`WriteHeaderError`]).
    #[error("during write header: {0}")]
    WriteHeader(WriteHeaderError),

    /// Returned when initializing a bitstream filter chain for an output
    /// stream fails; fields are the chain description and the underlying
    /// error.
    #[error("while initializing bitstream filter chain '{0}': {1}")]
    BitstreamFilterInit(String, MuxingError),

    /// Returned when writing an interleaved packet to the container fails
    /// (`av_interleaved_write_frame`).
    #[error("during interleaved write: {0}")]
    InterleavedWriteError(MuxingError),

    /// Returned when writing the container trailer fails
    /// (`av_write_trailer`).
    #[error("during trailer write: {0}")]
    TrailerWriteError(MuxingError),

    /// Returned when closing the output I/O context fails.
    #[error("during closing IO: {0}")]
    IOCloseError(MuxingError),

    /// Returned when spawning the muxer (or mux-init) thread fails, so
    /// muxing never started.
    #[error("Thread exited")]
    ThreadExited,
}

/// Errors specific to packet-sink outputs (`Output::new_by_packet_sink`).
///
/// The strict tier fails fast: configuration problems surface from `build()`
/// or from the job **before any sink callback runs**; per-packet violations
/// stop the job with the offending packet never delivered. `Clone` is
/// deliberate — for delivery-path errors the same value is recorded as the
/// job error and handed to the sink's `on_delivery_error` callback.
/// [`JobFailed`](Self::JobFailed) is the exception: it is synthesized for
/// that callback only, while first-error-wins may leave the job result owned
/// by a sibling worker's error.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum PacketSinkError {
    /// A builder option the packet sink cannot honor was set: either a
    /// container-only option (no container is written, so it could never
    /// take effect) or a pipeline feature outside the strict tier's
    /// delivery contract (filters, bitstream filters, subtitle codecs —
    /// rejected as policy, not for lack of a container).
    #[error("{0} is not supported on packet-sink outputs")]
    UnsupportedOption(&'static str),

    /// A stream was configured as `copy`; packet sinks require encoded
    /// streams.
    #[error("stream copy is not supported on packet-sink outputs (strict tier requires encoded streams)")]
    StreamCopyUnsupported,

    /// The output mapped a stream the strict tier cannot deliver (non-H.264
    /// video, non-AAC audio, or a non-audio/video kind).
    #[error("{kind} streams are not supported on packet-sink outputs (strict tier)")]
    UnsupportedStream {
        /// Label describing the rejected stream kind (e.g. "non-H.264
        /// video", "non-AAC audio").
        kind: &'static str,
    },

    /// The configured encoder is outside the strict-tier v1 whitelist.
    #[error("encoder '{encoder}' is not on the strict-tier whitelist for {kind} (v1 accepts: {allowed})")]
    EncoderNotWhitelisted {
        /// Media kind of the stream ("video" or "audio").
        kind: &'static str,
        /// The encoder name that was configured.
        encoder: String,
        /// The encoder names the strict tier accepts for this kind.
        allowed: &'static str,
    },

    /// An admitted video encoder was given an explicit B-frame option
    /// outside the strict-tier verified scope (`bf=0` / `max_b_frames=0`).
    ///
    /// Unset keys are not this error: they keep the wrapper default. This
    /// crate does not rewrite the caller's options.
    #[error(
        "encoder '{encoder}' rejected explicit B-frames on a packet-sink output \
         (every present bf / max_b_frames key must be integer 0 or removed; \
         unset keeps the wrapper default)"
    )]
    BFramesUnsupported {
        /// The encoder name that was configured.
        encoder: String,
    },

    /// No stream was mapped to the packet-sink output.
    #[error("packet-sink output has no streams")]
    NoStreams,

    /// An encoder finalized without the out-of-band codec configuration the
    /// strict tier delivers via `on_stream_info`.
    #[error("output stream {stream_index}: encoder produced no extradata; the strict tier requires codec configuration (avcC / AudioSpecificConfig) before the first callback")]
    MissingExtradata {
        /// Index of the offending output stream.
        stream_index: usize,
    },

    /// The encoder's codec configuration failed strict-tier validation.
    #[error("output stream {stream_index}: invalid codec configuration: {reason}")]
    InvalidExtradata {
        /// Index of the offending output stream.
        stream_index: usize,
        /// Why the codec configuration failed validation.
        reason: String,
    },

    /// A stream's time base is not a positive rational.
    #[error("output stream {stream_index}: invalid time base {num}/{den} (positive numerator and denominator required)")]
    InvalidTimeBase {
        /// Index of the offending output stream.
        stream_index: usize,
        /// Numerator of the rejected time base.
        num: i32,
        /// Denominator of the rejected time base.
        den: i32,
    },

    /// A packet was stamped in a time base other than its stream's.
    #[error("output stream {stream_index}: packet time base {packet_num}/{packet_den} differs from the stream time base {stream_num}/{stream_den}")]
    PacketTimeBaseMismatch {
        /// Index of the offending output stream.
        stream_index: usize,
        /// Numerator of the packet's time base.
        packet_num: i32,
        /// Denominator of the packet's time base.
        packet_den: i32,
        /// Numerator of the stream's time base.
        stream_num: i32,
        /// Denominator of the stream's time base.
        stream_den: i32,
    },

    /// A packet carries no pts or dts (`AV_NOPTS_VALUE`).
    #[error("output stream {stream_index}: packet carries no {which} (strict tier rejects AV_NOPTS_VALUE)")]
    MissingTimestamp {
        /// Index of the offending output stream.
        stream_index: usize,
        /// Which timestamp is missing: "pts" or "dts".
        which: &'static str,
    },

    /// A packet's dts did not strictly increase within its stream.
    #[error(
        "output stream {stream_index}: non-monotonic dts (previous {prev}, current {current})"
    )]
    NonMonotonicDts {
        /// Index of the offending output stream.
        stream_index: usize,
        /// dts of the previous packet, in stream time-base units.
        prev: i64,
        /// dts of the offending packet, in stream time-base units.
        current: i64,
    },

    /// A packet's pts collided with a still-pending pts on the same stream.
    #[error("output stream {stream_index}: duplicate pts {pts}")]
    DuplicatePts {
        /// Index of the offending output stream.
        stream_index: usize,
        /// The duplicated pts value, in stream time-base units.
        pts: i64,
    },

    /// A packet's pts is earlier than its dts.
    #[error("output stream {stream_index}: pts {pts} is earlier than dts {dts}")]
    PtsBeforeDts {
        /// Index of the offending output stream.
        stream_index: usize,
        /// The packet's pts, in stream time-base units.
        pts: i64,
        /// The packet's dts, in stream time-base units.
        dts: i64,
    },

    /// Rescaling a timestamp onto the shared time origin overflowed.
    #[error(
        "output stream {stream_index}: timestamp overflow while applying the shared time origin"
    )]
    TimestampOverflow {
        /// Index of the offending output stream.
        stream_index: usize,
    },

    /// A packet has no positive duration and none could be derived from the
    /// stream configuration (frame rate / codec frame size).
    #[error("output stream {stream_index}: packet duration is absent and cannot be derived (strict tier requires a positive duration)")]
    MissingDuration {
        /// Index of the offending output stream.
        stream_index: usize,
    },

    /// The packet payload failed bitstream validation.
    #[error("output stream {stream_index}: malformed packet payload: {reason}")]
    MalformedPacket {
        /// Index of the offending output stream.
        stream_index: usize,
        /// Description of the bitstream validation failure.
        reason: String,
    },

    /// Internal sequencing violation: a packet surfaced outside the delivery
    /// phase.
    #[error("output stream {stream_index}: packet processed outside the delivery phase (internal sequencing violation)")]
    PhaseViolation {
        /// Index of the offending output stream.
        stream_index: usize,
    },

    /// The stream configuration changed after `on_stream_info` delivered it.
    #[error("output stream {stream_index}: mid-stream configuration change ({what}); the strict tier requires an immutable stream configuration")]
    ConfigChange {
        /// Index of the offending output stream.
        stream_index: usize,
        /// Description of the configuration change that was detected.
        what: String,
    },

    /// An H.264 access unit carried in-band SPS/PPS parameter sets.
    #[error("output stream {stream_index}: in-band SPS/PPS parameter sets are not supported in the strict tier (WebCodecs avc requires out-of-band configuration)")]
    InBandParameterSets {
        /// Index of the offending output stream.
        stream_index: usize,
    },

    /// The sink's `on_stream_info` callback rejected the configuration.
    #[error("on_stream_info callback rejected the stream configuration: {error}")]
    StreamInfoCallbackFailed {
        /// The error the callback returned.
        #[source]
        error: crate::core::packet_sink::PacketCallbackError,
    },

    /// The sink's `on_packet` callback returned an error.
    #[error("on_packet callback failed on output stream {stream_index}: {error}")]
    PacketCallbackFailed {
        /// Index of the offending output stream.
        stream_index: usize,
        /// The error the callback returned.
        #[source]
        error: crate::core::packet_sink::PacketCallbackError,
    },

    /// The channel adapter's receiver was dropped, cancelling delivery and
    /// the job.
    #[error("the packet-sink channel receiver was dropped; delivery cancelled")]
    ChannelDisconnected,

    /// The job failed outside this sink's delivery path; handed to
    /// `on_delivery_error` only, while `wait()` keeps the original error.
    #[error(
        "the job failed outside this packet sink; delivery may have been truncated: {message}"
    )]
    JobFailed {
        /// Display rendering of the error that actually failed the job.
        message: String,
    },
}

/// Errors from opening and configuring an encoder.
/// Variants carrying an [`OpenEncoderError`] embed the mapped FFmpeg error.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenEncoderOperationError {
    /// Returned when cloning frame side data into the encoder context
    /// fails.
    #[error("during frame side data cloning: {0}")]
    FrameSideDataCloneError(OpenEncoderError),

    /// Returned when copying the audio channel layout into the encoder
    /// context fails.
    #[error("during channel layout copying: {0}")]
    ChannelLayoutCopyError(OpenEncoderError),

    /// Returned when opening the encoder fails (`avcodec_open2`).
    #[error("during codec opening: {0}")]
    CodecOpenError(OpenEncoderError),

    /// Returned when exporting encoder parameters to the output stream
    /// fails (`avcodec_parameters_from_context`).
    #[error("while setting codec parameters: {0}")]
    CodecParametersError(OpenEncoderError),

    /// Returned when the format of the frame to encode is unknown.
    #[error(": unknown format of the frame")]
    UnknownFrameFormat,

    /// Returned when configuring subtitle encoding parameters fails.
    #[error("while setting subtitle: {0}")]
    SettingSubtitleError(OpenEncoderError),

    /// Returned when setting up hardware acceleration for the encoder
    /// fails.
    #[error("while Hw setup: {0}")]
    HwSetupError(OpenEncoderError),

    /// Returned when allocating the encoder context fails
    /// (`avcodec_alloc_context3`).
    #[error("during context allocation: {0}")]
    ContextAllocationError(OpenEncoderError),

    /// Returned when the frame stream ends (EOF or upstream disconnect)
    /// before the encoder received any frame, so the encoder was never
    /// opened.
    #[error(": no frames were received before EOF; encoder never opened")]
    NoFramesReceived,

    /// Returned when the stream's media type cannot be encoded (not video,
    /// audio, or subtitle).
    #[error(": unsupported media type for encoding")]
    UnsupportedMediaType,

    /// Returned when spawning the encoder thread fails, so the encoder
    /// never started.
    #[error("Thread exited")]
    ThreadExited,
}

/// Errors from converting URL or path strings for FFmpeg.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum UrlError {
    /// Returned when the string contains an interior NUL byte, which C
    /// strings cannot represent; the payload is the byte position.
    #[error("Null byte found in string at position {0}")]
    NullByteError(usize),
}

impl From<NulError> for Error {
    fn from(err: NulError) -> Self {
        Error::Url(UrlError::NullByteError(err.nul_position()))
    }
}

/// Errors from opening an input file, stream, device, or custom input
/// source. Most variants are mapped from the FFmpeg error code returned by
/// `avformat_open_input`.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenInputError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// The file, URL, or device does not exist (`AVERROR(ENOENT)`).
    #[error("File or stream not found")]
    NotFound,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while opening the file or stream")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    /// The functionality or input format is not supported by the linked
    /// FFmpeg build (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported input format")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted to access the file or stream")]
    OperationNotPermitted,

    /// The file or stream contains invalid or corrupted data
    /// (`AVERROR_INVALIDDATA`).
    #[error("The data in the file or stream is invalid or corrupted")]
    InvalidData,

    /// The connection timed out (`AVERROR(ETIMEDOUT)`).
    #[error("The connection timed out while trying to open the stream")]
    Timeout,

    /// A builder option carried an invalid value (e.g. a non-positive
    /// `set_framerate`, a non-finite `set_ts_scale`, an out-of-range
    /// `set_io_buffer_size`). Setters store values as given and defer
    /// validation to open time, so a bad value surfaces here instead of
    /// panicking in the setter.
    #[error("Invalid input option: {0}")]
    InvalidOption(String),

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),

    /// Returned when the input has no usable source: neither a URL nor a
    /// custom read callback was configured.
    #[error("Invalid source provided")]
    InvalidSource,

    /// Returned when the explicitly requested input format name is unknown
    /// to FFmpeg; the payload is the requested name.
    #[error("Invalid source format:{0}")]
    InvalidFormat(String),

    /// Returned when the input requires seeking but the custom input source
    /// provides no seek callback.
    #[error("No seek callback is provided")]
    SeekFunctionMissing,

    /// `Input::from("https://…")` failed because the linked FFmpeg has no
    /// HTTPS protocol. This does **not** route the URL through rustls.
    #[cfg(not(feature = "http-input"))]
    #[error(
        "FFmpeg HTTPS input is unavailable. Enable the ez-ffmpeg \"http-input\" feature \
         and use HttpInput, or link an FFmpeg build with an HTTPS/TLS backend \
         (GnuTLS or OpenSSL)."
    )]
    HttpsProtocolUnavailable,

    /// Same failure with the feature already enabled: still not hijacked.
    #[cfg(feature = "http-input")]
    #[error(
        "FFmpeg HTTPS input is unavailable. The \"http-input\" feature is enabled; use \
         HttpInput::builder(url), or link an FFmpeg build with an HTTPS/TLS backend."
    )]
    HttpsProtocolUnavailable,
}

impl From<i32> for OpenInputError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => OpenInputError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => OpenInputError::InvalidArgument,
            AVERROR_NOT_FOUND => OpenInputError::NotFound,
            AVERROR_IO_ERROR => OpenInputError::IOError,
            AVERROR_PIPE_ERROR => OpenInputError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => OpenInputError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => OpenInputError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => OpenInputError::OperationNotPermitted,
            AVERROR_INVALIDDATA => OpenInputError::InvalidData,
            AVERROR_TIMEOUT => OpenInputError::Timeout,
            _ => OpenInputError::UnknownError(err_code),
        }
    }
}

const AVERROR_OUT_OF_MEMORY: i32 = AVERROR(ENOMEM);
const AVERROR_INVALID_ARGUMENT: i32 = AVERROR(EINVAL);
const AVERROR_NOT_FOUND: i32 = AVERROR(ENOENT);
const AVERROR_IO_ERROR: i32 = AVERROR(EIO);
const AVERROR_PIPE_ERROR: i32 = AVERROR(EPIPE);
const AVERROR_BAD_FILE_DESCRIPTOR: i32 = AVERROR(EBADF);
const AVERROR_NOT_IMPLEMENTED: i32 = AVERROR(ENOSYS);
const AVERROR_OPERATION_NOT_PERMITTED: i32 = AVERROR(EPERM);
const AVERROR_PERMISSION_DENIED: i32 = AVERROR(EACCES);
const AVERROR_TIMEOUT: i32 = AVERROR(ETIMEDOUT);
const AVERROR_NOT_SOCKET: i32 = AVERROR(ENOTSOCK);
const AVERROR_AGAIN: i32 = AVERROR(EAGAIN);

/// Errors from probing stream information after an input is opened
/// (`avformat_find_stream_info`).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FindStreamError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// Reached end of file before stream information could be determined
    /// (`AVERROR_EOF`).
    #[error("Reached end of file while looking for stream info")]
    EndOfFile,

    /// The operation timed out (`AVERROR(ETIMEDOUT)`).
    #[error("Timeout occurred while reading stream info")]
    Timeout,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while reading stream info")]
    IOError,

    /// The stream contains invalid or corrupted data
    /// (`AVERROR_INVALIDDATA`).
    #[error("The data in the stream is invalid or corrupted")]
    InvalidData,

    /// The functionality or stream format is not supported by the linked
    /// FFmpeg build (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported stream format")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted to access the file or stream")]
    OperationNotPermitted,

    /// Returned when the input contains no streams, or no stream of the
    /// requested kind.
    #[error("No Stream found")]
    NoStreamFound,

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for FindStreamError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => FindStreamError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => FindStreamError::InvalidArgument,
            AVERROR_EOF => FindStreamError::EndOfFile,
            AVERROR_TIMEOUT => FindStreamError::Timeout,
            AVERROR_IO_ERROR => FindStreamError::IOError,
            AVERROR_INVALIDDATA => FindStreamError::InvalidData,
            AVERROR_NOT_IMPLEMENTED => FindStreamError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => FindStreamError::OperationNotPermitted,
            _ => FindStreamError::UnknownError(err_code),
        }
    }
}

/// Errors from parsing a filtergraph description and wiring its inputs and
/// outputs.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FilterGraphParseError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// End of file was reached during parsing (`AVERROR_EOF`).
    #[error("End of file reached during parsing")]
    EndOfFile,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during parsing")]
    IOError,

    /// Invalid data was encountered during parsing
    /// (`AVERROR_INVALIDDATA`).
    #[error("Invalid data encountered during parsing")]
    InvalidData,

    /// The functionality or filter is not supported by the linked FFmpeg
    /// build (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported filter format")]
    NotImplemented,

    /// Permission was denied — e.g. by a filter that opens files, such as
    /// `movie=` (`AVERROR(EACCES)`).
    #[error("Permission denied during filter graph parsing")]
    PermissionDenied,

    /// A socket operation was attempted on a non-socket by a filter
    /// touching network resources (`AVERROR(ENOTSOCK)`).
    #[error("Socket operation on non-socket during filter graph parsing")]
    NotSocket,

    /// A filter option named in the description does not exist
    /// (`AVERROR_OPTION_NOT_FOUND`).
    #[error("Option not found during filter graph configuration")]
    OptionNotFound,

    /// Returned when a stream reference in the filtergraph description
    /// names an input file index that does not exist; fields are the index
    /// and the description.
    #[error("Invalid file index {0} in filtergraph description {1}")]
    InvalidFileIndexInFg(usize, String),

    /// Returned when an output URL references an input file index that does
    /// not exist; fields are the index and the URL.
    #[error("Invalid file index {0} in output url: {1}")]
    InvalidFileIndexInOutput(usize, String),

    /// Returned when a stream specifier in the filtergraph description is
    /// malformed; the payload is the offending text.
    #[error("Invalid filter specifier {0}")]
    InvalidFilterSpecifier(String),

    /// Returned when a filtergraph output pad is not connected to any
    /// output; fields are the filter name, the pad index, and its link
    /// label.
    #[error("Filter '{0}' has output {1} ({2}) unconnected")]
    OutputUnconnected(String, usize, String),

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("An unknown error occurred. ret: {0}")]
    UnknownError(i32),
}

impl From<i32> for FilterGraphParseError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => FilterGraphParseError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => FilterGraphParseError::InvalidArgument,
            AVERROR_EOF => FilterGraphParseError::EndOfFile,
            AVERROR_IO_ERROR => FilterGraphParseError::IOError,
            AVERROR_INVALIDDATA => FilterGraphParseError::InvalidData,
            AVERROR_NOT_IMPLEMENTED => FilterGraphParseError::NotImplemented,
            AVERROR_OPTION_NOT_FOUND => FilterGraphParseError::OptionNotFound,
            // EACCES/ENOTSOCK reach here from filters that touch files or
            // sockets (e.g. `movie=`); map them to the variants this enum
            // already declares instead of degrading to UnknownError.
            AVERROR_PERMISSION_DENIED => FilterGraphParseError::PermissionDenied,
            AVERROR_NOT_SOCKET => FilterGraphParseError::NotSocket,
            _ => FilterGraphParseError::UnknownError(err_code),
        }
    }
}

/// Errors from allocating an output format context
/// (`avformat_alloc_output_context2`).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AllocOutputContextError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// The file or stream does not exist (`AVERROR(ENOENT)`).
    #[error("File or stream not found")]
    NotFound,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while allocating the output context")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    /// The functionality or output format is not supported by the linked
    /// FFmpeg build (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported output format")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted to allocate the output context")]
    OperationNotPermitted,

    /// Permission was denied (`AVERROR(EACCES)`).
    #[error("Permission denied while allocating the output context")]
    PermissionDenied,

    /// The operation timed out (`AVERROR(ETIMEDOUT)`).
    #[error("The connection timed out while trying to allocate the output context")]
    Timeout,

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for AllocOutputContextError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => AllocOutputContextError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => AllocOutputContextError::InvalidArgument,
            AVERROR_NOT_FOUND => AllocOutputContextError::NotFound,
            AVERROR_IO_ERROR => AllocOutputContextError::IOError,
            AVERROR_PIPE_ERROR => AllocOutputContextError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => AllocOutputContextError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => AllocOutputContextError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => AllocOutputContextError::OperationNotPermitted,
            AVERROR_PERMISSION_DENIED => AllocOutputContextError::PermissionDenied,
            AVERROR_TIMEOUT => AllocOutputContextError::Timeout,
            _ => AllocOutputContextError::UnknownError(err_code),
        }
    }
}

/// Errors from opening and configuring an output: resolving formats and
/// encoders, mapping streams, validating options, and opening the target
/// for writing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenOutputError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// The file or stream does not exist (`AVERROR(ENOENT)`).
    #[error("File or stream not found")]
    NotFound,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while opening the file or stream")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    /// The functionality or output format is not supported by the linked
    /// FFmpeg build (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported output format")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted to open the file or stream")]
    OperationNotPermitted,

    /// Permission was denied (`AVERROR(EACCES)`).
    #[error("Permission denied while opening the file or stream")]
    PermissionDenied,

    /// The operation timed out (`AVERROR(ETIMEDOUT)`).
    #[error("The connection timed out while trying to open the file or stream")]
    Timeout,

    /// No encoder was found for the requested codec
    /// (`AVERROR_ENCODER_NOT_FOUND`).
    #[error("encoder not found")]
    EncoderNotFound,

    /// A named encoder could not be opened because the linked FFmpeg build
    /// does not provide it — either it was compiled without that encoder
    /// (e.g. no `--enable-libx264`) or the name is not a known encoder at all.
    /// Unlike the bare [`EncoderNotFound`](Self::EncoderNotFound) errno
    /// mapping, this names the encoder so the fix is actionable. `name` is the
    /// encoder the caller requested, or the codec the output format guessed
    /// when none was set explicitly.
    #[error(
        "encoder '{name}' is not available in the linked FFmpeg build — link \
         an FFmpeg build that provides it (for example one configured with \
         --enable-libx264 for libx264), or select a different encoder via \
         Output::set_video_codec / set_audio_codec / set_subtitle_codec \
         (list what the build provides with codec::get_encoders)"
    )]
    EncoderUnavailable {
        /// The requested encoder name, or the format's guessed default
        /// codec when none was set explicitly.
        name: String,
    },

    /// Returned when a stream map specifier matches no streams; the payload
    /// is the specifier.
    #[error("Stream map '{0}' matches no streams;")]
    MatchesNoStreams(String),

    /// A stream map combined stream copy with a per-map re-encoding
    /// request ([`StreamMap::codec`] / [`StreamMap::codec_opt`]): copied
    /// packets never pass through an encoder, so a per-map codec or
    /// per-map codec options could never take effect. Raised at `build()`
    /// instead of silently ignoring the request (the FFmpeg CLI merely
    /// warns about such unused options).
    ///
    /// [`StreamMap::codec`]: crate::core::context::output::StreamMap::codec
    /// [`StreamMap::codec_opt`]: crate::core::context::output::StreamMap::codec_opt
    #[error(
        "stream map '{spec}' requests stream copy together with {what}; \
         stream copy and per-map re-encoding settings are mutually exclusive"
    )]
    StreamMapCopyConflict {
        /// The offending stream map specifier.
        spec: String,
        /// The per-map re-encoding setting that conflicts with copy.
        what: &'static str,
    },

    /// Returned when an output references an invalid filtergraph link
    /// label; the payload is the label.
    #[error("Invalid label {0}")]
    InvalidLabel(String),

    /// Returned when the output ends up with no streams at all.
    #[error("not contain any stream")]
    NotContainStream,

    /// Returned when the format of the frame feeding an output stream is
    /// unknown, so encoder parameters cannot be derived from it.
    #[error("unknown format of the frame")]
    UnknownFrameFormat,

    /// Returned when an input URL references a file index that does not
    /// exist; fields are the index and the URL.
    #[error("Invalid file index {0} in input url: {1}")]
    InvalidFileIndexInIntput(usize, String),

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),

    /// Returned when the output has no usable destination: neither a URL
    /// nor a custom write callback was configured.
    #[error("Invalid sink provided")]
    InvalidSink,

    /// Returned when the output format requires seeking but the custom
    /// output sink provides no seek callback.
    #[error("No seek callback is provided")]
    SeekFunctionMissing,

    /// Returned when the requested output format name is unknown to FFmpeg;
    /// the payload is the requested name.
    #[error("Format '{0}' is unsupported")]
    FormatUnsupported(String),

    /// Returned when a pixel format name is not recognized; the payload is
    /// the name.
    #[error("Unknown pixel format: '{0}'")]
    UnknownPixelFormat(String),

    /// Returned when a sample format name is not recognized; the payload is
    /// the name.
    #[error("Unknown sample format: '{0}'")]
    UnknownSampleFormat(String),

    /// A builder option carried an invalid value (e.g. a malformed
    /// `set_force_key_frames` spec, an out-of-range `set_io_buffer_size`).
    /// Setters store values as given and defer validation to open time, so
    /// a bad value surfaces here instead of panicking in the setter or
    /// forcing a `Result` into the middle of a builder chain.
    #[error("Invalid output option: {0}")]
    InvalidOption(String),

    /// Returned when reading an attachment file fails; the payload is the
    /// path, with the underlying I/O error as the source.
    #[error("Failed to read attachment file '{0}'")]
    AttachmentRead(String, #[source] io::Error),

    /// Returned when an attachment file is empty; the payload is the path.
    #[error("Attachment file '{0}' is empty")]
    AttachmentEmpty(String),

    /// Returned when an attachment file exceeds the size limit; fields are
    /// the path, its size in bytes, and the limit in bytes.
    #[error("Attachment file '{0}' is too large ({1} bytes, limit {2} bytes)")]
    AttachmentTooLarge(String, u64, u64),

    /// Returned when an attachment was configured with an empty mimetype;
    /// the payload is the file path.
    #[error("Attachment mimetype must not be empty (file '{0}')")]
    AttachmentEmptyMimetype(String),

    /// A per-output simple filter ([`Output::set_video_filter`] or
    /// [`Output::set_audio_filter`]) was combined with stream copy for the
    /// same output stream — either `set_video_codec("copy")` /
    /// `set_audio_codec("copy")` or a copy stream map covering that stream.
    /// Mirrors the FFmpeg CLI error for `-vf`/`-af` + `-c copy`
    /// ("Filtering and streamcopy cannot be used together",
    /// ffmpeg_mux_init.c streamcopy_init).
    ///
    /// [`Output::set_video_filter`]: crate::core::context::output::Output::set_video_filter
    /// [`Output::set_audio_filter`]: crate::core::context::output::Output::set_audio_filter
    #[error(
        "Filtergraph '{0}' was specified, but codec copy was selected for the \
         matching output stream. Filtering and streamcopy cannot be used together"
    )]
    FilterWithStreamCopy(String),

    /// A per-output simple filter ([`Output::set_video_filter`] or
    /// [`Output::set_audio_filter`]) was set on an output whose matching
    /// stream is fed by a context-level filtergraph
    /// (`FfmpegContextBuilder::filter_desc`). Mirrors the FFmpeg CLI error for
    /// `-vf`/`-af` + `-filter_complex` on the same stream (ffmpeg_mux_init.c
    /// ost_get_filters: "Simple and complex filtering cannot be used together
    /// for the same stream").
    ///
    /// [`Output::set_video_filter`]: crate::core::context::output::Output::set_video_filter
    /// [`Output::set_audio_filter`]: crate::core::context::output::Output::set_audio_filter
    #[error(
        "Filtergraph '{0}' was specified for a stream fed from a \
         context-level filtergraph. Simple and complex filtering cannot be \
         used together for the same stream"
    )]
    SimpleAndComplexFilter(String),

    /// A per-output simple filtergraph must be one connected linear chain:
    /// exactly one video input pad, one video output pad, a single connected
    /// component, and a directed path from the input to the output (fftools
    /// fg_create_simple's contract plus the topology rules a simple graph
    /// implies — a disconnected or unreachable description would encode
    /// unrelated frames or hang instead of filtering the stream). The path
    /// requirement is structural: the input pad must be wired into the flow
    /// that feeds the output pad, while a filter that may discard it at
    /// runtime (`streamselect` whose applied `map` selects another input —
    /// rewritable mid-stream via `sendcmd`) is accepted, matching the CLI.
    /// `reason` names the violated rule. Descriptions that split, merge or
    /// source streams belong in the context-level `filter_desc`.
    #[error(
        "Simple filtergraph '{desc}' is not a single connected chain: {reason}; \
         use FfmpegContextBuilder::filter_desc for complex graphs"
    )]
    SimpleFilterInvalidShape {
        /// The offending filtergraph description, as configured.
        desc: String,
        /// The topology rule the description violates.
        reason: String,
    },

    /// A configured [`Output::set_video_filter`] chain that no re-encoded
    /// video stream ended up consuming: the output has no video stream at all
    /// (audio-only input, `disable_video()`, or maps that matched no video
    /// stream). The ffmpeg CLI silently ignores `-vf` in that situation; the
    /// crate refuses instead of dropping configuration on the floor.
    ///
    /// [`Output::set_video_filter`]: crate::core::context::output::Output::set_video_filter
    #[error(
        "video filter '{0}' was configured, but the output ended up with no \
         re-encoded video stream to run it (audio-only input, disable_video(), \
         or maps matching no video stream); remove the filter or map a video \
         stream"
    )]
    VideoFilterUnused(String),

    /// A configured [`Output::set_audio_filter`] chain that no re-encoded
    /// audio stream ended up consuming: the output has no audio stream at all
    /// (video-only input, `disable_audio()`, or maps that matched no audio
    /// stream). The ffmpeg CLI silently ignores `-af` in that situation; the
    /// crate refuses instead of dropping configuration on the floor.
    ///
    /// [`Output::set_audio_filter`]: crate::core::context::output::Output::set_audio_filter
    #[error(
        "audio filter '{0}' was configured, but the output ended up with no \
         re-encoded audio stream to run it (video-only input, disable_audio(), \
         or maps matching no audio stream); remove the filter or map an audio \
         stream"
    )]
    AudioFilterUnused(String),

    /// A per-output simple filtergraph's pads must match the stream's media
    /// type (fftools fg_create_simple: "Filtergraph has a %s output, cannot
    /// connect it to %s output stream") — e.g. an audio chain like `anull`
    /// cannot be attached as a video filter.
    ///
    /// The media-type labels are static (`"video"`, `"audio"`, ... — the
    /// strings fftools prints), which keeps this variant inside `Error`'s
    /// 64-byte layout; three owned `String`s would grow every hot-path
    /// `Result` in the crate.
    #[error(
        "Simple filtergraph '{desc}' has a {found} pad, cannot connect it to \
         the {expected} stream of this output"
    )]
    SimpleFilterMediaTypeMismatch {
        /// The offending filtergraph description, as configured.
        desc: String,
        /// The media type of the mismatched pad.
        found: &'static str,
        /// The media type the output stream requires.
        expected: &'static str,
    },
}

impl From<i32> for OpenOutputError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => OpenOutputError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => OpenOutputError::InvalidArgument,
            AVERROR_NOT_FOUND => OpenOutputError::NotFound,
            AVERROR_IO_ERROR => OpenOutputError::IOError,
            AVERROR_PIPE_ERROR => OpenOutputError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => OpenOutputError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => OpenOutputError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => OpenOutputError::OperationNotPermitted,
            AVERROR_PERMISSION_DENIED => OpenOutputError::PermissionDenied,
            AVERROR_TIMEOUT => OpenOutputError::Timeout,
            AVERROR_ENCODER_NOT_FOUND => OpenOutputError::EncoderNotFound,
            _ => OpenOutputError::UnknownError(err_code),
        }
    }
}

/// Errors from enumerating capture devices (cameras, microphones,
/// screens).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FindDevicesError {
    /// Returned on macOS when the `AVCaptureDevice` class is not available.
    #[error("AVCaptureDevice class not found in macOS")]
    AVCaptureDeviceNotFound,

    /// Returned when device enumeration for the requested media type is not
    /// supported; the payload is the raw `AVMediaType` value.
    #[error("current media_type({0}) is not supported")]
    MediaTypeSupported(i32),
    /// Returned when device enumeration is not supported on the current
    /// operating system.
    #[error("current OS is not supported")]
    OsNotSupported,
    /// Returned when a device description could not be converted to a UTF-8
    /// string.
    #[error("device_description can not to string")]
    UTF8Error,

    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,
    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,
    /// The device or stream does not exist (`AVERROR(ENOENT)`).
    #[error("Device or stream not found")]
    NotFound,
    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while accessing the device or stream")]
    IOError,
    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted for this device or stream")]
    OperationNotPermitted,
    /// Permission was denied (`AVERROR(EACCES)`).
    #[error("Permission denied while accessing the device or stream")]
    PermissionDenied,
    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("This functionality is not implemented")]
    NotImplemented,
    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor")]
    BadFileDescriptor,
    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for FindDevicesError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => FindDevicesError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => FindDevicesError::InvalidArgument,
            AVERROR_NOT_FOUND => FindDevicesError::NotFound,
            AVERROR_IO_ERROR => FindDevicesError::IOError,
            AVERROR_OPERATION_NOT_PERMITTED => FindDevicesError::OperationNotPermitted,
            AVERROR_PERMISSION_DENIED => FindDevicesError::PermissionDenied,
            AVERROR_NOT_IMPLEMENTED => FindDevicesError::NotImplemented,
            AVERROR_BAD_FILE_DESCRIPTOR => FindDevicesError::BadFileDescriptor,
            _ => FindDevicesError::UnknownError(err_code),
        }
    }
}

/// Errors from writing the output container header
/// (`avformat_write_header`), carried by
/// [`MuxingOperationError::WriteHeader`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WriteHeaderError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// The file or stream does not exist (`AVERROR(ENOENT)`).
    #[error("File or stream not found")]
    NotFound,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while writing the header")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    /// The functionality or output format is not supported by the linked
    /// FFmpeg build (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported output format")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted to write the header")]
    OperationNotPermitted,

    /// Permission was denied (`AVERROR(EACCES)`).
    #[error("Permission denied while writing the header")]
    PermissionDenied,

    /// The operation timed out (`AVERROR(ETIMEDOUT)`).
    #[error("The connection timed out while trying to write the header")]
    Timeout,

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for WriteHeaderError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => WriteHeaderError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => WriteHeaderError::InvalidArgument,
            AVERROR_NOT_FOUND => WriteHeaderError::NotFound,
            AVERROR_IO_ERROR => WriteHeaderError::IOError,
            AVERROR_PIPE_ERROR => WriteHeaderError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => WriteHeaderError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => WriteHeaderError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => WriteHeaderError::OperationNotPermitted,
            AVERROR_PERMISSION_DENIED => WriteHeaderError::PermissionDenied,
            AVERROR_TIMEOUT => WriteHeaderError::Timeout,
            _ => WriteHeaderError::UnknownError(err_code),
        }
    }
}

/// Errors from encoding a subtitle (`avcodec_encode_subtitle`).
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EncodeSubtitleError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error while encoding subtitle")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided for subtitle encoding")]
    InvalidArgument,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted while encoding subtitle")]
    OperationNotPermitted,

    /// Subtitle encoding is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("The encoding functionality is not implemented or unsupported")]
    NotImplemented,

    /// The encoder is temporarily unable to accept input
    /// (`AVERROR(EAGAIN)`); retry later.
    #[error("Encoder temporarily unable to process, please retry")]
    TryAgain,

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("Subtitle encoding failed with unknown error. ret: {0}")]
    UnknownError(i32),
}

impl From<i32> for EncodeSubtitleError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => EncodeSubtitleError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => EncodeSubtitleError::InvalidArgument,
            AVERROR_OPERATION_NOT_PERMITTED => EncodeSubtitleError::OperationNotPermitted,
            AVERROR_NOT_IMPLEMENTED => EncodeSubtitleError::NotImplemented,
            AVERROR_AGAIN => EncodeSubtitleError::TryAgain,
            _ => EncodeSubtitleError::UnknownError(err_code),
        }
    }
}

/// Errors from allocating an `AVPacket`.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AllocPacketError {
    /// Packet allocation failed (`av_packet_alloc` returned no packet).
    #[error("Memory allocation error while alloc packet")]
    OutOfMemory,
}

/// Errors from allocating an `AVFrame`.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AllocFrameError {
    /// Frame allocation failed (`av_frame_alloc` returned no frame).
    #[error("Memory allocation error while alloc frame")]
    OutOfMemory,
}

/// Errors from [`make_frame_writable`], the safe wrapper over FFmpeg's
/// `av_frame_make_writable`: ensuring exclusive ownership of a frame's data
/// buffers may allocate new buffers and copy into them, and that underlying
/// call can fail. Common AVERROR codes map to named variants; anything else
/// carries the raw code.
///
/// [`make_frame_writable`]: crate::util::ffmpeg_utils::make_frame_writable
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FrameWritableError {
    /// Allocating or copying the frame's data buffers failed
    /// (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error while copying frame data")]
    OutOfMemory,

    /// FFmpeg rejected the frame as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for FrameWritableError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => FrameWritableError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => FrameWritableError::InvalidArgument,
            _ => FrameWritableError::UnknownError(err_code),
        }
    }
}

/// FFmpeg-level errors during muxing, carried by [`MuxingOperationError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MuxingError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during muxing")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Broken pipe during muxing")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor encountered")]
    BadFileDescriptor,

    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted")]
    OperationNotPermitted,

    /// The resource is temporarily unavailable (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable")]
    TryAgain,

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for MuxingError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => MuxingError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => MuxingError::InvalidArgument,
            AVERROR_IO_ERROR => MuxingError::IOError,
            AVERROR_PIPE_ERROR => MuxingError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => MuxingError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => MuxingError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => MuxingError::OperationNotPermitted,
            AVERROR_AGAIN => MuxingError::TryAgain,
            _ => MuxingError::UnknownError(err_code),
        }
    }
}

/// FFmpeg-level errors while opening an encoder, carried by
/// [`OpenEncoderOperationError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenEncoderError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error occurred during encoder initialization")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided to encoder")]
    InvalidArgument,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred while opening encoder")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Broken pipe encountered during encoder initialization")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor used in encoder")]
    BadFileDescriptor,

    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("Encoder functionality not implemented or unsupported")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted while configuring encoder")]
    OperationNotPermitted,

    /// The resource is temporarily unavailable (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable during encoder setup")]
    TryAgain,

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("An unknown error occurred in encoder setup. ret:{0}")]
    UnknownError(i32),
}

impl From<i32> for OpenEncoderError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => OpenEncoderError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => OpenEncoderError::InvalidArgument,
            AVERROR_IO_ERROR => OpenEncoderError::IOError,
            AVERROR_PIPE_ERROR => OpenEncoderError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => OpenEncoderError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => OpenEncoderError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => OpenEncoderError::OperationNotPermitted,
            AVERROR_AGAIN => OpenEncoderError::TryAgain,
            _ => OpenEncoderError::UnknownError(err_code),
        }
    }
}

/// FFmpeg-level errors during encoding, carried by
/// [`EncodingOperationError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EncodingError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error during encoding")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided to encoder")]
    InvalidArgument,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during encoding")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Broken pipe encountered during encoding")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor encountered during encoding")]
    BadFileDescriptor,

    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported encoding feature")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted for encoder")]
    OperationNotPermitted,

    /// The encoder is temporarily unable to accept or produce data
    /// (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable, try again later")]
    TryAgain,

    /// The encoder reached end of stream; no more packets will be produced
    /// (`AVERROR_EOF`).
    #[error("End of stream reached or no more frames to encode")]
    EndOfStream,

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("An unknown error occurred during encoding. ret: {0}")]
    UnknownError(i32),
}

impl From<i32> for EncodingError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => EncodingError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => EncodingError::InvalidArgument,
            AVERROR_IO_ERROR => EncodingError::IOError,
            AVERROR_PIPE_ERROR => EncodingError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => EncodingError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => EncodingError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => EncodingError::OperationNotPermitted,
            AVERROR_AGAIN => EncodingError::TryAgain,
            AVERROR_EOF => EncodingError::EndOfStream,
            _ => EncodingError::UnknownError(err_code),
        }
    }
}

/// FFmpeg-level errors during filtergraph processing, carried by
/// [`FilterGraphOperationError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FilterGraphError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error during filter graph processing")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided to filter graph processing")]
    InvalidArgument,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during filter graph processing")]
    IOError,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Broken pipe during filter graph processing")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor encountered during filter graph processing")]
    BadFileDescriptor,

    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported during filter graph processing")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted during filter graph processing")]
    OperationNotPermitted,

    /// The graph is temporarily unable to accept or produce data
    /// (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable during filter graph processing")]
    TryAgain,

    /// The filtergraph reached end of stream (`AVERROR_EOF`).
    #[error("EOF")]
    EOF,

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("An unknown error occurred during filter graph processing. ret:{0}")]
    UnknownError(i32),
}

impl From<i32> for FilterGraphError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => FilterGraphError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => FilterGraphError::InvalidArgument,
            AVERROR_IO_ERROR => FilterGraphError::IOError,
            AVERROR_PIPE_ERROR => FilterGraphError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => FilterGraphError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => FilterGraphError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => FilterGraphError::OperationNotPermitted,
            AVERROR_AGAIN => FilterGraphError::TryAgain,
            AVERROR_EOF => FilterGraphError::EOF,
            _ => FilterGraphError::UnknownError(err_code),
        }
    }
}

/// FFmpeg-level errors while opening a decoder, carried by
/// [`OpenDecoderOperationError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenDecoderError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error during decoder initialization")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided during decoder initialization")]
    InvalidArgument,

    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported during decoder initialization")]
    NotImplemented,

    /// The resource is temporarily unavailable (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable during decoder initialization")]
    TryAgain,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during decoder initialization")]
    IOError,

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("An unknown error occurred during decoder initialization: {0}")]
    UnknownError(i32),
}

impl From<i32> for OpenDecoderError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => OpenDecoderError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => OpenDecoderError::InvalidArgument,
            AVERROR_NOT_IMPLEMENTED => OpenDecoderError::NotImplemented,
            AVERROR_AGAIN => OpenDecoderError::TryAgain,
            AVERROR_IO_ERROR => OpenDecoderError::IOError,
            _ => OpenDecoderError::UnknownError(err_code),
        }
    }
}

/// FFmpeg-level errors during decoding, carried by
/// [`DecodingOperationError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DecodingError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during decoding")]
    IOError,

    /// The operation timed out (`AVERROR(ETIMEDOUT)`).
    #[error("Timeout occurred during decoding")]
    Timeout,

    /// The stream or data connection was broken (`AVERROR(EPIPE)`).
    #[error("Broken pipe encountered during decoding")]
    PipeError,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor encountered during decoding")]
    BadFileDescriptor,

    /// The functionality or format is not supported by the linked FFmpeg
    /// build (`AVERROR(ENOSYS)`).
    #[error("Unsupported functionality or format encountered")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted")]
    OperationNotPermitted,

    /// The decoder is temporarily unable to accept or produce data
    /// (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable")]
    TryAgain,

    /// Any other failure; the payload is the raw FFmpeg error code.
    #[error("An unknown decoding error occurred. ret:{0}")]
    UnknownError(i32),
}

impl From<i32> for DecodingError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => DecodingError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => DecodingError::InvalidArgument,
            AVERROR_IO_ERROR => DecodingError::IOError,
            AVERROR_TIMEOUT => DecodingError::Timeout,
            AVERROR_PIPE_ERROR => DecodingError::PipeError,
            AVERROR_BAD_FILE_DESCRIPTOR => DecodingError::BadFileDescriptor,
            AVERROR_NOT_IMPLEMENTED => DecodingError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => DecodingError::OperationNotPermitted,
            AVERROR_AGAIN => DecodingError::TryAgain,
            _ => DecodingError::UnknownError(err_code),
        }
    }
}

/// Errors from resolving a decoder.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DecoderError {
    /// Returned when a decoder requested by name is not provided by the
    /// linked FFmpeg build; the payload is the requested name.
    #[error("decoder '{0}' not found")]
    NotFound(String),
}

/// FFmpeg-level errors during demuxing, carried by
/// [`DemuxingOperationError`] and [`PacketScannerError`].
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DemuxingError {
    /// Memory allocation failed (`AVERROR(ENOMEM)`).
    #[error("Memory allocation error")]
    OutOfMemory,

    /// FFmpeg rejected an argument as invalid (`AVERROR(EINVAL)`).
    #[error("Invalid argument provided")]
    InvalidArgument,

    /// A low-level I/O error occurred (`AVERROR(EIO)`).
    #[error("I/O error occurred during demuxing")]
    IOError,

    /// End of file was reached during demuxing (`AVERROR_EOF`).
    #[error("End of file reached during demuxing")]
    EndOfFile,

    /// The resource is temporarily unavailable (`AVERROR(EAGAIN)`).
    #[error("Resource temporarily unavailable")]
    TryAgain,

    /// The functionality is not supported by the linked FFmpeg build
    /// (`AVERROR(ENOSYS)`).
    #[error("Functionality not implemented or unsupported")]
    NotImplemented,

    /// The operation was not permitted (`AVERROR(EPERM)`).
    #[error("Operation not permitted")]
    OperationNotPermitted,

    /// An invalid file descriptor was used (`AVERROR(EBADF)`).
    #[error("Bad file descriptor encountered")]
    BadFileDescriptor,

    /// The input contains invalid or corrupted data
    /// (`AVERROR_INVALIDDATA`).
    #[error("Invalid data found when processing input")]
    InvalidData,

    /// Any other failure; the payload is the raw FFmpeg error code
    /// (rendered with `av_err2str` in the message).
    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),
}

impl From<i32> for DemuxingError {
    fn from(err_code: i32) -> Self {
        match err_code {
            AVERROR_OUT_OF_MEMORY => DemuxingError::OutOfMemory,
            AVERROR_INVALID_ARGUMENT => DemuxingError::InvalidArgument,
            AVERROR_IO_ERROR => DemuxingError::IOError,
            AVERROR_EOF => DemuxingError::EndOfFile,
            AVERROR_AGAIN => DemuxingError::TryAgain,
            AVERROR_NOT_IMPLEMENTED => DemuxingError::NotImplemented,
            AVERROR_OPERATION_NOT_PERMITTED => DemuxingError::OperationNotPermitted,
            AVERROR_BAD_FILE_DESCRIPTOR => DemuxingError::BadFileDescriptor,
            AVERROR_INVALIDDATA => DemuxingError::InvalidData,
            _ => DemuxingError::UnknownError(err_code),
        }
    }
}

/// Errors that can occur during packet scanning operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum PacketScannerError {
    /// Failed to seek to the requested timestamp.
    #[error("while seeking: {0}")]
    SeekError(DemuxingError),

    /// Failed to read the next packet from the demuxer.
    #[error("while reading packet: {0}")]
    ReadError(DemuxingError),
}

#[cfg(test)]
mod tests {
    // Regression: FrameSourceThreadExited is payload-less, but the manual
    // PartialEq whitelist omitted it, so the variant compared unequal to
    // itself — breaking the impl's documented "structural equality for
    // payload-less variants" contract.
    #[test]
    fn frame_source_thread_exited_equals_itself() {
        use super::Error;
        assert_eq!(
            Error::FrameSourceThreadExited,
            Error::FrameSourceThreadExited
        );
        assert_ne!(Error::FrameSourceThreadExited, Error::NotStarted);
    }

    // Regression: FilterGraphParseError declares PermissionDenied and NotSocket,
    // but its From<i32> once omitted them, so an EACCES/ENOTSOCK filtergraph
    // error degraded to UnknownError and the two declared variants were
    // unreachable. Map the codes to the variants the enum already exposes.
    #[test]
    fn filter_graph_parse_error_maps_permission_and_socket_codes() {
        use super::{FilterGraphParseError, AVERROR_NOT_SOCKET, AVERROR_PERMISSION_DENIED};
        assert!(matches!(
            FilterGraphParseError::from(AVERROR_PERMISSION_DENIED),
            FilterGraphParseError::PermissionDenied
        ));
        assert!(matches!(
            FilterGraphParseError::from(AVERROR_NOT_SOCKET),
            FilterGraphParseError::NotSocket
        ));
    }

    // make_frame_writable's failure is typed like every other AVERROR-coded
    // failure in this file: common codes map to named variants, the rest keep
    // the raw code. Pin the mapping and the user-facing Display string.
    #[test]
    fn frame_writable_error_maps_codes_and_pins_display() {
        use super::{Error, FrameWritableError, AVERROR_INVALID_ARGUMENT, AVERROR_OUT_OF_MEMORY};
        assert!(matches!(
            FrameWritableError::from(AVERROR_OUT_OF_MEMORY),
            FrameWritableError::OutOfMemory
        ));
        assert!(matches!(
            FrameWritableError::from(AVERROR_INVALID_ARGUMENT),
            FrameWritableError::InvalidArgument
        ));
        assert!(matches!(
            FrameWritableError::from(-99),
            FrameWritableError::UnknownError(-99)
        ));
        let err = Error::from(FrameWritableError::from(AVERROR_OUT_OF_MEMORY));
        assert_eq!(
            err.to_string(),
            "Frame writable error: Memory allocation error while copying frame data"
        );
    }

    // The deprecated OpenGL filter's constructor failures are typed like the
    // wgpu successor's: they carry OpenGLFilterError and convert into
    // Error::OpenGLFilter. Pin the user-facing Display strings.
    #[cfg(feature = "opengl")]
    #[test]
    fn opengl_filter_error_pins_display() {
        use super::{Error, OpenGLFilterError};
        let err = Error::from(OpenGLFilterError::InvalidOption(
            "fragment shader must declare 'in vec2 TexCoord;'".to_string(),
        ));
        assert_eq!(
            err.to_string(),
            "OpenGL filter error: invalid OpenGL filter option: \
             fragment shader must declare 'in vec2 TexCoord;'"
        );
        let err = Error::from(OpenGLFilterError::ContextCreation(
            "Failed to create Surfman connection".to_string(),
        ));
        assert_eq!(
            err.to_string(),
            "OpenGL filter error: OpenGL context creation failed: \
             Failed to create Surfman connection"
        );
    }

    #[test]
    fn hls_encoder_selection_error_is_boxed_under_size_cap() {
        use super::{Error, HlsEncoderSelectionError};
        assert!(std::mem::size_of::<Error>() <= 64);
        let err = Error::from(HlsEncoderSelectionError::HistoricalDefaultUnavailable {
            registered_auto_candidates: Vec::new(),
            registered_explicit_h264_encoders: Vec::new(),
        });
        match &err {
            Error::HlsEncoderSelection(_) => {}
            other => panic!("expected boxed HLS error, got {other}"),
        }
        let text = err.to_string();
        assert!(text.contains("libx264"));
        assert!(text.contains("LGPL"));
        assert!(text.contains(".video_codec_auto()"));
        assert!(text.contains(": none."));
    }

    #[test]
    fn hls_explicit_open_failed_pins_display() {
        use super::{Error, HlsEncoderSelectionError};
        let err = Error::from(HlsEncoderSelectionError::ExplicitOpenFailed {
            encoder: "h264_qsv".into(),
            width: 1920,
            height: 1080,
            raw_code: -1,
            message: "device busy".into(),
        });
        let text = err.to_string();
        assert!(text.contains("pinned encoder 'h264_qsv'"), "{text}");
        assert!(text.contains("1920x1080"), "{text}");
        assert!(text.contains("device busy"), "{text}");
        assert!(
            text.contains("Output directories were not created"),
            "{text}"
        );
    }

    #[test]
    fn hls_master_write_error_is_boxed_under_size_cap() {
        use super::{Error, HlsMasterWriteError};
        assert!(std::mem::size_of::<Error>() <= 64);
        let err = Error::from(HlsMasterWriteError {
            master_name: "custom.m3u8".into(),
            detail: "failed to write master playlist".into(),
        });
        match &err {
            Error::HlsMasterWrite(_) => {}
            other => panic!("expected boxed HLS master-write error, got {other}"),
        }
        let text = err.to_string();
        assert!(text.contains("transcode succeeded"), "{text}");
        assert!(text.contains("custom.m3u8"), "{text}");
        assert!(text.contains("failed to write master playlist"), "{text}");
    }

    #[test]
    fn analysis_frame_error_is_boxed_under_size_cap() {
        use super::Error;
        assert!(std::mem::size_of::<Error>() <= 64);
        let err = Error::AnalysisFrame("interlaced".into());
        match &err {
            Error::AnalysisFrame(_) => {}
            other => panic!("expected boxed analysis-frame error, got {other}"),
        }
        assert_eq!(err.to_string(), "analysis frame error: interlaced");
    }

    #[test]
    fn packet_sink_b_frames_unsupported_pins_display() {
        use super::{Error, PacketSinkError};
        let err = Error::from(PacketSinkError::BFramesUnsupported {
            encoder: "h264_videotoolbox".into(),
        });
        assert_eq!(
            err.to_string(),
            "Packet sink error: encoder 'h264_videotoolbox' rejected explicit \
             B-frames on a packet-sink output (every present bf / max_b_frames \
             key must be integer 0 or removed; unset keeps the wrapper default)"
        );
    }
}
