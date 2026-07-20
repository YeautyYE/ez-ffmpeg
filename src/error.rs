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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Scheduler is not started")]
    NotStarted,

    #[error("URL error: {0}")]
    Url(#[from] UrlError),

    #[error("Open input stream error: {0}")]
    OpenInputStream(#[from] OpenInputError),

    #[error("Find stream info error: {0}")]
    FindStream(#[from] FindStreamError),

    #[error("Decoder error: {0}")]
    Decoder(#[from] DecoderError),

    #[error("Filter graph parse error: {0}")]
    FilterGraphParse(#[from] FilterGraphParseError),

    #[error("Filter description converted to utf8 string error")]
    FilterDescUtf8,

    #[error("Filter name converted to utf8 string error")]
    FilterNameUtf8,

    #[error("A filtergraph has zero outputs, this is not supported")]
    FilterZeroOutputs,

    #[error("A filtergraph has zero inputs, this is not supported")]
    FilterZeroInputs,

    #[error("Input is not a valid number")]
    ParseInteger,

    #[error("Alloc output context error: {0}")]
    AllocOutputContext(#[from] AllocOutputContextError),

    #[error("Open output error: {0}")]
    OpenOutput(#[from] OpenOutputError),

    #[error("Output file '{0}' is the same as an input file")]
    FileSameAsInput(String),

    #[error("Find devices error: {0}")]
    FindDevices(#[from] FindDevicesError),

    #[error("Alloc frame error: {0}")]
    AllocFrame(#[from] AllocFrameError),

    #[error("Alloc packet error: {0}")]
    AllocPacket(#[from] AllocPacketError),

    #[error("Frame writable error: {0}")]
    FrameWritable(#[from] FrameWritableError),

    // ---- Muxing ----
    #[error("Muxing operation failed {0}")]
    Muxing(#[from] MuxingOperationError),

    // ---- Open Encoder ----
    #[error("Open encoder operation failed {0}")]
    OpenEncoder(#[from] OpenEncoderOperationError),

    // ---- Encoding ----
    #[error("Encoding operation failed {0}")]
    Encoding(#[from] EncodingOperationError),

    // ---- FilterGraph ----
    #[error("Filter graph operation failed {0}")]
    FilterGraph(#[from] FilterGraphOperationError),

    // ---- Open Decoder ----
    #[error("Open decoder operation failed {0}")]
    OpenDecoder(#[from] OpenDecoderOperationError),

    // ---- Decoding ----
    #[error("Decoding operation failed {0}")]
    Decoding(#[from] DecodingOperationError),

    // ---- Demuxing ----
    #[error("Demuxing operation failed {0}")]
    Demuxing(#[from] DemuxingOperationError),

    // ---- Packet Scanner ----
    #[error("Packet scanner error: {0}")]
    PacketScanner(#[from] PacketScannerError),

    // ---- Frame Filter ----
    #[error("Frame filter init failed: {0}")]
    FrameFilterInit(Box<dyn std::error::Error + Send + Sync>),

    #[error("Frame filter process failed: {0}")]
    FrameFilterProcess(Box<dyn std::error::Error + Send + Sync>),

    #[error("Frame filter request failed: {0}")]
    FrameFilterRequest(Box<dyn std::error::Error + Send + Sync>),

    #[error("No {0} stream of the type:{1} were found while build frame pipeline")]
    FrameFilterTypeNoMatched(String, String),

    #[error("{0} stream:{1} of the type:{2} were mismatched while build frame pipeline")]
    FrameFilterStreamTypeNoMatched(String, usize, String),

    #[error("Frame filter pipeline destination already finished")]
    FrameFilterDstFinished,

    #[error("Frame filter pipeline failed to duplicate a frame for an additional destination")]
    FrameFilterFrameDuplicateFailed,

    #[error("Frame filter pipeline thread exited")]
    FrameFilterThreadExited,

    #[error("Worker thread '{0}' panicked; output may be incomplete")]
    WorkerPanicked(String),

    #[cfg(feature = "rtmp")]
    #[error("Rtmp stream already exists with key: {0}")]
    RtmpStreamAlreadyExists(String),

    #[cfg(feature = "rtmp")]
    #[error("Rtmp create stream failed. Check whether the server is stopped.")]
    RtmpCreateStream,

    #[cfg(feature = "rtmp")]
    #[error("Rtmp server thread exited")]
    RtmpThreadExited,

    #[cfg(feature = "rtmp")]
    #[error("Rtmp stream closed: the server is no longer consuming this stream")]
    RtmpStreamClosed,

    #[cfg(feature = "subtitle")]
    #[error("Subtitle error: {0}")]
    Subtitle(#[from] crate::subtitle::SubtitleError),

    #[cfg(feature = "wgpu")]
    #[error("Wgpu filter error: {0}")]
    WgpuFilter(#[from] crate::wgpu_filter::WgpuFilterError),

    // The allow covers the deprecation that OpenGLFilterError inherits from
    // the deprecated `opengl` module; the variant must still carry the type.
    // From is hand-written below the enum (a derived #[from] would re-name
    // the type in generated code that no #[allow] on the variant reaches).
    #[cfg(feature = "opengl")]
    #[allow(deprecated)]
    #[error("OpenGL filter error: {0}")]
    OpenGLFilter(#[source] OpenGLFilterError),

    #[error("IO error:{0}")]
    IO(#[from] io::Error),

    #[error("EOF")]
    EOF,
    #[error("Exit")]
    Exit,
    #[error("Bug")]
    Bug,

    #[error("Invalid recipe argument: {0}")]
    InvalidRecipeArg(String),

    #[error("Container info error: {0}")]
    ContainerInfo(#[from] ContainerInfoError),

    #[error("Frame export error: {0}")]
    FrameExport(#[from] crate::core::frame_export::FrameExportError),

    #[error("Video writer error: {0}")]
    Writer(#[from] crate::core::writer::WriterError),

    #[error("Video writer push error: {0}")]
    Push(#[from] crate::core::writer::PushError),

    #[error("Frame source thread failed to start")]
    FrameSourceThreadExited,

    #[error("Packet sink error: {0}")]
    PacketSink(#[from] PacketSinkError),

    /// Strict AVOption handling (CLI-compat pipelines): an option the caller
    /// supplied was not consumed by the component it targeted. The default
    /// builder path only WARNS about such leftovers; pipelines built through
    /// the `cli` feature's entry points fail instead, mirroring fftools'
    /// `check_avoptions` abort.
    #[error("option '{option}' was not consumed by {site}; CLI-compat strict mode treats leftover AVOptions as errors")]
    UnconsumedCliOption { site: String, option: String },
}

/// Builder/open-time validation errors for [`crate::VideoWriter`]. Exported here
/// (not from the crate root) to mirror the existing `OpenInputError` /
/// `OpenOutputError` organization; the root surface stays the three settled
/// writer types.
pub use crate::core::writer::WriterError;

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
    #[error("chapter index {index} out of range: the container has {count} chapter(s)")]
    ChapterIndexOutOfRange { index: usize, count: usize },

    #[error("stream index {index} out of range: the container has {count} stream(s)")]
    StreamIndexOutOfRange { index: usize, count: usize },
}

/// Error type for RTMP streaming operations using StreamBuilder
#[cfg(feature = "rtmp")]
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum StreamError {
    #[error("missing required parameter: {0}")]
    MissingParameter(&'static str),

    #[error("input path is not a valid file: {path}")]
    InputNotFound { path: std::path::PathBuf },

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
            | (RtmpThreadExited, RtmpThreadExited)
            | (RtmpStreamClosed, RtmpStreamClosed) => true,
            _ => false,
        }
    }
}

// No Eq impl: variants carrying payloads are not equal to themselves, so
// the relation is not reflexive and claiming Eq would be a lie.

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DemuxingOperationError {
    #[error("while reading frame: {0}")]
    ReadFrameError(DemuxingError),

    #[error("while referencing packet: {0}")]
    PacketRefError(DemuxingError),

    #[error("while seeking file: {0}")]
    SeekFileError(DemuxingError),

    #[error("Thread exited")]
    ThreadExited,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DecodingOperationError {
    #[error("during frame reference creation: {0}")]
    FrameRefError(DecodingError),

    #[error("during frame properties copy: {0}")]
    FrameCopyPropsError(DecodingError),

    #[error("during subtitle decoding: {0}")]
    DecodeSubtitleError(DecodingError),

    #[error("during subtitle copy: {0}")]
    CopySubtitleError(DecodingError),

    #[error("during packet submission to decoder: {0}")]
    SendPacketError(DecodingError),

    #[error("during frame reception from decoder: {0}")]
    ReceiveFrameError(DecodingError),

    #[error("during frame allocation: {0}")]
    FrameAllocationError(DecodingError),

    #[error("during packet allocation: {0}")]
    PacketAllocationError(DecodingError),

    #[error("during AVSubtitle allocation: {0}")]
    SubtitleAllocationError(DecodingError),

    #[error("corrupt decoded frame")]
    CorruptFrame,

    #[error("decode error rate exceeded the maximum allowed")]
    ErrorRateExceeded,

    #[error("during retrieve data on hw: {0}")]
    HWRetrieveDataError(DecodingError),

    #[error("during cropping: {0}")]
    CroppingError(DecodingError),
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenDecoderOperationError {
    #[error("during context allocation: {0}")]
    ContextAllocationError(OpenDecoderError),

    #[error("while applying parameters to context: {0}")]
    ParameterApplicationError(OpenDecoderError),

    #[error("while opening decoder: {0}")]
    DecoderOpenError(OpenDecoderError),

    #[error("while copying channel layout: {0}")]
    ChannelLayoutCopyError(OpenDecoderError),

    #[error("while Hw setup: {0}")]
    HwSetupError(OpenDecoderError),

    #[error("Invalid decoder name")]
    InvalidName,

    #[error("Thread exited")]
    ThreadExited,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FilterGraphOperationError {
    #[error("during requesting oldest frame: {0}")]
    RequestOldestError(FilterGraphError),

    #[error("during process frames: {0}")]
    ProcessFramesError(FilterGraphError),

    #[error("during send frames: {0}")]
    SendFramesError(FilterGraphError),

    #[error("during copying channel layout: {0}")]
    ChannelLayoutCopyError(FilterGraphError),

    #[error("during buffer source add frame: {0}")]
    BufferSourceAddFrameError(FilterGraphError),

    #[error("during closing buffer source: {0}")]
    BufferSourceCloseError(FilterGraphError),

    #[error("during replace buffer: {0}")]
    BufferReplaceoseError(FilterGraphError),

    #[error("during cloning frame side data: {0}")]
    FrameSideDataCloneError(FilterGraphError),

    #[error("during parse: {0}")]
    ParseError(FilterGraphParseError),

    #[error("The data in the frame is invalid or corrupted")]
    InvalidData,

    #[error(
        "graph input '{0}' already holds {1} buffered frames and admitting the next \
         one would raise the best-effort retained-memory estimate to ~{2} bytes, \
         while another input has not yet delivered its first frame, so the filter \
         graph cannot be configured; check that every graph input actually produces \
         data (or produces it within the buffering window)"
    )]
    PreConfigQueueOverflow(String, usize, usize),

    // Only constructed on the FFmpeg 8+ buffersrc side-data clone path.
    #[cfg_attr(not(ffmpeg_8_0), allow(dead_code))]
    #[error(
        "graph input '{0}' would deep-copy an estimated {1} bytes of side-data \
         metadata into the buffersrc parameters, exceeding the side-data clone \
         estimate threshold; the frame's combined side-data metadata (across its \
         global and downmix entries) is pathologically large"
    )]
    OversizedSideDataClone(String, usize),

    #[error("Thread exited")]
    ThreadExited,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EncodingOperationError {
    #[error("during frame submission: {0}")]
    SendFrameError(EncodingError),

    #[error("during packet retrieval: {0}")]
    ReceivePacketError(EncodingError),

    #[error("during audio frame receive: {0}")]
    ReceiveAudioError(EncodingError),

    #[error(": Subtitle packets must have a pts")]
    SubtitleNotPts,

    #[error(": Muxer already finished")]
    MuxerFinished,

    /// An output stream buffered more packets before the muxer started than the
    /// pre-mux queue admits (fftools `AVERROR_BUFFER_TOO_SMALL`, "Too many
    /// packets buffered for output stream"). Unlike `MuxerFinished` this is a
    /// hard failure — never a silent truncation — so it must reach the
    /// scheduler error, not the graceful stop path.
    #[error(": too many packets buffered for an output stream before the muxer started; raise Output::set_max_muxing_queue_size / Output::set_muxing_queue_data_threshold, or check that every mapped output stream receives data")]
    MuxQueueFull,

    #[error("Encode subtitle error: {0}")]
    EncodeSubtitle(#[from] EncodeSubtitleError),

    #[error(": {0}")]
    AllocPacket(AllocPacketError),
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MuxingOperationError {
    #[error("during write header: {0}")]
    WriteHeader(WriteHeaderError),

    #[error("while initializing bitstream filter chain '{0}': {1}")]
    BitstreamFilterInit(String, MuxingError),

    #[error("during interleaved write: {0}")]
    InterleavedWriteError(MuxingError),

    #[error("during trailer write: {0}")]
    TrailerWriteError(MuxingError),

    #[error("during closing IO: {0}")]
    IOCloseError(MuxingError),

    #[error("Thread exited")]
    ThreadExited,
}

/// Errors specific to packet-sink outputs (`Output::new_by_packet_sink`).
///
/// The strict tier fails fast: configuration problems surface from `build()`
/// or from the job **before any sink callback runs**; per-packet violations
/// stop the job with the offending packet never delivered. `Clone` is
/// deliberate — the same value is recorded as the job error and handed to the
/// sink's `on_error` callback.
#[derive(thiserror::Error, Debug, Clone)]
#[non_exhaustive]
pub enum PacketSinkError {
    #[error("{0} is not supported on packet-sink outputs")]
    UnsupportedOption(&'static str),

    #[error("stream copy is not supported on packet-sink outputs (strict tier requires encoded streams)")]
    StreamCopyUnsupported,

    #[error("{kind} streams are not supported on packet-sink outputs (strict tier)")]
    UnsupportedStream { kind: &'static str },

    #[error("encoder '{encoder}' is not on the strict-tier whitelist for {kind} (v1 accepts: {allowed})")]
    EncoderNotWhitelisted {
        kind: &'static str,
        encoder: String,
        allowed: &'static str,
    },

    #[error("packet-sink output has no streams")]
    NoStreams,

    #[error("output stream {stream_index}: encoder produced no extradata; the strict tier requires codec configuration (avcC / AudioSpecificConfig) before the first callback")]
    MissingExtradata { stream_index: usize },

    #[error("output stream {stream_index}: invalid codec configuration: {reason}")]
    InvalidExtradata { stream_index: usize, reason: String },

    #[error("output stream {stream_index}: invalid time base {num}/{den} (positive numerator and denominator required)")]
    InvalidTimeBase {
        stream_index: usize,
        num: i32,
        den: i32,
    },

    #[error("output stream {stream_index}: packet time base {packet_num}/{packet_den} differs from the stream time base {stream_num}/{stream_den}")]
    PacketTimeBaseMismatch {
        stream_index: usize,
        packet_num: i32,
        packet_den: i32,
        stream_num: i32,
        stream_den: i32,
    },

    #[error("output stream {stream_index}: packet carries no {which} (strict tier rejects AV_NOPTS_VALUE)")]
    MissingTimestamp {
        stream_index: usize,
        which: &'static str,
    },

    #[error("output stream {stream_index}: non-monotonic dts (previous {prev}, current {current})")]
    NonMonotonicDts {
        stream_index: usize,
        prev: i64,
        current: i64,
    },

    #[error("output stream {stream_index}: duplicate pts {pts}")]
    DuplicatePts { stream_index: usize, pts: i64 },

    #[error("output stream {stream_index}: pts {pts} is earlier than dts {dts}")]
    PtsBeforeDts {
        stream_index: usize,
        pts: i64,
        dts: i64,
    },

    #[error("output stream {stream_index}: timestamp overflow while applying the shared time origin")]
    TimestampOverflow { stream_index: usize },

    #[error("output stream {stream_index}: packet duration is absent and cannot be derived (strict tier requires a positive duration)")]
    MissingDuration { stream_index: usize },

    #[error("output stream {stream_index}: malformed packet payload: {reason}")]
    MalformedPacket {
        stream_index: usize,
        reason: String,
    },

    #[error("output stream {stream_index}: packet processed outside the delivery phase (internal sequencing violation)")]
    PhaseViolation { stream_index: usize },

    #[error("output stream {stream_index}: mid-stream configuration change ({what}); the strict tier requires an immutable stream configuration")]
    ConfigChange {
        stream_index: usize,
        what: String,
    },

    #[error("output stream {stream_index}: in-band SPS/PPS parameter sets are not supported in the strict tier (WebCodecs avc requires out-of-band configuration)")]
    InBandParameterSets { stream_index: usize },

    #[error("on_stream_info callback rejected the stream configuration: {error}")]
    StreamInfoCallbackFailed {
        #[source]
        error: crate::core::packet_sink::PacketCallbackError,
    },

    #[error("on_packet callback failed on output stream {stream_index}: {error}")]
    PacketCallbackFailed {
        stream_index: usize,
        #[source]
        error: crate::core::packet_sink::PacketCallbackError,
    },

    #[error("the packet-sink channel receiver was dropped; delivery cancelled")]
    ChannelDisconnected,

    #[error("the job failed after this packet sink finished delivering: {message}")]
    JobFailed { message: String },
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenEncoderOperationError {
    #[error("during frame side data cloning: {0}")]
    FrameSideDataCloneError(OpenEncoderError),

    #[error("during channel layout copying: {0}")]
    ChannelLayoutCopyError(OpenEncoderError),

    #[error("during codec opening: {0}")]
    CodecOpenError(OpenEncoderError),

    #[error("while setting codec parameters: {0}")]
    CodecParametersError(OpenEncoderError),

    #[error(": unknown format of the frame")]
    UnknownFrameFormat,

    #[error("while setting subtitle: {0}")]
    SettingSubtitleError(OpenEncoderError),

    #[error("while Hw setup: {0}")]
    HwSetupError(OpenEncoderError),

    #[error("during context allocation: {0}")]
    ContextAllocationError(OpenEncoderError),

    #[error(": no frames were received before EOF; encoder never opened")]
    NoFramesReceived,

    #[error(": unsupported media type for encoding")]
    UnsupportedMediaType,

    #[error("Thread exited")]
    ThreadExited,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum UrlError {
    #[error("Null byte found in string at position {0}")]
    NullByteError(usize),
}

impl From<NulError> for Error {
    fn from(err: NulError) -> Self {
        Error::Url(UrlError::NullByteError(err.nul_position()))
    }
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenInputError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("File or stream not found")]
    NotFound,

    #[error("I/O error occurred while opening the file or stream")]
    IOError,

    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported input format")]
    NotImplemented,

    #[error("Operation not permitted to access the file or stream")]
    OperationNotPermitted,

    #[error("The data in the file or stream is invalid or corrupted")]
    InvalidData,

    #[error("The connection timed out while trying to open the stream")]
    Timeout,

    /// A builder option carried an invalid value (e.g. a non-positive
    /// `set_framerate`, a non-finite `set_ts_scale`, an out-of-range
    /// `set_io_buffer_size`). Setters store values as given and defer
    /// validation to open time, so a bad value surfaces here instead of
    /// panicking in the setter.
    #[error("Invalid input option: {0}")]
    InvalidOption(String),

    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),

    #[error("Invalid source provided")]
    InvalidSource,

    #[error("Invalid source format:{0}")]
    InvalidFormat(String),

    #[error("No seek callback is provided")]
    SeekFunctionMissing,
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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FindStreamError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("Reached end of file while looking for stream info")]
    EndOfFile,

    #[error("Timeout occurred while reading stream info")]
    Timeout,

    #[error("I/O error occurred while reading stream info")]
    IOError,

    #[error("The data in the stream is invalid or corrupted")]
    InvalidData,

    #[error("Functionality not implemented or unsupported stream format")]
    NotImplemented,

    #[error("Operation not permitted to access the file or stream")]
    OperationNotPermitted,

    #[error("No Stream found")]
    NoStreamFound,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FilterGraphParseError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("End of file reached during parsing")]
    EndOfFile,

    #[error("I/O error occurred during parsing")]
    IOError,

    #[error("Invalid data encountered during parsing")]
    InvalidData,

    #[error("Functionality not implemented or unsupported filter format")]
    NotImplemented,

    #[error("Permission denied during filter graph parsing")]
    PermissionDenied,

    #[error("Socket operation on non-socket during filter graph parsing")]
    NotSocket,

    #[error("Option not found during filter graph configuration")]
    OptionNotFound,

    #[error("Invalid file index {0} in filtergraph description {1}")]
    InvalidFileIndexInFg(usize, String),

    #[error("Invalid file index {0} in output url: {1}")]
    InvalidFileIndexInOutput(usize, String),

    #[error("Invalid filter specifier {0}")]
    InvalidFilterSpecifier(String),

    #[error("Filter '{0}' has output {1} ({2}) unconnected")]
    OutputUnconnected(String, usize, String),

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AllocOutputContextError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("File or stream not found")]
    NotFound,

    #[error("I/O error occurred while allocating the output context")]
    IOError,

    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported output format")]
    NotImplemented,

    #[error("Operation not permitted to allocate the output context")]
    OperationNotPermitted,

    #[error("Permission denied while allocating the output context")]
    PermissionDenied,

    #[error("The connection timed out while trying to allocate the output context")]
    Timeout,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenOutputError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("File or stream not found")]
    NotFound,

    #[error("I/O error occurred while opening the file or stream")]
    IOError,

    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported output format")]
    NotImplemented,

    #[error("Operation not permitted to open the file or stream")]
    OperationNotPermitted,

    #[error("Permission denied while opening the file or stream")]
    PermissionDenied,

    #[error("The connection timed out while trying to open the file or stream")]
    Timeout,

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
    EncoderUnavailable { name: String },

    #[error("Stream map '{0}' matches no streams;")]
    MatchesNoStreams(String),

    #[error("Invalid label {0}")]
    InvalidLabel(String),

    #[error("not contain any stream")]
    NotContainStream,

    #[error("unknown format of the frame")]
    UnknownFrameFormat,

    #[error("Invalid file index {0} in input url: {1}")]
    InvalidFileIndexInIntput(usize, String),

    #[error("{}. ret:{0}", crate::util::ffmpeg_utils::av_err2str(*.0))]
    UnknownError(i32),

    #[error("Invalid sink provided")]
    InvalidSink,

    #[error("No seek callback is provided")]
    SeekFunctionMissing,

    #[error("Format '{0}' is unsupported")]
    FormatUnsupported(String),

    #[error("Unknown pixel format: '{0}'")]
    UnknownPixelFormat(String),

    #[error("Unknown sample format: '{0}'")]
    UnknownSampleFormat(String),

    /// A builder option carried an invalid value (e.g. a malformed
    /// `set_force_key_frames` spec, an out-of-range `set_io_buffer_size`).
    /// Setters store values as given and defer validation to open time, so
    /// a bad value surfaces here instead of panicking in the setter or
    /// forcing a `Result` into the middle of a builder chain.
    #[error("Invalid output option: {0}")]
    InvalidOption(String),

    #[error("Failed to read attachment file '{0}'")]
    AttachmentRead(String, #[source] io::Error),

    #[error("Attachment file '{0}' is empty")]
    AttachmentEmpty(String),

    #[error("Attachment file '{0}' is too large ({1} bytes, limit {2} bytes)")]
    AttachmentTooLarge(String, u64, u64),

    #[error("Attachment mimetype must not be empty (file '{0}')")]
    AttachmentEmptyMimetype(String),

    /// A per-output video filter ([`Output::set_video_filter`]) was combined
    /// with stream copy for the same output's video — either
    /// `set_video_codec("copy")` or a copy stream map covering a video
    /// stream. Mirrors the FFmpeg CLI error for `-vf` + `-c:v copy`
    /// ("Filtering and streamcopy cannot be used together",
    /// ffmpeg_mux_init.c streamcopy_init).
    ///
    /// [`Output::set_video_filter`]: crate::core::context::output::Output::set_video_filter
    #[error(
        "Filtergraph '{0}' was specified, but codec copy was selected for the \
         output's video stream. Filtering and streamcopy cannot be used together"
    )]
    FilterWithStreamCopy(String),

    /// A per-output video filter ([`Output::set_video_filter`]) was set on an
    /// output whose video stream is fed by a context-level filtergraph
    /// (`FfmpegContextBuilder::filter_desc`). Mirrors the FFmpeg CLI error for
    /// `-vf` + `-filter_complex` on the same stream (ffmpeg_mux_init.c
    /// ost_get_filters: "Simple and complex filtering cannot be used together
    /// for the same stream").
    ///
    /// [`Output::set_video_filter`]: crate::core::context::output::Output::set_video_filter
    #[error(
        "Filtergraph '{0}' was specified for a video stream fed from a \
         context-level filtergraph. Simple and complex filtering cannot be \
         used together for the same stream"
    )]
    SimpleAndComplexFilter(String),

    /// A per-output simple filtergraph must be one connected linear chain:
    /// exactly one video input pad, one video output pad, a single connected
    /// component, and a directed path from the input to the output (fftools
    /// fg_create_simple's contract plus the topology rules a simple graph
    /// implies — a disconnected or unreachable description would encode
    /// unrelated frames or hang instead of filtering the stream). `reason`
    /// names the violated rule. Descriptions that split, merge or source
    /// streams belong in the context-level `filter_desc`.
    #[error(
        "Simple filtergraph '{desc}' is not a single connected chain: {reason}; \
         use FfmpegContextBuilder::filter_desc for complex graphs"
    )]
    SimpleFilterInvalidShape { desc: String, reason: String },

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

    /// A per-output simple filtergraph's pads must match the stream's media
    /// type (fftools fg_create_simple: "Filtergraph has a %s output, cannot
    /// connect it to %s output stream") — e.g. an audio chain like `anull`
    /// cannot be attached as a video filter.
    #[error(
        "Simple filtergraph '{desc}' has a {found} pad, cannot connect it to \
         the {expected} stream of this output"
    )]
    SimpleFilterMediaTypeMismatch {
        desc: String,
        found: String,
        expected: String,
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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FindDevicesError {
    #[error("AVCaptureDevice class not found in macOS")]
    AVCaptureDeviceNotFound,

    #[error("current media_type({0}) is not supported")]
    MediaTypeSupported(i32),
    #[error("current OS is not supported")]
    OsNotSupported,
    #[error("device_description can not to string")]
    UTF8Error,

    #[error("Memory allocation error")]
    OutOfMemory,
    #[error("Invalid argument provided")]
    InvalidArgument,
    #[error("Device or stream not found")]
    NotFound,
    #[error("I/O error occurred while accessing the device or stream")]
    IOError,
    #[error("Operation not permitted for this device or stream")]
    OperationNotPermitted,
    #[error("Permission denied while accessing the device or stream")]
    PermissionDenied,
    #[error("This functionality is not implemented")]
    NotImplemented,
    #[error("Bad file descriptor")]
    BadFileDescriptor,
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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WriteHeaderError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("File or stream not found")]
    NotFound,

    #[error("I/O error occurred while writing the header")]
    IOError,

    #[error("Pipe error, possibly the stream or data connection was broken")]
    PipeError,

    #[error("Invalid file descriptor")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported output format")]
    NotImplemented,

    #[error("Operation not permitted to write the header")]
    OperationNotPermitted,

    #[error("Permission denied while writing the header")]
    PermissionDenied,

    #[error("The connection timed out while trying to write the header")]
    Timeout,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EncodeSubtitleError {
    #[error("Memory allocation error while encoding subtitle")]
    OutOfMemory,

    #[error("Invalid argument provided for subtitle encoding")]
    InvalidArgument,

    #[error("Operation not permitted while encoding subtitle")]
    OperationNotPermitted,

    #[error("The encoding functionality is not implemented or unsupported")]
    NotImplemented,

    #[error("Encoder temporarily unable to process, please retry")]
    TryAgain,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AllocPacketError {
    #[error("Memory allocation error while alloc packet")]
    OutOfMemory,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum AllocFrameError {
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
    #[error("Memory allocation error while copying frame data")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum MuxingError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("I/O error occurred during muxing")]
    IOError,

    #[error("Broken pipe during muxing")]
    PipeError,

    #[error("Bad file descriptor encountered")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported")]
    NotImplemented,

    #[error("Operation not permitted")]
    OperationNotPermitted,

    #[error("Resource temporarily unavailable")]
    TryAgain,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenEncoderError {
    #[error("Memory allocation error occurred during encoder initialization")]
    OutOfMemory,

    #[error("Invalid argument provided to encoder")]
    InvalidArgument,

    #[error("I/O error occurred while opening encoder")]
    IOError,

    #[error("Broken pipe encountered during encoder initialization")]
    PipeError,

    #[error("Bad file descriptor used in encoder")]
    BadFileDescriptor,

    #[error("Encoder functionality not implemented or unsupported")]
    NotImplemented,

    #[error("Operation not permitted while configuring encoder")]
    OperationNotPermitted,

    #[error("Resource temporarily unavailable during encoder setup")]
    TryAgain,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum EncodingError {
    #[error("Memory allocation error during encoding")]
    OutOfMemory,

    #[error("Invalid argument provided to encoder")]
    InvalidArgument,

    #[error("I/O error occurred during encoding")]
    IOError,

    #[error("Broken pipe encountered during encoding")]
    PipeError,

    #[error("Bad file descriptor encountered during encoding")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported encoding feature")]
    NotImplemented,

    #[error("Operation not permitted for encoder")]
    OperationNotPermitted,

    #[error("Resource temporarily unavailable, try again later")]
    TryAgain,

    #[error("End of stream reached or no more frames to encode")]
    EndOfStream,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum FilterGraphError {
    #[error("Memory allocation error during filter graph processing")]
    OutOfMemory,

    #[error("Invalid argument provided to filter graph processing")]
    InvalidArgument,

    #[error("I/O error occurred during filter graph processing")]
    IOError,

    #[error("Broken pipe during filter graph processing")]
    PipeError,

    #[error("Bad file descriptor encountered during filter graph processing")]
    BadFileDescriptor,

    #[error("Functionality not implemented or unsupported during filter graph processing")]
    NotImplemented,

    #[error("Operation not permitted during filter graph processing")]
    OperationNotPermitted,

    #[error("Resource temporarily unavailable during filter graph processing")]
    TryAgain,

    #[error("EOF")]
    EOF,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenDecoderError {
    #[error("Memory allocation error during decoder initialization")]
    OutOfMemory,

    #[error("Invalid argument provided during decoder initialization")]
    InvalidArgument,

    #[error("Functionality not implemented or unsupported during decoder initialization")]
    NotImplemented,

    #[error("Resource temporarily unavailable during decoder initialization")]
    TryAgain,

    #[error("I/O error occurred during decoder initialization")]
    IOError,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DecodingError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("I/O error occurred during decoding")]
    IOError,

    #[error("Timeout occurred during decoding")]
    Timeout,

    #[error("Broken pipe encountered during decoding")]
    PipeError,

    #[error("Bad file descriptor encountered during decoding")]
    BadFileDescriptor,

    #[error("Unsupported functionality or format encountered")]
    NotImplemented,

    #[error("Operation not permitted")]
    OperationNotPermitted,

    #[error("Resource temporarily unavailable")]
    TryAgain,

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

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DecoderError {
    #[error("decoder '{0}' not found")]
    NotFound(String),
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum DemuxingError {
    #[error("Memory allocation error")]
    OutOfMemory,

    #[error("Invalid argument provided")]
    InvalidArgument,

    #[error("I/O error occurred during demuxing")]
    IOError,

    #[error("End of file reached during demuxing")]
    EndOfFile,

    #[error("Resource temporarily unavailable")]
    TryAgain,

    #[error("Functionality not implemented or unsupported")]
    NotImplemented,

    #[error("Operation not permitted")]
    OperationNotPermitted,

    #[error("Bad file descriptor encountered")]
    BadFileDescriptor,

    #[error("Invalid data found when processing input")]
    InvalidData,

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
}
