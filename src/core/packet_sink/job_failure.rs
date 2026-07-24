//! Structured job-failure summaries for packet-sink consumers.
//!
//! When a job fails OUTSIDE a packet sink's own delivery path, the sink's
//! terminal reports it as [`PacketSinkError::JobFailed`], which carries only
//! a preformatted message. [`JobFailureSummary`] is the structured companion
//! handed to the optional `on_job_failed` observer
//! ([`PacketSinkBuilder::on_job_failed`](super::PacketSinkBuilder::on_job_failed)):
//! a coarse [`JobFailureKind`], the raw FFmpeg error code where the recorded
//! error visibly carries one, the output stream index where the error names
//! one, and the exact `JobFailed` message.
//!
//! [`PacketSinkError::JobFailed`]: crate::error::PacketSinkError::JobFailed

use crate::error::{
    DecodingOperationError, DemuxingOperationError, EncodingOperationError, Error,
    FilterGraphOperationError, MuxingOperationError, OpenDecoderOperationError,
    OpenEncoderOperationError, OpenOutputError, PacketSinkError,
};

/// Coarse classification of a job failure reported to a packet sink via
/// [`PacketSinkError::JobFailed`].
///
/// Deliberately coarse: one bucket per pipeline stage, not one variant per
/// typed error. Consumers that need the exact failure keep reading the
/// authoritative error from `wait()`/`stop()`; the kind exists so a sink
/// consumer can route retry/telemetry decisions without parsing the message.
/// The enum is `#[non_exhaustive]` — match with a wildcard arm, and treat an
/// unknown kind like [`Other`](Self::Other).
///
/// [`PacketSinkError::JobFailed`]: crate::error::PacketSinkError::JobFailed
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobFailureKind {
    /// A decoder failed: decoder lookup, decoder open, or the decode loop.
    Decode,
    /// An encoder failed: encoder open or the encode loop.
    Encode,
    /// A filter stage failed: lavfi graph parse/configuration/processing, or
    /// a frame-filter pipeline.
    Filter,
    /// An output stage failed: container muxing (header/packet/trailer
    /// writes, bitstream filters), output-context allocation, or a sibling
    /// packet sink's delivery contract.
    Mux,
    /// A transport failed: opening or probing an input, demuxer reads, or a
    /// raw I/O error.
    Io,
    /// A user-supplied packet-sink callback rejected delivery on a sibling
    /// sink (`on_stream_info` / `on_packet` returning an error).
    Callback,
    /// No pipeline stage to name: worker panics, cancellation-adjacent
    /// states, startup teardown, and everything ambiguous.
    Other,
}

/// Structured summary of a job failure that happened OUTSIDE the receiving
/// packet sink's delivery path — the typed companion of the
/// [`PacketSinkError::JobFailed`] message.
///
/// Handed by reference to the optional
/// [`on_job_failed`](super::PacketSinkBuilder::on_job_failed) observer,
/// immediately before the matching `on_delivery_error(JobFailed)` dispatch.
/// The summary is derived from the recorded job error at the terminal slot;
/// `wait()`/`stop()` keep returning that original error unchanged, and
/// [`message`](Self::message) is byte-identical to the `JobFailed` message.
///
/// [`PacketSinkError::JobFailed`]: crate::error::PacketSinkError::JobFailed
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct JobFailureSummary {
    kind: JobFailureKind,
    stream_index: Option<usize>,
    ffmpeg_code: Option<i32>,
    message: String,
}

impl JobFailureSummary {
    /// Coarse classification of the recorded job error; see
    /// [`JobFailureKind`] for the buckets.
    pub fn kind(&self) -> JobFailureKind {
        self.kind
    }

    /// The output stream index the recorded error names, when it names one
    /// (today: a sibling packet sink's per-stream delivery errors and
    /// per-stream frame-filter mismatches). `None` whenever the error does
    /// not carry an index — absence is the common case, not an anomaly.
    pub fn stream_index(&self) -> Option<usize> {
        self.stream_index
    }

    /// The raw FFmpeg error code (a negative `AVERROR` value), when the
    /// recorded error's chain visibly carries one. The crate's typed errors
    /// convert well-known codes into named variants and drop the integer, so
    /// this is `Some` only for the unrecognized-code leaves; no code is ever
    /// re-synthesized from a named variant.
    pub fn ffmpeg_code(&self) -> Option<i32> {
        self.ffmpeg_code
    }

    /// The failure message — byte-identical to the `message` of the
    /// [`PacketSinkError::JobFailed`] handed to `on_delivery_error` right
    /// after the observer (the recorded job error's `Display` output, or a
    /// fixed substitute when formatting that error panicked).
    ///
    /// [`PacketSinkError::JobFailed`]: crate::error::PacketSinkError::JobFailed
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Classifies a recorded job error into a summary. The variant matching
    /// is pure crate code; only the `Display` formatting of `error` can run
    /// user code (frame-filter variants wrap user error types) — callers on
    /// the terminal path therefore invoke this inside their formatting panic
    /// containment and substitute [`Self::formatting_panicked`] on a caught
    /// panic.
    pub(crate) fn from_error(error: &Error) -> Self {
        let (kind, stream_index, ffmpeg_code) = classify(error);
        Self {
            kind,
            stream_index,
            ffmpeg_code,
            message: error.to_string(),
        }
    }

    /// The fallback summary for a recorded error whose `Display` panicked:
    /// [`JobFailureKind::Other`] plus the exact fixed message the terminal
    /// has always substituted on that path.
    pub(crate) fn formatting_panicked() -> Self {
        Self {
            kind: JobFailureKind::Other,
            stream_index: None,
            ffmpeg_code: None,
            message: String::from(
                "job error message unavailable: formatting the recorded error panicked",
            ),
        }
    }

    /// Consumes the summary, yielding the message for the `JobFailed`
    /// terminal — the byte-identity guarantee is a move, not a re-format.
    pub(crate) fn into_message(self) -> String {
        self.message
    }
}

/// Displays exactly the [`message`](JobFailureSummary::message).
impl std::fmt::Display for JobFailureSummary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// One leaf-code extractor per error enum whose `UnknownError(i32)` variant
/// still carries the raw AVERROR value (the named variants dropped it during
/// `From<i32>` conversion — re-deriving a code from a name would be
/// synthesis, not extraction).
macro_rules! unknown_code_fn {
    ($name:ident, $leaf:ident) => {
        fn $name(e: &crate::error::$leaf) -> Option<i32> {
            match e {
                crate::error::$leaf::UnknownError(code) => Some(*code),
                _ => None,
            }
        }
    };
}

unknown_code_fn!(muxing_leaf_code, MuxingError);
unknown_code_fn!(write_header_code, WriteHeaderError);
unknown_code_fn!(encoding_leaf_code, EncodingError);
unknown_code_fn!(encode_subtitle_code, EncodeSubtitleError);
unknown_code_fn!(open_encoder_leaf_code, OpenEncoderError);
unknown_code_fn!(decoding_leaf_code, DecodingError);
unknown_code_fn!(open_decoder_leaf_code, OpenDecoderError);
unknown_code_fn!(filter_graph_leaf_code, FilterGraphError);
unknown_code_fn!(filter_parse_code, FilterGraphParseError);
unknown_code_fn!(demuxing_leaf_code, DemuxingError);
unknown_code_fn!(open_input_code, OpenInputError);
unknown_code_fn!(find_stream_code, FindStreamError);
unknown_code_fn!(open_output_code, OpenOutputError);
unknown_code_fn!(alloc_output_code, AllocOutputContextError);

/// Coarse variant -> kind mapping. Stage-named variants land in their
/// stage's bucket; grab-bag, build-time and feature-specific variants that
/// name no pipeline stage stay `Other`. Wildcards (never exhaustive lists)
/// cover future variants of these `#[non_exhaustive]` enums, so an unmapped
/// addition degrades to `Other`/`None` instead of breaking the build.
fn classify(error: &Error) -> (JobFailureKind, Option<usize>, Option<i32>) {
    match error {
        // ---- Decode: decoder lookup, decoder open, decode loop. ----
        Error::Decoder(_) => (JobFailureKind::Decode, None, None),
        Error::OpenDecoder(e) => (JobFailureKind::Decode, None, open_decoder_code(e)),
        Error::Decoding(e) => (JobFailureKind::Decode, None, decoding_code(e)),
        // ---- Encode: encoder open, encode loop. ----
        Error::OpenEncoder(e) => (JobFailureKind::Encode, None, open_encoder_code(e)),
        Error::Encoding(e) => (JobFailureKind::Encode, None, encoding_code(e)),
        // ---- Filter: lavfi graphs and frame-filter pipelines. ----
        Error::FilterGraphParse(e) => (JobFailureKind::Filter, None, filter_parse_code(e)),
        Error::FilterGraph(e) => (JobFailureKind::Filter, None, filter_graph_code(e)),
        Error::FilterDescUtf8
        | Error::FilterNameUtf8
        | Error::FilterZeroOutputs
        | Error::FilterZeroInputs
        | Error::FrameFilterInit(_)
        | Error::FrameFilterProcess(_)
        | Error::FrameFilterRequest(_)
        | Error::FrameFilterTypeNoMatched(_, _)
        | Error::FrameFilterDstFinished
        | Error::FrameFilterFrameDuplicateFailed
        | Error::FrameFilterThreadExited => (JobFailureKind::Filter, None, None),
        Error::FrameFilterStreamTypeNoMatched(_, stream_index, _) => {
            (JobFailureKind::Filter, Some(*stream_index), None)
        }
        #[cfg(feature = "wgpu")]
        Error::WgpuFilter(_) => (JobFailureKind::Filter, None, None),
        #[cfg(feature = "opengl")]
        Error::OpenGLFilter(_) => (JobFailureKind::Filter, None, None),
        // ---- Mux: container muxing, output allocation, sibling sinks. ----
        Error::Muxing(e) => (JobFailureKind::Mux, None, muxing_code(e)),
        Error::AllocOutputContext(e) => (JobFailureKind::Mux, None, alloc_output_code(e)),
        Error::PacketSink(e) => classify_packet_sink(e),
        // ---- Io: input open/probe, demux reads, raw I/O. ----
        Error::OpenInputStream(e) => (JobFailureKind::Io, None, open_input_code(e)),
        Error::FindStream(e) => (JobFailureKind::Io, None, find_stream_code(e)),
        // OpenOutput is an open-time grab-bag with ONE runtime escapee: the
        // encoder task records UnknownFrameFormat while lazily opening the
        // encoder on the first frame (malformed init frame) — an
        // encode-stage failure, not transport.
        Error::OpenOutput(OpenOutputError::UnknownFrameFormat) => {
            (JobFailureKind::Encode, None, None)
        }
        Error::OpenOutput(e) => (JobFailureKind::Io, None, open_output_code(e)),
        Error::Demuxing(e) => (JobFailureKind::Io, None, demuxing_code(e)),
        Error::IO(_) => (JobFailureKind::Io, None, None),
        // ---- Other: no pipeline stage to name (worker panics, scheduler
        // states, non-job subsystems, CLI/argument validation, RTMP server
        // lifecycle, subtitles). ----
        _ => (JobFailureKind::Other, None, None),
    }
}

/// A sibling packet sink's recorded error: the two consumer-callback
/// rejections are `Callback`; every other variant is that sink's delivery
/// contract at the output stage — `Mux`. The offending output stream index
/// rides along wherever the variant carries one.
fn classify_packet_sink(e: &PacketSinkError) -> (JobFailureKind, Option<usize>, Option<i32>) {
    match e {
        PacketSinkError::StreamInfoCallbackFailed { .. } => (JobFailureKind::Callback, None, None),
        PacketSinkError::PacketCallbackFailed { stream_index, .. } => {
            (JobFailureKind::Callback, Some(*stream_index), None)
        }
        PacketSinkError::MissingExtradata { stream_index }
        | PacketSinkError::InvalidExtradata { stream_index, .. }
        | PacketSinkError::InvalidTimeBase { stream_index, .. }
        | PacketSinkError::PacketTimeBaseMismatch { stream_index, .. }
        | PacketSinkError::MissingTimestamp { stream_index, .. }
        | PacketSinkError::NonMonotonicDts { stream_index, .. }
        | PacketSinkError::DuplicatePts { stream_index, .. }
        | PacketSinkError::PtsBeforeDts { stream_index, .. }
        | PacketSinkError::TimestampOverflow { stream_index }
        | PacketSinkError::MissingDuration { stream_index }
        | PacketSinkError::MalformedPacket { stream_index, .. }
        | PacketSinkError::PhaseViolation { stream_index }
        | PacketSinkError::ConfigChange { stream_index, .. }
        | PacketSinkError::InBandParameterSets { stream_index } => {
            (JobFailureKind::Mux, Some(*stream_index), None)
        }
        _ => (JobFailureKind::Mux, None, None),
    }
}

fn muxing_code(e: &MuxingOperationError) -> Option<i32> {
    match e {
        MuxingOperationError::WriteHeader(w) => write_header_code(w),
        MuxingOperationError::BitstreamFilterInit(_, m)
        | MuxingOperationError::InterleavedWriteError(m)
        | MuxingOperationError::TrailerWriteError(m)
        | MuxingOperationError::IOCloseError(m) => muxing_leaf_code(m),
        _ => None,
    }
}

fn encoding_code(e: &EncodingOperationError) -> Option<i32> {
    match e {
        EncodingOperationError::SendFrameError(x)
        | EncodingOperationError::ReceivePacketError(x)
        | EncodingOperationError::ReceiveAudioError(x) => encoding_leaf_code(x),
        // Subtitle encoding records a live avcodec_encode_subtitle return
        // through its own leaf type — a literal raw code the extraction
        // rule covers like every other UnknownError leaf.
        EncodingOperationError::EncodeSubtitle(s) => encode_subtitle_code(s),
        _ => None,
    }
}

fn open_encoder_code(e: &OpenEncoderOperationError) -> Option<i32> {
    match e {
        OpenEncoderOperationError::FrameSideDataCloneError(x)
        | OpenEncoderOperationError::ChannelLayoutCopyError(x)
        | OpenEncoderOperationError::CodecOpenError(x)
        | OpenEncoderOperationError::CodecParametersError(x)
        | OpenEncoderOperationError::SettingSubtitleError(x)
        | OpenEncoderOperationError::HwSetupError(x)
        | OpenEncoderOperationError::ContextAllocationError(x) => open_encoder_leaf_code(x),
        _ => None,
    }
}

fn decoding_code(e: &DecodingOperationError) -> Option<i32> {
    match e {
        DecodingOperationError::FrameRefError(x)
        | DecodingOperationError::FrameCopyPropsError(x)
        | DecodingOperationError::DecodeSubtitleError(x)
        | DecodingOperationError::CopySubtitleError(x)
        | DecodingOperationError::SendPacketError(x)
        | DecodingOperationError::ReceiveFrameError(x)
        | DecodingOperationError::FrameAllocationError(x)
        | DecodingOperationError::PacketAllocationError(x)
        | DecodingOperationError::SubtitleAllocationError(x)
        | DecodingOperationError::HWRetrieveDataError(x)
        | DecodingOperationError::CroppingError(x) => decoding_leaf_code(x),
        _ => None,
    }
}

fn open_decoder_code(e: &OpenDecoderOperationError) -> Option<i32> {
    match e {
        OpenDecoderOperationError::ContextAllocationError(x)
        | OpenDecoderOperationError::ParameterApplicationError(x)
        | OpenDecoderOperationError::DecoderOpenError(x)
        | OpenDecoderOperationError::ChannelLayoutCopyError(x)
        | OpenDecoderOperationError::HwSetupError(x) => open_decoder_leaf_code(x),
        _ => None,
    }
}

fn filter_graph_code(e: &FilterGraphOperationError) -> Option<i32> {
    match e {
        FilterGraphOperationError::RequestOldestError(x)
        | FilterGraphOperationError::ProcessFramesError(x)
        | FilterGraphOperationError::SendFramesError(x)
        | FilterGraphOperationError::ChannelLayoutCopyError(x)
        | FilterGraphOperationError::BufferSourceAddFrameError(x)
        | FilterGraphOperationError::BufferSourceCloseError(x)
        | FilterGraphOperationError::BufferReplaceoseError(x)
        | FilterGraphOperationError::FrameSideDataCloneError(x) => filter_graph_leaf_code(x),
        FilterGraphOperationError::ParseError(p) => filter_parse_code(p),
        _ => None,
    }
}

fn demuxing_code(e: &DemuxingOperationError) -> Option<i32> {
    match e {
        DemuxingOperationError::ReadFrameError(x)
        | DemuxingOperationError::PacketRefError(x)
        | DemuxingOperationError::SeekFileError(x) => demuxing_leaf_code(x),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::packet_sink::PacketCallbackError;
    use crate::error::{
        AllocOutputContextError, DecoderError, DecodingError, DecodingOperationError,
        DemuxingError, DemuxingOperationError, EncodeSubtitleError, EncodingError,
        EncodingOperationError, Error, FilterGraphOperationError, FilterGraphParseError,
        MuxingError, MuxingOperationError, OpenDecoderOperationError, OpenEncoderOperationError,
        OpenInputError, OpenOutputError, PacketSinkError, WriteHeaderError,
    };

    fn classified(e: &Error) -> (JobFailureKind, Option<usize>, Option<i32>) {
        let s = JobFailureSummary::from_error(e);
        (s.kind(), s.stream_index(), s.ffmpeg_code())
    }

    /// The coarse variant -> kind mapping: every pipeline stage the Error
    /// enum names lands in its bucket, and everything ambiguous stays
    /// `Other` rather than guessing.
    #[test]
    fn maps_error_variants_to_coarse_kinds() {
        // Decode: decoder lookup, decoder open, decode loop.
        assert_eq!(
            classified(&Error::Decoder(DecoderError::NotFound("h264".into()))).0,
            JobFailureKind::Decode
        );
        assert_eq!(
            classified(&Error::OpenDecoder(OpenDecoderOperationError::InvalidName)).0,
            JobFailureKind::Decode
        );
        assert_eq!(
            classified(&Error::Decoding(DecodingOperationError::CorruptFrame)).0,
            JobFailureKind::Decode
        );
        // Encode: encoder open, encode loop.
        assert_eq!(
            classified(&Error::OpenEncoder(OpenEncoderOperationError::NoFramesReceived)).0,
            JobFailureKind::Encode
        );
        assert_eq!(
            classified(&Error::Encoding(EncodingOperationError::MuxerFinished)).0,
            JobFailureKind::Encode
        );
        // Filter: lavfi graphs (parse and runtime) and frame-filter
        // pipelines.
        assert_eq!(
            classified(&Error::FilterGraph(FilterGraphOperationError::InvalidData)).0,
            JobFailureKind::Filter
        );
        assert_eq!(
            classified(&Error::FilterZeroInputs).0,
            JobFailureKind::Filter
        );
        assert_eq!(
            classified(&Error::FrameFilterProcess(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "user filter failed",
            ))))
            .0,
            JobFailureKind::Filter
        );
        assert_eq!(
            classified(&Error::FrameFilterStreamTypeNoMatched(
                "video".into(),
                2,
                "audio".into()
            )),
            (JobFailureKind::Filter, Some(2), None)
        );
        // Mux: container muxing and output-context allocation.
        assert_eq!(
            classified(&Error::Muxing(MuxingOperationError::ThreadExited)).0,
            JobFailureKind::Mux
        );
        assert_eq!(
            classified(&Error::AllocOutputContext(
                AllocOutputContextError::OutOfMemory
            ))
            .0,
            JobFailureKind::Mux
        );
        // Io: input open/probe, demux reads, raw IO.
        assert_eq!(
            classified(&Error::OpenInputStream(OpenInputError::Timeout)).0,
            JobFailureKind::Io
        );
        assert_eq!(
            classified(&Error::Demuxing(DemuxingOperationError::ThreadExited)).0,
            JobFailureKind::Io
        );
        assert_eq!(
            classified(&Error::IO(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "peer gone",
            )))
            .0,
            JobFailureKind::Io
        );
        // Other: no pipeline stage to name.
        assert_eq!(
            classified(&Error::WorkerPanicked("muxer1:mpegts".into())).0,
            JobFailureKind::Other
        );
        assert_eq!(classified(&Error::StartFailed).0, JobFailureKind::Other);
        assert_eq!(classified(&Error::Bug).0, JobFailureKind::Other);
        assert_eq!(classified(&Error::NotStarted).0, JobFailureKind::Other);
    }

    /// A sibling packet sink's recorded error: consumer-callback rejections
    /// classify as `Callback`, every other sink delivery violation as `Mux`,
    /// and the offending output stream index rides along where the variant
    /// carries one.
    #[test]
    fn sibling_packet_sink_errors_split_callback_from_delivery() {
        assert_eq!(
            classified(&Error::PacketSink(PacketSinkError::PacketCallbackFailed {
                stream_index: 3,
                error: PacketCallbackError::new("consumer said no"),
            })),
            (JobFailureKind::Callback, Some(3), None)
        );
        assert_eq!(
            classified(&Error::PacketSink(
                PacketSinkError::StreamInfoCallbackFailed {
                    error: PacketCallbackError::new("config rejected"),
                }
            )),
            (JobFailureKind::Callback, None, None)
        );
        assert_eq!(
            classified(&Error::PacketSink(PacketSinkError::NonMonotonicDts {
                stream_index: 1,
                prev: 5,
                current: 5,
            })),
            (JobFailureKind::Mux, Some(1), None)
        );
        assert_eq!(
            classified(&Error::PacketSink(PacketSinkError::ChannelDisconnected)),
            (JobFailureKind::Mux, None, None)
        );
    }

    /// The raw FFmpeg code is extracted ONLY where the chain visibly carries
    /// one — the leaf `UnknownError(i32)` variants. Named leaf variants
    /// dropped their code at conversion, and re-deriving one from the name
    /// would be synthesis, not extraction.
    #[test]
    fn extracts_raw_codes_only_from_unknown_error_leaves() {
        let ext = ffmpeg_sys_next::AVERROR_EXTERNAL;
        assert_eq!(
            classified(&Error::Muxing(MuxingOperationError::InterleavedWriteError(
                MuxingError::UnknownError(ext)
            )))
            .2,
            Some(ext)
        );
        assert_eq!(
            classified(&Error::Muxing(MuxingOperationError::WriteHeader(
                WriteHeaderError::UnknownError(-77)
            )))
            .2,
            Some(-77)
        );
        assert_eq!(
            classified(&Error::Encoding(EncodingOperationError::SendFrameError(
                EncodingError::UnknownError(-1)
            )))
            .2,
            Some(-1)
        );
        assert_eq!(
            classified(&Error::Decoding(DecodingOperationError::ReceiveFrameError(
                DecodingError::UnknownError(-2)
            )))
            .2,
            Some(-2)
        );
        assert_eq!(
            classified(&Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::UnknownError(-3)
            )))
            .2,
            Some(-3)
        );
        assert_eq!(
            classified(&Error::Demuxing(DemuxingOperationError::ReadFrameError(
                DemuxingError::UnknownError(-4)
            )))
            .2,
            Some(-4)
        );
        // Named leaves carry no code anymore; nothing is synthesized.
        assert_eq!(
            classified(&Error::Muxing(MuxingOperationError::InterleavedWriteError(
                MuxingError::IOError
            )))
            .2,
            None
        );
        assert_eq!(
            classified(&Error::IO(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "peer gone",
            )))
            .2,
            None
        );
    }

    /// A malformed init frame fails the ENCODER's lazy open with
    /// `OpenOutputError::UnknownFrameFormat`, recorded as the job error by
    /// the encoder task — an encode-stage failure that must not classify as
    /// transport I/O (a retry router treating Io as transient would retry
    /// deterministic malformed input). The rest of the open-time grab-bag
    /// stays Io.
    #[test]
    fn runtime_encoder_open_failure_classifies_as_encode_not_io() {
        assert_eq!(
            classified(&Error::OpenOutput(OpenOutputError::UnknownFrameFormat)).0,
            JobFailureKind::Encode
        );
        assert_eq!(
            classified(&Error::OpenOutput(OpenOutputError::Timeout)).0,
            JobFailureKind::Io
        );
    }

    /// Subtitle encoding records a LIVE raw code from
    /// `avcodec_encode_subtitle` through `EncodeSubtitleError::UnknownError`
    /// — a literal leaf the extraction rule promises to surface.
    #[test]
    fn subtitle_encode_unknown_code_is_extracted() {
        assert_eq!(
            classified(&Error::Encoding(EncodingOperationError::EncodeSubtitle(
                EncodeSubtitleError::UnknownError(-77)
            ))),
            (JobFailureKind::Encode, None, Some(-77))
        );
    }

    /// The summary's message IS the recorded error's Display output, and the
    /// summary displays as exactly that message.
    #[test]
    fn message_is_the_error_display_and_the_summary_displays_it() {
        let recorded = Error::WorkerPanicked("muxer1:mpegts".to_string());
        let summary = JobFailureSummary::from_error(&recorded);
        assert_eq!(summary.message(), recorded.to_string());
        assert_eq!(summary.to_string(), recorded.to_string());
        // Pin the absolute text once so drift on either side is caught.
        assert_eq!(
            summary.message(),
            "Worker thread 'muxer1:mpegts' panicked; output may be incomplete"
        );
    }

    /// The Display-panic fallback is `Other` plus the exact fixed message
    /// the terminal used before the summary existed.
    #[test]
    fn formatting_panic_fallback_matches_the_fixed_message() {
        let summary = JobFailureSummary::formatting_panicked();
        assert_eq!(summary.kind(), JobFailureKind::Other);
        assert_eq!(summary.stream_index(), None);
        assert_eq!(summary.ffmpeg_code(), None);
        assert_eq!(
            summary.message(),
            "job error message unavailable: formatting the recorded error panicked"
        );
    }
}
