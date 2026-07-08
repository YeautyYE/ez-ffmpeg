use crate::core::context::encoder_stream::EncoderStream;
use crate::core::context::output::{AttachmentSpec, StreamMap, VSyncMethod};
use crate::core::context::pre_mux_queue::{
    self, PreMuxQueueConfig, PreMuxQueueReceiver, PreMuxQueueSender,
};
use crate::core::context::{FrameBox, PacketBox};
use crate::core::filter::frame_pipeline::FramePipeline;
use crate::core::scheduler::input_controller::SchNode;
use crate::error::OpenOutputError;
use crate::raw::FormatContext;
use crossbeam_channel::{Receiver, Sender};
use ffmpeg_sys_next::{
    avformat_new_stream, AVCodec, AVFormatContext, AVMediaType, AVRational, AVSampleFormat,
    AVStream, AVFMT_NOTIMESTAMPS, AVFMT_VARIABLE_FPS,
};
use log::error;
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr::null;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Tracks which input stream produced each output stream.
/// Mirrors FFmpeg's `OutputStream->ist` bookkeeping in
/// `fftools/ffmpeg_mux_init.c`, enabling accurate metadata propagation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamSource {
    pub(crate) input_file_index: usize,
    pub(crate) input_stream_index: usize,
    pub(crate) encoded: bool,
}

/// Registry for per-output stream source information (FFmpeg reference:
/// `OutputStream->ist` and related logic in `ffmpeg_mux_init.c`).
#[derive(Default, Clone)]
pub(crate) struct StreamSourceRegistry {
    entries: Vec<Option<StreamSource>>,
}

impl StreamSourceRegistry {
    pub(crate) fn register(
        &mut self,
        output_idx: usize,
        input_file_index: usize,
        input_stream_index: usize,
        encoded: bool,
    ) {
        if self.entries.len() <= output_idx {
            self.entries.resize(output_idx + 1, None);
        }
        self.entries[output_idx] = Some(StreamSource {
            input_file_index,
            input_stream_index,
            encoded,
        });
    }

    pub(crate) fn stream_input_mapping(&self) -> Vec<(usize, (usize, usize))> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(idx, entry)| {
                entry
                    .as_ref()
                    .map(|source| (idx, (source.input_file_index, source.input_stream_index)))
            })
            .collect()
    }

    pub(crate) fn encoding_streams(&self) -> Vec<usize> {
        self.entries
            .iter()
            .enumerate()
            .filter_map(|(idx, entry)| match entry {
                Some(source) if source.encoded => Some(idx),
                _ => None,
            })
            .collect()
    }
}

/// Per-media-type bitstream-filter chains for one output, materialized from
/// the public `Output::set_{video,audio,subtitle}_bsf` API into NUL-terminated
/// `CString`s (validated at build time). Resolved down to a per-output-stream
/// chain by media type in `_mux_init` (FFmpeg `-bsf:v/-bsf:a/-bsf:s`).
///
/// PR2 seam: explicit per-map BSF (`add_stream_map_with_bsf`) will override the
/// media-type default per stream; that override is not carried here yet.
#[derive(Clone, Default)]
pub(crate) struct StreamBsfChains {
    pub(crate) video: Option<CString>,
    pub(crate) audio: Option<CString>,
    pub(crate) subtitle: Option<CString>,
}

impl StreamBsfChains {
    /// True when no media type has a BSF chain: the mux packet path then takes
    /// exactly the pre-BSF code path (zero behavior change).
    pub(crate) fn is_empty(&self) -> bool {
        self.video.is_none() && self.audio.is_none() && self.subtitle.is_none()
    }

    /// The chain that applies to a stream of `media_type`, or `None`.
    pub(crate) fn for_media_type(&self, media_type: AVMediaType) -> Option<&CString> {
        match media_type {
            AVMediaType::AVMEDIA_TYPE_VIDEO => self.video.as_ref(),
            AVMediaType::AVMEDIA_TYPE_AUDIO => self.audio.as_ref(),
            AVMediaType::AVMEDIA_TYPE_SUBTITLE => self.subtitle.as_ref(),
            _ => None,
        }
    }
}

/// Plan for a muxer's `-shortest` packet sync-queue (`sq_mux`), computed while the
/// output context is still owned and consumed by the mux worker to build the
/// actual `SyncQueue<PacketBox>`. Mirrors the `sq_mux` branch of FFmpeg's
/// `setup_sync_queues` (`ffmpeg_mux_init.c:2114-2138`). Plain data so it threads
/// cleanly from the build layer into the scheduler layer.
pub(crate) struct SqMuxPlan {
    /// `-shortest_buf_duration` in microseconds.
    pub(crate) buf_size_us: i64,
    /// One entry per interleaved (non-attachment) output stream, in
    /// output-stream-index order: `(output_stream_index, limiting, frames_max)`.
    /// `limiting` mirrors `shortest || max_frames < MAX`; `frames_max` drives
    /// `sq_limit_frames`.
    pub(crate) streams: Vec<(usize, bool, Option<u64>)>,
}

pub(crate) struct Muxer {
    pub(crate) url: String,

    /// Owns the output `AVFormatContext`. `Some` from construction until the mux
    /// worker takes it (see `mux_task`); `None` afterward. Dropping the `Muxer`
    /// before that hand-off (built but never started) frees the context via
    /// `FormatContext`'s Drop — no manual `Drop` needed.
    pub(crate) out_fmt_ctx: Option<FormatContext>,
    pub(crate) oformat_flags: i32,
    pub(crate) frame_pipelines: Option<Vec<FramePipeline>>,

    pub(crate) stream_map_specs: Vec<crate::core::context::output::StreamMapSpec>,
    pub(crate) stream_maps: Vec<StreamMap>,
    pub(crate) video_codec: Option<String>,
    pub(crate) audio_codec: Option<String>,
    pub(crate) subtitle_codec: Option<String>,

    /// Per-media-type BSF chains (`-bsf:v/-bsf:a/-bsf:s`), NUL-validated at
    /// build time. Resolved per output stream by media type in `_mux_init`.
    pub(crate) bsf_chains: StreamBsfChains,
    pub(crate) start_time_us: Option<i64>,
    pub(crate) recording_time_us: Option<i64>,
    /// FFmpeg `-shortest` for this output (`Output::set_shortest`): finish when the
    /// shortest limiting stream ends. Drives the per-mux `sq_enc`/`sq_mux` build.
    pub(crate) shortest: bool,
    /// `-shortest_buf_duration` in microseconds (default 10 s); the sync-queue
    /// buffering bound.
    pub(crate) shortest_buf_duration_us: i64,
    pub(crate) framerate: Option<AVRational>,
    pub(crate) framerate_max: Option<AVRational>,
    pub(crate) vsync_method: VSyncMethod,
    pub(crate) bits_per_raw_sample: Option<i32>,
    pub(crate) audio_sample_rate: Option<i32>,
    pub(crate) audio_channels: Option<i32>,
    pub(crate) audio_sample_fmt: Option<AVSampleFormat>,

    pub(crate) video_qscale: Option<i32>,
    pub(crate) audio_qscale: Option<i32>,

    /// Sorted forced-keyframe times in microseconds (`AV_TIME_BASE_Q`); `None` = off.
    /// FFmpeg `-force_key_frames` list form, applied to re-encoded video only.
    pub(crate) forced_kf_pts: Option<Vec<i64>>,

    pub(crate) max_video_frames: Option<i64>,
    pub(crate) max_audio_frames: Option<i64>,
    pub(crate) max_subtitle_frames: Option<i64>,

    pub(crate) video_codec_opts: Option<HashMap<CString, CString>>,
    pub(crate) audio_codec_opts: Option<HashMap<CString, CString>>,
    pub(crate) subtitle_codec_opts: Option<HashMap<CString, CString>>,
    pub(crate) format_opts: Option<HashMap<CString, CString>>,

    pub(crate) copy_ts: bool,

    // Metadata fields
    pub(crate) global_metadata: Option<HashMap<String, String>>,
    pub(crate) stream_metadata: Vec<(String, String, String)>, // (spec, key, value)
    pub(crate) chapter_metadata: HashMap<usize, HashMap<String, String>>,
    pub(crate) program_metadata: HashMap<usize, HashMap<String, String>>,
    pub(crate) metadata_map: Vec<crate::core::metadata::MetadataMapping>,
    pub(crate) auto_copy_metadata: bool,
    pub(crate) stream_sources: StreamSourceRegistry,

    // Stream disable flags (P1 Features)
    pub(crate) video_disable: bool,
    pub(crate) audio_disable: bool,
    pub(crate) subtitle_disable: bool,
    pub(crate) data_disable: bool,

    // Output pixel format (P1 Features)
    // Parsed from string in open_output_file, stored as AVPixelFormat
    pub(crate) pix_fmt: Option<ffmpeg_sys_next::AVPixelFormat>,

    // Auto-conversion tuning (NEW-SC-03): sws/swr option strings requested by
    // this output for the auto-inserted scale/aresample filters. Threaded into
    // the output's OutputFilterOptions in configure_output_filter_opts; the
    // graph-level value is finally resolved and applied in filter_task.
    pub(crate) sws_opts: Option<String>,
    pub(crate) swr_opts: Option<String>,

    /// Files to embed as attachment streams (`-attach`). Resolved into
    /// `AVMEDIA_TYPE_ATTACHMENT` streams during `outputs_bind`, after every
    /// mapped/encoded stream has been created (so they take the highest indices
    /// and never collide with the packet path).
    pub(crate) attachments: Vec<AttachmentSpec>,

    streams: Vec<EncoderStream>,
    queue: Option<(Sender<PacketBox>, Receiver<PacketBox>)>,
    src_pre_receivers: Vec<PreMuxQueueReceiver>,
    pre_mux_queue_config: PreMuxQueueConfig,
    mux_start_gate: Arc<crate::core::context::MuxStartGate>,

    // Join handles of this muxer's encoder threads, delivered by enc_init.
    // The muxer joins them before freeing its AVFormatContext: the encoders
    // write into AVStreams owned by that context.
    enc_handles: (
        Sender<std::thread::JoinHandle<()>>,
        Receiver<std::thread::JoinHandle<()>>,
    ),

    pub(crate) nb_streams: usize,
    pub(crate) nb_streams_ready: Arc<AtomicUsize>,

    pub(crate) mux_stream_nodes: Vec<Arc<SchNode>>,
}

// SAFETY: Muxer can be sent to another thread. The raw FFmpeg pointers are only
// accessed from the owning thread. Note: Muxer is NOT Sync because it contains
// frame_pipelines with Box<dyn FrameFilter> which only implements Send.
unsafe impl Send for Muxer {}

// No manual `Drop`: the `Option<FormatContext>` field owns the context and frees it
// on drop (output path, custom-IO-aware) whether the worker took it (`None`) or the
// job never started (`Some`). Replaces the old hand-written Drop.

impl Muxer {
    /// Raw output `AVFormatContext` pointer for read-only FFI (field reads,
    /// `avformat_new_stream`, `avformat_write_header`). Returns null once the mux
    /// worker has taken ownership. Callers must not free through it.
    pub(crate) fn out_fmt_ctx_ptr(&self) -> *mut AVFormatContext {
        // SAFETY: as_ptr just returns the stored pointer.
        self.out_fmt_ctx
            .as_ref()
            .map_or(std::ptr::null_mut(), |fc| unsafe { fc.as_ptr() })
    }

    pub(crate) fn new(
        url: String,
        out_fmt_ctx: FormatContext,
        frame_pipelines: Option<Vec<FramePipeline>>,
        stream_map_specs: Vec<crate::core::context::output::StreamMapSpec>,
        stream_maps: Vec<StreamMap>,
        video_codec: Option<String>,
        audio_codec: Option<String>,
        subtitle_codec: Option<String>,
        bsf_chains: StreamBsfChains,
        start_time_us: Option<i64>,
        recording_time_us: Option<i64>,
        shortest: bool,
        shortest_buf_duration_us: i64,
        framerate: Option<AVRational>,
        framerate_max: Option<AVRational>,
        vsync_method: VSyncMethod,
        bits_per_raw_sample: Option<i32>,
        audio_sample_rate: Option<i32>,
        audio_channels: Option<i32>,
        audio_sample_fmt: Option<AVSampleFormat>,
        video_qscale: Option<i32>,
        audio_qscale: Option<i32>,
        forced_kf_pts: Option<Vec<i64>>,
        max_video_frames: Option<i64>,
        max_audio_frames: Option<i64>,
        max_subtitle_frames: Option<i64>,
        video_codec_opts: Option<HashMap<CString, CString>>,
        audio_codec_opts: Option<HashMap<CString, CString>>,
        subtitle_codec_opts: Option<HashMap<CString, CString>>,
        format_opts: Option<HashMap<CString, CString>>,
        copy_ts: bool,
        global_metadata: Option<HashMap<String, String>>,
        stream_metadata: Vec<(String, String, String)>,
        chapter_metadata: HashMap<usize, HashMap<String, String>>,
        program_metadata: HashMap<usize, HashMap<String, String>>,
        metadata_map: Vec<crate::core::metadata::MetadataMapping>,
        auto_copy_metadata: bool,
        video_disable: bool,
        audio_disable: bool,
        subtitle_disable: bool,
        data_disable: bool,
        pix_fmt: Option<ffmpeg_sys_next::AVPixelFormat>,
        pre_mux_queue_config: PreMuxQueueConfig,
        sws_opts: Option<String>,
        swr_opts: Option<String>,
        attachments: Vec<AttachmentSpec>,
    ) -> Self {
        // Read oformat flags via the pointer BEFORE moving `out_fmt_ctx` into the
        // struct (the field can no longer be the access path once it's moved).
        // SAFETY: out_fmt_ctx is a valid, just-allocated output context.
        let oformat_flags = unsafe { (*(*out_fmt_ctx.as_ptr()).oformat).flags };
        Self {
            url,
            frame_pipelines,
            out_fmt_ctx: Some(out_fmt_ctx),
            oformat_flags,
            stream_map_specs,
            stream_maps,
            video_codec,
            audio_codec,
            subtitle_codec,
            bsf_chains,
            start_time_us,
            recording_time_us,
            shortest,
            shortest_buf_duration_us,
            framerate,
            framerate_max,
            vsync_method,
            bits_per_raw_sample,
            audio_sample_rate,
            audio_channels,
            audio_sample_fmt,
            video_qscale,
            audio_qscale,
            forced_kf_pts,
            max_video_frames,
            max_audio_frames,
            max_subtitle_frames,
            video_codec_opts,
            audio_codec_opts,
            subtitle_codec_opts,
            format_opts,
            copy_ts,
            streams: vec![],
            queue: None,
            src_pre_receivers: vec![],
            pre_mux_queue_config,
            mux_start_gate: Arc::new(crate::core::context::MuxStartGate::new()),
            enc_handles: crossbeam_channel::unbounded(),
            nb_streams: 0,
            nb_streams_ready: Arc::new(Default::default()),
            mux_stream_nodes: vec![],
            global_metadata,
            stream_metadata,
            chapter_metadata,
            program_metadata,
            metadata_map,
            auto_copy_metadata,
            stream_sources: StreamSourceRegistry::default(),
            video_disable,
            audio_disable,
            subtitle_disable,
            data_disable,
            pix_fmt,
            sws_opts,
            swr_opts,
            attachments,
        }
    }

    pub(crate) fn register_stream_source(
        &mut self,
        output_stream_index: usize,
        input_file_index: usize,
        input_stream_index: usize,
        encoded: bool,
    ) {
        self.stream_sources.register(
            output_stream_index,
            input_file_index,
            input_stream_index,
            encoded,
        );
    }

    pub(crate) fn stream_input_mapping(&self) -> Vec<(usize, (usize, usize))> {
        self.stream_sources.stream_input_mapping()
    }

    pub(crate) fn encoding_streams(&self) -> Vec<usize> {
        self.stream_sources.encoding_streams()
    }

    pub(crate) fn add_enc_stream(
        &mut self,
        media_type: AVMediaType,
        enc: *const AVCodec,
        src_node: Arc<SchNode>,
        single_stream_direct_input: bool,
    ) -> crate::error::Result<(Sender<FrameBox>, usize)> {
        let (packet_sender, st, stream_index) = self.new_stream(src_node)?;
        let (frame_sender, frame_receiver) = crossbeam_channel::bounded(8);

        let vsync_method = if media_type == AVMediaType::AVMEDIA_TYPE_VIDEO {
            Some(unsafe {
                determine_vsync_method(
                    self.vsync_method,
                    self.framerate,
                    self.framerate_max,
                    self.out_fmt_ctx_ptr(),
                    self.copy_ts,
                    single_stream_direct_input,
                )?
            })
        } else {
            None
        };

        let qscale = if media_type == AVMediaType::AVMEDIA_TYPE_VIDEO {
            self.video_qscale
        } else if media_type == AVMediaType::AVMEDIA_TYPE_AUDIO {
            self.audio_qscale
        } else {
            None
        };

        // Forced-keyframe times apply to re-encoded video only; audio and subtitle
        // encoders receive an empty list, so the request never reaches them.
        let forced_kf_pts = if media_type == AVMediaType::AVMEDIA_TYPE_VIDEO {
            self.forced_kf_pts.clone().unwrap_or_default()
        } else {
            Vec::new()
        };

        // Pre-mux buffer: packets an encoder produces before the muxer opens
        // (it waits until every output stream has emitted a first packet).
        // Byte-metered with FFmpeg's max_muxing_queue_size /
        // muxing_queue_data_threshold semantics (PERF-12): below the byte
        // threshold the packet cap does not apply, so sparse small-packet
        // streams keep parking, while high-bitrate streams are bounded by
        // bytes instead of the old blind 65536-packet window that could park
        // gigabytes. The bound still doubles as the demux read-ahead window
        // before backpressure stalls the demuxer; a job that needs a larger
        // window (e.g. a sparse subtitle stream whose first packet lands deep
        // into a high-bitrate file) parks ~60s and then fails with an error
        // naming the Output knobs that raise it — the same remedy FFmpeg CLI
        // prescribes for "Too many packets buffered for output stream".
        let (pre_packet_sender, pre_packet_receiver) =
            pre_mux_queue::channel(self.pre_mux_queue_config);
        self.src_pre_receivers.push(pre_packet_receiver);

        let stream = EncoderStream::new(
            stream_index,
            st,
            media_type,
            enc,
            vsync_method,
            qscale,
            forced_kf_pts,
            frame_receiver,
            packet_sender,
            pre_packet_sender,
            self.mux_start_gate.clone(),
        );
        self.streams.push(stream);
        Ok((frame_sender, stream_index))
    }

    pub(crate) fn new_stream(
        &mut self,
        src: Arc<SchNode>,
    ) -> crate::error::Result<(Sender<PacketBox>, *mut AVStream, usize)> {
        let packet_sender = match &self.queue {
            None => {
                let (packet_sender, packet_receiver) = crossbeam_channel::bounded(8);
                self.queue = Some((packet_sender.clone(), packet_receiver));
                packet_sender
            }
            Some((packet_sender, _packet_receiver)) => packet_sender.clone(),
        };

        let index = self.nb_streams;
        self.mux_stream_nodes.insert(
            index,
            Arc::new(SchNode::MuxStream {
                src,
                last_dts: Arc::new(AtomicI64::new(0)),
                source_finished: Arc::new(AtomicBool::new(false)),
            }),
        );

        self.nb_streams += 1;
        unsafe {
            let st = avformat_new_stream(self.out_fmt_ctx_ptr(), null());
            if st.is_null() {
                return Err(OpenOutputError::OutOfMemory.into());
            }
            Ok((packet_sender, st, index))
        }
    }

    /// Streamcopy analog of the stream wiring inside `add_enc_stream`: allocate
    /// the output stream and the shared live packet sender via `new_stream`,
    /// then build this stream's own pre-mux queue and register its receiver in
    /// `src_pre_receivers` — exactly like the encoder path (`add_enc_stream`
    /// ~L444-446). Returns the live sender, the pre-mux sender, and the shared
    /// `MuxStartGate` so the demux can route copy packets through the same
    /// deferred-start gate the encoders use: fftools sends demux->mux packets
    /// through the same `sch_send` path as encoders (ffmpeg_sched.c:2038-2077),
    /// so copy and encode park pre-start and drain in DTS order uniformly
    /// instead of a copy stream blocking the shared demuxer on the live queue.
    pub(crate) fn new_copy_stream(
        &mut self,
        src: Arc<SchNode>,
    ) -> crate::error::Result<(
        Sender<PacketBox>,
        PreMuxQueueSender,
        Arc<crate::core::context::MuxStartGate>,
        *mut AVStream,
        usize,
    )> {
        let (packet_sender, st, index) = self.new_stream(src)?;
        let (pre_sender, pre_receiver) = pre_mux_queue::channel(self.pre_mux_queue_config);
        self.src_pre_receivers.push(pre_receiver);
        Ok((
            packet_sender,
            pre_sender,
            self.mux_start_gate.clone(),
            st,
            index,
        ))
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.nb_streams == self.nb_streams_ready.load(Ordering::Acquire)
    }

    pub(crate) fn stream_ready(&self) {
        self.nb_streams_ready.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn stream_count(&self) -> usize {
        self.nb_streams
    }

    /// This output stream's `source_finished` flag (`SchNode::MuxStream`), cloned
    /// so the demux can observe an `sq_mux` cascade-finish (Architecture Y').
    /// `None` if the index is out of range or the node is not a mux stream.
    pub(crate) fn stream_source_finished(
        &self,
        output_stream_index: usize,
    ) -> Option<Arc<AtomicBool>> {
        match self.mux_stream_nodes.get(output_stream_index)?.as_ref() {
            SchNode::MuxStream {
                source_finished, ..
            } => Some(source_finished.clone()),
            _ => None,
        }
    }

    /// Per-media-type frame cap (`-frames:v/a/s N`), as an `sq_limit_frames` bound.
    fn max_frames_for_type(&self, media_type: AVMediaType) -> Option<u64> {
        let v = match media_type {
            AVMediaType::AVMEDIA_TYPE_VIDEO => self.max_video_frames,
            AVMediaType::AVMEDIA_TYPE_AUDIO => self.max_audio_frames,
            AVMediaType::AVMEDIA_TYPE_SUBTITLE => self.max_subtitle_frames,
            _ => None,
        };
        v.and_then(|n| if n >= 0 { Some(n as u64) } else { None })
    }

    /// Compute the `-shortest` packet sync-queue plan (mirrors the `sq_mux`
    /// branch of `setup_sync_queues`, `ffmpeg_mux_init.c:2114-2138`). Built only
    /// when `shortest && nb_interleaved > 1 && nb_interleaved > nb_av_enc`, i.e.
    /// there is at least one interleaved stream that is NOT an encoded A/V stream
    /// (a copy / subtitle / data follower) to truncate. Every interleaved
    /// (non-attachment) stream is a member; under `-shortest` all are limiting.
    ///
    /// MUST be called while `out_fmt_ctx` is still owned (before the worker takes
    /// it) — it reads each output stream's media type from the output context.
    pub(crate) fn sq_mux_plan(&self) -> Option<SqMuxPlan> {
        if !self.shortest {
            return None;
        }
        let stream_count = self.stream_count();
        if stream_count < 2 {
            return None;
        }
        let oc = self.out_fmt_ctx_ptr();
        if oc.is_null() {
            return None;
        }

        // Encoded A/V streams: the `EncoderStream` list holds exactly the streams
        // with an encoder; A/V among them are `nb_av_enc` (IS_AV_ENC).
        let mut is_av_enc = vec![false; stream_count];
        for s in &self.streams {
            if matches!(
                s.codec_type,
                AVMediaType::AVMEDIA_TYPE_VIDEO | AVMediaType::AVMEDIA_TYPE_AUDIO
            ) && s.stream_index < stream_count
            {
                is_av_enc[s.stream_index] = true;
            }
        }

        // Media type per output stream index (set at stream creation).
        let types: Vec<AVMediaType> = (0..stream_count)
            .map(|i| unsafe {
                let st = *(*oc).streams.add(i);
                (*(*st).codecpar).codec_type
            })
            .collect();

        let is_interleaved = |t: AVMediaType| t != AVMediaType::AVMEDIA_TYPE_ATTACHMENT;
        let nb_interleaved = types.iter().filter(|&&t| is_interleaved(t)).count();
        let nb_av_enc = is_av_enc.iter().filter(|&&b| b).count();

        // FFmpeg gate (shortest trigger): the outer `nb_interleaved > 1 &&
        // shortest` plus the sq_mux-specific `nb_interleaved > nb_av_enc`.
        if !(nb_interleaved > 1 && nb_interleaved > nb_av_enc) {
            return None;
        }

        let mut streams = Vec::with_capacity(nb_interleaved);
        for i in 0..stream_count {
            if !is_interleaved(types[i]) {
                continue;
            }
            // limiting = shortest || max_frames < MAX; shortest is true here.
            streams.push((i, true, self.max_frames_for_type(types[i])));
        }

        Some(SqMuxPlan {
            buf_size_us: self.shortest_buf_duration_us,
            streams,
        })
    }

    pub(crate) fn has_src(&self) -> bool {
        self.queue.is_some()
    }

    pub(crate) fn take_queue(&mut self) -> Option<(Sender<PacketBox>, Receiver<PacketBox>)> {
        self.queue.take()
    }

    pub(crate) fn take_src_pre_recvs(&mut self) -> Vec<PreMuxQueueReceiver> {
        std::mem::take(&mut self.src_pre_receivers)
    }

    pub(crate) fn get_streams(&self) -> &Vec<EncoderStream> {
        &self.streams
    }

    pub(crate) fn get_streams_mut(&mut self) -> &mut Vec<EncoderStream> {
        &mut self.streams
    }

    pub(crate) fn take_streams_mut(&mut self) -> Vec<EncoderStream> {
        std::mem::take(&mut self.streams)
    }

    pub(crate) fn mux_start_gate(&self) -> Arc<crate::core::context::MuxStartGate> {
        self.mux_start_gate.clone()
    }

    pub(crate) fn enc_handle_sender(&self) -> Sender<std::thread::JoinHandle<()>> {
        self.enc_handles.0.clone()
    }

    pub(crate) fn enc_handle_receiver(&self) -> Receiver<std::thread::JoinHandle<()>> {
        self.enc_handles.1.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::StreamSourceRegistry;

    #[test]
    fn test_stream_source_registry() {
        let mut registry = StreamSourceRegistry::default();
        registry.register(2, 0, 1, false);
        registry.register(0, 1, 3, true);

        assert_eq!(
            registry.stream_input_mapping(),
            vec![(0, (1, 3)), (2, (0, 1)),]
        );

        assert_eq!(registry.encoding_streams(), vec![0]);

        registry.register(0, 1, 3, false);
        assert!(registry.encoding_streams().is_empty());
    }
}

unsafe fn determine_vsync_method(
    vsync_method: VSyncMethod,
    framerate: Option<AVRational>,
    framerate_max: Option<AVRational>,
    out_fmt_ctx: *mut AVFormatContext,
    copy_ts: bool,
    single_stream_direct_input: bool,
) -> crate::error::Result<VSyncMethod> {
    // A frame rate or cap only acts through CFR-style conversion; combining
    // one with an explicit non-CFR mode is contradictory
    // (ffmpeg_mux_init.c:798-804).
    if (framerate.is_some_and(|fr| fr.num != 0) || framerate_max.is_some_and(|fr| fr.num != 0))
        && !matches!(
            vsync_method,
            VSyncMethod::VsyncAuto | VSyncMethod::VsyncCfr | VSyncMethod::VsyncVscfr
        )
    {
        error!(
            "One of framerate/framerate_max was specified together a non-CFR \
             vsync method. This is contradictory."
        );
        return Err(crate::error::Error::OpenOutput(
            OpenOutputError::InvalidArgument,
        ));
    }

    if vsync_method != VSyncMethod::VsyncAuto {
        return Ok(vsync_method);
    }

    // 1. -r or -fpsmax both force CFR (ffmpeg_mux_init.c:807-808)
    let mut vsync_method =
        if framerate.is_some_and(|fr| fr.num != 0) || framerate_max.is_some_and(|fr| fr.num != 0) {
            VSyncMethod::VsyncCfr
        }
        // 2. If output format is "avi", set VSYNC_VFR
        else if match CStr::from_ptr((*(*out_fmt_ctx).oformat).name).to_str() {
            Ok(s) => s == "avi",
            Err(_) => false,
        } {
            VSyncMethod::VsyncVfr
        }
        // 3. Otherwise, check the format flags
        else {
            let oformat = (*out_fmt_ctx).oformat;
            if (*oformat).flags & AVFMT_VARIABLE_FPS != 0 {
                if (*oformat).flags & AVFMT_NOTIMESTAMPS != 0 {
                    VSyncMethod::VsyncPassthrough
                } else {
                    VSyncMethod::VsyncVfr
                }
            } else {
                VSyncMethod::VsyncCfr
            }
        };

    // 4. A stream fed directly by a single-stream input keeps its original
    // grid (ffmpeg_mux_init.c:817-822; input_ts_offset is always 0 here
    // since -itsoffset is not supported).
    if vsync_method == VSyncMethod::VsyncCfr && single_stream_direct_input {
        vsync_method = VSyncMethod::VsyncVscfr;
    }

    // 5. If input stream exists and VSYNC_CFR is selected, check additional conditions
    if vsync_method == VSyncMethod::VsyncCfr && copy_ts {
        vsync_method = VSyncMethod::VsyncVscfr;
    }

    Ok(vsync_method)
}
