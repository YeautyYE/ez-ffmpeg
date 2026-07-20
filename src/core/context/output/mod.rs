use crate::filter::frame_pipeline::FramePipeline;
use ffmpeg_sys_next::AVRational;
use std::collections::HashMap;

mod attachment;
mod bsf;
mod codec_opts;
mod metadata;

pub(crate) use attachment::AttachmentSpec;

// Note: Output is Send if all callback fields are Send.
// We require `+ Send` on callback types to ensure this.
// Output is !Sync because FnMut callbacks require exclusive access.

/// Where an [`Output`]'s encoded data goes — exactly one of a URL/path, a
/// custom byte-write callback, or a packet sink. One typed discriminant
/// instead of correlated `Option` fields, so the build path selects the
/// context/RAII mode by variant rather than inferring it from "no URL means
/// custom AVIO".
pub(crate) enum OutputTarget {
    /// A file path or URL (e.g. `output.mp4`, `rtmp://...`); FFmpeg opens and
    /// writes it at runtime mux initialization.
    Url(String),
    /// A custom byte sink: the muxed container bytes are handed to this
    /// write callback through a custom AVIO context.
    ///
    /// The callback receives a buffer of encoded container bytes and returns
    /// the number of bytes written, or a negative `AVERROR` value (e.g.
    /// `ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)`) on failure.
    CustomIo {
        write: Box<dyn FnMut(&[u8]) -> i32 + Send>,
    },
    /// A packet sink: encoded packets are delivered to callbacks and no
    /// container is written (see [`crate::packet_sink`]).
    PacketSink(crate::core::packet_sink::PacketSink),
    /// The target was moved into the muxer when the context was built. An
    /// `Output` is single-use; this state only exists after `build()`.
    Consumed,
}

pub struct Output {
    /// The output destination (URL, custom byte sink, or packet sink).
    /// Moved out (leaving [`OutputTarget::Consumed`]) when the context is
    /// built.
    pub(crate) target: OutputTarget,

    /// Size of the AVIO buffer backing a custom `write_callback`, in bytes.
    /// Only used when the output is a callback (no URL). Larger values reduce
    /// Rust↔FFmpeg round-trips for sequential/network sinks; unset means
    /// [`DEFAULT_CUSTOM_IO_BUFFER_SIZE`](crate::core::context::DEFAULT_CUSTOM_IO_BUFFER_SIZE)
    /// (64 KiB). `Some` records that [`Output::set_io_buffer_size`] was
    /// called — packet-sink validation must distinguish "set to the default
    /// value" from "never set".
    pub(crate) io_buffer_size: Option<usize>,

    /// FFmpeg `-max_muxing_queue_size` parity: per-stream packet cap for the
    /// pre-mux queue, applied only once
    /// [`muxing_queue_data_threshold`](Output::set_muxing_queue_data_threshold)
    /// is exceeded. Default 128. Set via [`Output::set_max_muxing_queue_size`].
    pub(crate) max_muxing_queue_size: usize,

    /// FFmpeg `-muxing_queue_data_threshold` parity: parked payload bytes per
    /// stream below which the packet cap does not apply. Default 50 MiB. Set
    /// via [`Output::set_muxing_queue_data_threshold`].
    pub(crate) muxing_queue_data_threshold: usize,

    /// A callback function for custom seeking within the output stream.
    ///
    /// The `seek_callback` function allows custom logic for adjusting the write position in
    /// the output stream. This is essential for formats that require seeking, such as `mp4`
    /// and `mkv`, where metadata or index information must be updated at specific positions.
    ///
    /// If the output format requires seeking but no `seek_callback` is provided, the operation
    /// may fail, resulting in errors such as:
    /// ```text
    /// [mp4 @ 0x...] muxer does not support non seekable output
    /// ```
    ///
    /// **FFmpeg may invoke `seek_callback` from different threads, so thread safety is required.**
    /// If the destination is a `File`, **wrap it in `Arc<Mutex<File>>`** to ensure safe access.
    ///
    /// ### Parameters:
    /// - `offset: i64`: The target position in the output stream where seeking should occur.
    /// - `whence: i32`: The seek mode, which determines how `offset` should be interpreted:
    ///   - `ffmpeg_sys_next::SEEK_SET` (0) - Seek to an absolute position.
    ///   - `ffmpeg_sys_next::SEEK_CUR` (1) - Seek relative to the current position.
    ///   - `ffmpeg_sys_next::SEEK_END` (2) - Seek relative to the end of the output.
    ///   - `ffmpeg_sys_next::AVSEEK_SIZE` (65536) - Query the **total size** of the stream
    ///     instead of seeking.
    ///
    ///   `avio_seek` strips `ffmpeg_sys_next::AVSEEK_FORCE` (131072) from `whence` before
    ///   invoking a custom callback; the example masks it anyway as cheap defense. No
    ///   other `whence` values reach a custom seek callback.
    ///
    /// ### Return Value:
    /// - **Positive Value**: The new offset position after seeking.
    /// - **Negative Value**: An error occurred. Common errors include:
    ///   - `ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE)`: Seek is not supported.
    ///   - `ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)`: General I/O error.
    ///
    /// ### Example (Thread-safe seek callback using `Arc<Mutex<File>>`):
    /// Since `FFmpeg` may call `write_callback` and `seek_callback` from different threads,
    /// **use `Arc<Mutex<File>>` to ensure safe concurrent access.**
    ///
    /// ```rust,ignore
    /// use std::fs::File;
    /// use std::io::{Seek, SeekFrom};
    /// use std::sync::{Arc, Mutex};
    ///
    /// let file = Arc::new(Mutex::new(File::create("output.mp4").expect("Failed to create file")));
    ///
    /// let seek_callback = {
    ///     let file = Arc::clone(&file);
    ///     Box::new(move |offset: i64, whence: i32| -> i64 {
    ///         let mut file = file.lock().unwrap();
    ///
    ///         // ✅ Handle AVSEEK_SIZE: FFmpeg asks for the total stream size instead of seeking
    ///         if whence == ffmpeg_sys_next::AVSEEK_SIZE {
    ///             if let Ok(size) = file.metadata().map(|m| m.len() as i64) {
    ///                 return size;
    ///             }
    ///             return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
    ///         }
    ///
    ///         // ✅ Defensive: mask AVSEEK_FORCE (avio_seek strips it before a custom
    ///         // callback). The AVIO layer sends no other whence values (lseek extensions
    ///         // like SEEK_HOLE/SEEK_DATA never reach a custom callback).
    ///         let seek_result = match whence & !ffmpeg_sys_next::AVSEEK_FORCE {
    ///             ffmpeg_sys_next::SEEK_SET => file.seek(SeekFrom::Start(offset as u64)),
    ///             ffmpeg_sys_next::SEEK_CUR => file.seek(SeekFrom::Current(offset)),
    ///             ffmpeg_sys_next::SEEK_END => file.seek(SeekFrom::End(offset)),
    ///             _ => {
    ///                 println!("Unsupported seek mode: {}", whence);
    ///                 return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64;
    ///             }
    ///         };
    ///
    ///         match seek_result {
    ///             Ok(new_pos) => {
    ///                 println!("Seek successful, new position: {}", new_pos);
    ///                 new_pos as i64
    ///             }
    ///             Err(e) => {
    ///                 println!("Seek failed: {}", e);
    ///                 ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64
    ///             }
    ///         }
    ///     })
    /// };
    /// ```
    pub(crate) seek_callback: Option<Box<dyn FnMut(i64, i32) -> i64 + Send>>,

    /// A pipeline specifying how frames will be processed **before encoding**.
    ///
    /// Once input data is decoded into [`Frame`]s, these frames pass through
    /// this pipeline on their way to the encoder. The pipeline is composed of
    /// one or more [`FrameFilter`]s, each providing a specific transformation,
    /// effect, or filter (e.g., resizing, color correction, OpenGL shader
    /// effects, etc.).
    ///
    /// If set to [`None`], no additional processing is applied — frames
    /// are sent to the encoder as they are.
    pub(crate) frame_pipelines: Option<Vec<FramePipeline>>,

    /// Unparsed stream map specifications (user input stage)
    /// These get parsed and expanded into stream_maps during outputs_bind()
    pub(crate) stream_map_specs: Vec<StreamMapSpec>,

    /// Expanded stream maps (FFmpeg-compatible, ready for use)
    /// Each entry maps exactly one input stream to one output stream
    pub(crate) stream_maps: Vec<StreamMap>,

    /// The output format for the container.
    ///
    /// This field specifies the desired output format, such as `mp4`, `flv`, or `mkv`. If `None`, FFmpeg
    /// will attempt to automatically detect the format based on the output URL or filename extension.
    ///
    /// The format can be specified explicitly for scenarios where the format detection is insufficient or
    /// where you want to force a particular container format regardless of the URL or extension.
    pub(crate) format: Option<String>,

    /// The codec to be used for **video** encoding.
    ///
    /// If this field is `None`, FFmpeg will try to select an appropriate video codec based on the
    /// output format or other settings. By setting this field to a specific codec (e.g., `"h264"`, `"hevc"`, etc.),
    /// you can override FFmpeg’s default codec selection. If the specified codec is not available
    /// in your FFmpeg build, an error will be returned during initialization.
    pub(crate) video_codec: Option<String>,

    /// The codec to be used for **audio** encoding.
    ///
    /// If this field is `None`, FFmpeg will try to select an appropriate audio codec based on the
    /// output format or other settings. By providing a value (e.g., `"aac"`, `"mp3"`, etc.),
    /// you override FFmpeg’s default codec choice. If the specified codec is not available
    /// in your FFmpeg build, an error will be returned during initialization.
    pub(crate) audio_codec: Option<String>,

    /// The codec to be used for **subtitle** encoding.
    ///
    /// If this field is `None`, FFmpeg will try to select an appropriate subtitle codec based on
    /// the output format or other settings. Setting this field (e.g., `"mov_text"` for MP4 subtitles)
    /// forces FFmpeg to use the specified codec. If the chosen codec is not supported by your build of FFmpeg,
    /// an error will be returned during initialization.
    pub(crate) subtitle_codec: Option<String>,

    /// Bitstream-filter chain applied to the **video** output stream(s),
    /// equivalent to FFmpeg `-bsf:v`. See [`set_video_bsf`](Self::set_video_bsf).
    pub(crate) video_bsf: Option<String>,

    /// Bitstream-filter chain applied to the **audio** output stream(s),
    /// equivalent to FFmpeg `-bsf:a`. See [`set_audio_bsf`](Self::set_audio_bsf).
    pub(crate) audio_bsf: Option<String>,

    /// Bitstream-filter chain applied to the **subtitle** output stream(s),
    /// equivalent to FFmpeg `-bsf:s`. See [`set_subtitle_bsf`](Self::set_subtitle_bsf).
    pub(crate) subtitle_bsf: Option<String>,
    pub(crate) start_time_us: Option<i64>,
    pub(crate) recording_time_us: Option<i64>,
    pub(crate) stop_time_us: Option<i64>,
    /// FFmpeg `-shortest`: finish the output when its shortest limiting stream
    /// ends. Encoded audio/video truncate at the frame level (sq_enc, no B-frame
    /// stranding); copy/subtitle/data truncate at the packet level (sq_mux).
    /// Default `false`. Set via [`Output::set_shortest`].
    pub(crate) shortest: bool,
    /// FFmpeg `-shortest_buf_duration` (seconds upstream, microseconds here): the
    /// maximum time one stream is buffered waiting for a lagging peer before it is
    /// released anyway. Bounds `-shortest` memory and precision. Default 10 s.
    pub(crate) shortest_buf_duration_us: i64,
    pub(crate) framerate: Option<AVRational>,
    /// Maximum output frame rate cap (`-fpsmax`): the native rate is kept and
    /// only clamped when it exceeds the cap or is unknown
    /// (ffmpeg_mux_init.c ms->max_frame_rate).
    pub(crate) framerate_max: Option<AVRational>,
    pub(crate) vsync_method: VSyncMethod,
    pub(crate) bits_per_raw_sample: Option<i32>,
    pub(crate) audio_sample_rate: Option<i32>,
    pub(crate) audio_channels: Option<i32>,
    /// FFmpeg sample format name (e.g. `"s16"`), resolved to an
    /// `AVSampleFormat` at open time like `pix_fmt`.
    pub(crate) audio_sample_fmt: Option<String>,

    // -q:v
    // use fixed quality scale (VBR)
    pub(crate) video_qscale: Option<i32>,

    // -q:a
    // set audio quality (codec-specific)
    pub(crate) audio_qscale: Option<i32>,

    /// Raw forced-keyframe spec (FFmpeg `-force_key_frames` list form), as
    /// given to [`Output::set_force_key_frames`]. `None` = feature off.
    /// Parsed and validated at open time (`parse_forced_key_frames`), like
    /// every other deferred option; applies to re-encoded video only.
    pub(crate) forced_kf_spec: Option<String>,

    /// Maximum number of **video** frames to encode (equivalent to `-frames:v` in FFmpeg).
    ///
    /// This option limits the number of **video** frames processed by the encoder.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -frames:v 100 output.mp4
    /// ```
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_max_video_frames(300);
    /// ```
    pub(crate) max_video_frames: Option<i64>,

    /// Maximum number of **audio** frames to encode (equivalent to `-frames:a` in FFmpeg).
    ///
    /// This option limits the number of **audio** frames processed by the encoder.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -frames:a 500 output.mp4
    /// ```
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_max_audio_frames(500);
    /// ```
    pub(crate) max_audio_frames: Option<i64>,

    /// Maximum number of **subtitle** frames to encode (equivalent to `-frames:s` in FFmpeg).
    ///
    /// This option limits the number of **subtitle** frames processed by the encoder.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -frames:s 200 output.mp4
    /// ```
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_max_subtitle_frames(200);
    /// ```
    pub(crate) max_subtitle_frames: Option<i64>,

    /// Video encoder-specific options.
    ///
    /// This field stores key-value pairs for configuring the **video encoder**.
    /// These options are passed to the video encoder before encoding begins.
    ///
    /// **Common Examples:**
    /// - `crf=0` (for lossless quality in x264/x265)
    /// - `preset=ultrafast` (for faster encoding speed in H.264)
    /// - `tune=zerolatency` (for real-time streaming)
    pub(crate) video_codec_opts: Option<HashMap<String, String>>,

    /// Audio encoder-specific options.
    ///
    /// This field stores key-value pairs for configuring the **audio encoder**.
    /// These options are passed to the audio encoder before encoding begins.
    ///
    /// **Common Examples:**
    /// - `b=192k` (for setting bitrate in AAC/MP3)
    /// - `ar=44100` (for setting sample rate)
    pub(crate) audio_codec_opts: Option<HashMap<String, String>>,

    /// Subtitle encoder-specific options.
    ///
    /// This field stores key-value pairs for configuring the **subtitle encoder**.
    /// These options are passed to the subtitle encoder before encoding begins.
    ///
    /// **Common Examples:**
    /// - `mov_text` (for MP4 subtitles)
    /// - `srt` (for subtitle format)
    pub(crate) subtitle_codec_opts: Option<HashMap<String, String>>,

    /// The output format options for the container.
    ///
    /// This field stores additional format-specific options that are passed to the FFmpeg muxer.
    /// It is a collection of key-value pairs that can modify the behavior of the output format.
    ///
    /// Common examples include:
    /// - `movflags=faststart` (for MP4 files)
    /// - `flvflags=no_duration_filesize` (for FLV files)
    ///
    /// These options are used when initializing the FFmpeg output format.
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_format_opt("movflags", "faststart");
    /// ```
    pub(crate) format_opts: Option<HashMap<String, String>>,

    // ========== Metadata Fields ==========
    /// Global metadata for the entire output file
    pub(crate) global_metadata: Option<HashMap<String, String>>,

    /// Stream-specific metadata with stream specifiers
    /// Key: stream specifier string (e.g., "v:0", "a", "s:0")
    /// Value: metadata key-value pairs for matching streams
    /// During output initialization, each specifier is matched against actual streams
    pub(crate) stream_metadata: Vec<(String, String, String)>, // (spec, key, value) tuples

    /// Chapter-specific metadata, indexed by chapter index
    pub(crate) chapter_metadata: HashMap<usize, HashMap<String, String>>,

    /// Program-specific metadata, indexed by program index
    pub(crate) program_metadata: HashMap<usize, HashMap<String, String>>,

    /// Metadata mappings from input files
    pub(crate) metadata_map: Vec<crate::core::metadata::MetadataMapping>,

    /// Whether to automatically copy metadata from input files (default: true)
    /// Replicates FFmpeg's default behavior of copying global and stream metadata
    pub(crate) auto_copy_metadata: bool,

    // ========== Stream Disable Flags (P1 Features) ==========
    /// Disable video stream mapping (equivalent to `-vn` in FFmpeg).
    /// When true, video streams will be excluded from automatic stream mapping.
    pub(crate) video_disable: bool,

    /// Disable audio stream mapping (equivalent to `-an` in FFmpeg).
    /// When true, audio streams will be excluded from automatic stream mapping.
    pub(crate) audio_disable: bool,

    /// Disable subtitle stream mapping (equivalent to `-sn` in FFmpeg).
    /// When true, subtitle streams will be excluded from automatic stream mapping.
    pub(crate) subtitle_disable: bool,

    /// Disable data stream mapping (equivalent to `-dn` in FFmpeg).
    /// When true, data streams will be excluded from automatic stream mapping.
    /// Data streams include things like timed metadata, chapter markers, etc.
    pub(crate) data_disable: bool,

    /// Output pixel format (equivalent to `-pix_fmt` in FFmpeg).
    /// When set, forces the output video to use the specified pixel format.
    /// Only effective when re-encoding (not when using stream copy).
    pub(crate) pix_fmt: Option<String>,

    /// CLI-compat only (crate-internal): the hard simple-filter
    /// prerequisite — when set, context binding fails unless the opened
    /// input carries exactly one video stream. Set by the `cli` feature's
    /// lowering for `-vf` commands; never by the public builder API.
    #[cfg_attr(not(feature = "cli"), allow(dead_code))]
    pub(crate) require_unique_video_source: bool,

    /// CLI-compat strict mode (crate-internal): leftover AVOptions error
    /// instead of warning on every component this output drives (muxer,
    /// encoders). Set only by the `cli` feature's entry points; the default
    /// builder path keeps today's warn behavior.
    pub(crate) strict_avoptions: bool,

    /// Per-output simple **video** filter chain (FFmpeg `-vf`), applied to
    /// this output's re-encoded video stream through the implicit per-output
    /// filtergraph (it replaces the default `null` chain). Must be a linear
    /// chain: exactly one video input pad and one video output pad. `None` ⇒
    /// the passthrough `null` chain. Set via [`Output::set_video_filter`].
    pub(crate) video_filter: Option<String>,

    /// sws (libswscale) options for the `scale` filters libavfilter
    /// auto-inserts ahead of this output's encoder. Maps to the graph-level
    /// `AVFilterGraph.scale_sws_opts`. Default `None`. Set via
    /// [`Output::set_sws_opts`].
    pub(crate) sws_opts: Option<String>,

    /// swr (libswresample) options for the `aresample` filters libavfilter
    /// auto-inserts ahead of this output's encoder. Maps to the graph-level
    /// `AVFilterGraph.aresample_swr_opts`. Default `None`. Set via
    /// [`Output::set_swr_opts`].
    pub(crate) swr_opts: Option<String>,

    /// Files to embed as attachment streams (FFmpeg `-attach`), e.g. fonts or
    /// cover art. Empty ⇒ no attachments and zero behavior change. Each entry
    /// is resolved into an `AVMEDIA_TYPE_ATTACHMENT` stream at output build
    /// time; the file is read then, so a missing/unreadable/empty/oversized
    /// file surfaces as an `Err` from the context build — never a panic.
    pub(crate) attachments: Vec<AttachmentSpec>,
}

#[derive(Copy, Clone, PartialEq)]
#[non_exhaustive]
pub enum VSyncMethod {
    VsyncAuto,
    VsyncCfr,
    VsyncVfr,
    VsyncPassthrough,
    VsyncVscfr,
}

impl Output {
    pub fn new(url: impl Into<String>) -> Self {
        url.into().into()
    }

    /// The destination URL, when this output targets one.
    pub(crate) fn url(&self) -> Option<&str> {
        match &self.target {
            OutputTarget::Url(url) => Some(url),
            _ => None,
        }
    }

    /// The single field-literal constructor every public entry point funnels
    /// through; the target discriminant is the only per-entry difference.
    fn with_target(target: OutputTarget) -> Self {
        Self {
            target,
            io_buffer_size: None,
            max_muxing_queue_size: crate::core::context::pre_mux_queue::DEFAULT_PRE_MUX_MAX_PACKETS,
            muxing_queue_data_threshold:
                crate::core::context::pre_mux_queue::DEFAULT_PRE_MUX_DATA_THRESHOLD,
            seek_callback: None,
            frame_pipelines: None,
            stream_map_specs: vec![],
            stream_maps: vec![],
            format: None,
            video_codec: None,
            audio_codec: None,
            subtitle_codec: None,
            video_bsf: None,
            audio_bsf: None,
            subtitle_bsf: None,
            start_time_us: None,
            recording_time_us: None,
            stop_time_us: None,
            framerate: None,
            framerate_max: None,
            vsync_method: VSyncMethod::VsyncAuto,
            bits_per_raw_sample: None,
            audio_sample_rate: None,
            audio_channels: None,
            audio_sample_fmt: None,
            video_qscale: None,
            audio_qscale: None,
            forced_kf_spec: None,
            max_video_frames: None,
            max_audio_frames: None,
            max_subtitle_frames: None,
            video_codec_opts: None,
            audio_codec_opts: None,
            subtitle_codec_opts: None,
            format_opts: None,
            global_metadata: None,
            stream_metadata: Vec::new(),
            chapter_metadata: HashMap::new(),
            program_metadata: HashMap::new(),
            metadata_map: Vec::new(),
            auto_copy_metadata: true, // FFmpeg default: auto-copy enabled
            video_disable: false,
            audio_disable: false,
            subtitle_disable: false,
            data_disable: false,
            pix_fmt: None,
            require_unique_video_source: false,
            strict_avoptions: false,
            video_filter: None,
            sws_opts: None,
            swr_opts: None,
            attachments: Vec::new(),
            shortest: false,
            shortest_buf_duration_us: 10_000_000,
        }
    }

    /// Creates a new `Output` instance with a custom write callback and format string.
    ///
    /// This method initializes an `Output` object that uses a provided `write_callback` function
    /// to handle the encoded data being written to the output stream. You can optionally specify
    /// the desired output format via the `format` method.
    ///
    /// ### Parameters:
    /// - `write_callback: fn(buf: &[u8]) -> i32`: A function that processes the provided buffer of
    ///   encoded data and writes it to the destination. The function should return the number of bytes
    ///   successfully written (positive value) or a negative value in case of error.
    ///
    /// ### Return Value:
    /// - Returns a new `Output` instance configured with the specified `write_callback` function.
    ///
    /// ### Behavior of `write_callback`:
    /// - **Positive Value**: Indicates the number of bytes successfully written.
    /// - **Negative Value**: Indicates an error occurred. For example:
    ///   - `ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)`: Represents an input/output error.
    ///   - Other custom-defined error codes can also be returned to signal specific issues.
    ///
    /// ### Example:
    /// ```rust,ignore
    /// let output = Output::new_by_write_callback(move |buf| {
    ///     println!("Processing {} bytes of data for output", buf.len());
    ///     buf.len() as i32 // Return the number of bytes processed
    /// })
    /// .set_format("mp4");
    /// ```
    pub fn new_by_write_callback<F>(write_callback: F) -> Self
    where
        F: FnMut(&[u8]) -> i32 + Send + 'static,
    {
        (Box::new(write_callback) as Box<dyn FnMut(&[u8]) -> i32 + Send>).into()
    }

    /// Creates an `Output` that delivers **encoded packets** to the given
    /// [`PacketSink`](crate::packet_sink::PacketSink) callbacks instead of
    /// muxing them into container bytes.
    ///
    /// No container is written and no I/O happens: `on_stream_info` fires
    /// once with the finalized stream configuration (valid avcC for H.264,
    /// AudioSpecificConfig for AAC), then each encoded packet is handed to
    /// `on_packet` as a borrowed [`PacketView`](crate::packet_sink::PacketView).
    /// See the [`packet_sink`](crate::packet_sink) module docs for the strict
    /// tier contract, the callback order, and the **blocking backpressure**
    /// behavior (a slow callback stalls the pipeline; nothing is dropped).
    ///
    /// Muxer-only options are rejected when the context is built:
    /// `set_format`, `set_seek_callback`, `set_io_buffer_size`,
    /// `set_format_opt(s)`, bitstream filters (`set_*_bsf`), attachments and
    /// stream copy all fail with a typed
    /// [`PacketSinkError`](crate::error::PacketSinkError). The v1 strict tier
    /// accepts only whitelisted encoders (video: `libx264`; audio: AAC).
    ///
    /// `Output::from(sink)` is the equivalent, crate-conventional spelling
    /// and the one used throughout the documentation.
    ///
    /// ### Example
    /// ```rust,no_run
    /// use ez_ffmpeg::packet_sink::PacketSink;
    /// use ez_ffmpeg::Output;
    ///
    /// let sink = PacketSink::builder(|packet| {
    ///     println!("stream {} pts {}", packet.stream_index(), packet.pts());
    ///     Ok(())
    /// })
    /// .build();
    /// let output = Output::from(sink).set_video_codec("libx264");
    /// ```
    pub fn new_by_packet_sink(sink: crate::core::packet_sink::PacketSink) -> Self {
        sink.into()
    }

    /// Sets the AVIO buffer size, in bytes, for a custom `write_callback` output.
    ///
    /// FFmpeg hands one buffer-sized chunk per callback, so a larger buffer means
    /// fewer Rust↔FFmpeg round-trips for sequential or network sinks. Only applies
    /// when the output is a callback (no URL); ignored otherwise. The default is
    /// 64 KiB, which keeps first-packet latency low for live use.
    ///
    /// # Errors
    /// The value is validated when the context is built:
    /// `FfmpegContext::builder().build()` fails with
    /// [`OpenOutputError::InvalidOption`](crate::error::OpenOutputError::InvalidOption)
    /// if `size` is 0 or exceeds `i32::MAX` (FFmpeg's `avio_alloc_context`
    /// takes an `int` buffer size).
    pub fn set_io_buffer_size(mut self, size: usize) -> Self {
        self.io_buffer_size = Some(size);
        self
    }

    /// Sets the per-stream packet cap of the pre-mux queue (FFmpeg
    /// `-max_muxing_queue_size` parity; default 128).
    ///
    /// Until the muxer starts (it waits for every mapped output stream to
    /// become ready), each encoder parks its packets in a per-stream queue.
    /// The cap only applies once the queue's byte threshold
    /// ([`set_muxing_queue_data_threshold`](Output::set_muxing_queue_data_threshold))
    /// is exceeded — below it, packet count is unlimited. Raise this (or the
    /// byte threshold) if a job fails with a pre-mux backpressure error, e.g.
    /// a sparse subtitle stream whose first packet lands deep into a
    /// high-bitrate file.
    ///
    /// # Errors
    /// Validated when the context is built: `0` fails with
    /// [`OpenOutputError::InvalidOption`](crate::error::OpenOutputError::InvalidOption).
    pub fn set_max_muxing_queue_size(mut self, size: usize) -> Self {
        self.max_muxing_queue_size = size;
        self
    }

    /// Sets the per-stream byte threshold below which the pre-mux queue's
    /// packet cap does not apply (FFmpeg `-muxing_queue_data_threshold`
    /// parity; default 50 MiB).
    ///
    /// This is a trigger, not a hard byte cap: below the threshold the packet
    /// count is unbounded, and above it admission stops at
    /// [`max_muxing_queue_size`](Output::set_max_muxing_queue_size). Together
    /// they bound how much a fast encoder parks before the muxer starts, which
    /// doubles as the demux read-ahead window: jobs that must read further
    /// ahead (late first packet on one mapped stream) need a larger threshold
    /// (and/or packet cap).
    ///
    /// # Errors
    /// Validated when the context is built: `0` fails with
    /// [`OpenOutputError::InvalidOption`](crate::error::OpenOutputError::InvalidOption).
    pub fn set_muxing_queue_data_threshold(mut self, bytes: usize) -> Self {
        self.muxing_queue_data_threshold = bytes;
        self
    }

    /// Sets a custom seek callback for the output stream.
    ///
    /// This function assigns a user-defined function that handles seeking within the output stream.
    /// Seeking is required for certain formats (e.g., `mp4`, `mkv`) where metadata or index information
    /// needs to be updated at specific positions in the file.
    ///
    /// **Why is `seek_callback` necessary?**
    /// - Some formats (e.g., MP4) require `seek` operations to update metadata (`moov`, `mdat`).
    /// - If no `seek_callback` is provided for formats that require seeking, FFmpeg will fail with:
    ///   ```text
    ///   [mp4 @ 0x...] muxer does not support non seekable output
    ///   ```
    /// - For streaming formats (`flv`, `ts`, `rtmp`, `hls`), seeking is **not required**.
    ///
    /// **FFmpeg may invoke `seek_callback` from different threads.**
    /// - If using a `File` as the output, **wrap it in `Arc<Mutex<File>>`** to ensure thread-safe access.
    ///
    /// ### Parameters:
    /// - `seek_callback: FnMut(i64, i32) -> i64`
    ///   - `offset: i64`: The target seek position in the stream.
    ///   - `whence: i32`: The seek mode determining how `offset` should be interpreted:
    ///     - `ffmpeg_sys_next::SEEK_SET` (0): Seek to an absolute position.
    ///     - `ffmpeg_sys_next::SEEK_CUR` (1): Seek relative to the current position.
    ///     - `ffmpeg_sys_next::SEEK_END` (2): Seek relative to the end of the output.
    ///     - `ffmpeg_sys_next::AVSEEK_SIZE` (65536): Query the **total size** of the stream
    ///       instead of seeking.
    ///
    ///     `avio_seek` strips `ffmpeg_sys_next::AVSEEK_FORCE` (131072) from `whence` before
    ///     invoking a custom callback; the example masks it anyway as cheap defense. No
    ///     other `whence` values reach a custom seek callback.
    ///
    /// ### Return Value:
    /// - **Positive Value**: The new offset position after seeking.
    /// - **Negative Value**: An error occurred. Common errors include:
    ///   - `ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE)`: Seek is not supported.
    ///   - `ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)`: General I/O error.
    ///
    /// ### Example (Thread-safe seek callback using `Arc<Mutex<File>>`):
    /// Since `FFmpeg` may call `write_callback` and `seek_callback` from different threads,
    /// **use `Arc<Mutex<File>>` to ensure safe concurrent access.**
    ///
    /// ```rust,no_run
    /// use ez_ffmpeg::Output;
    /// use std::fs::File;
    /// use std::io::{Seek, SeekFrom, Write};
    /// use std::sync::{Arc, Mutex};
    ///
    /// // ✅ Create a thread-safe file handle
    /// let file = Arc::new(Mutex::new(File::create("output.mp4").expect("Failed to create file")));
    ///
    /// // ✅ Define the write callback (data writing logic)
    /// let write_callback = {
    ///     let file = Arc::clone(&file);
    ///     move |buf: &[u8]| -> i32 {
    ///         let mut file = file.lock().unwrap();
    ///         match file.write_all(buf) {
    ///             Ok(_) => buf.len() as i32,
    ///             Err(e) => {
    ///                 println!("Write error: {}", e);
    ///                 ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i32
    ///             }
    ///         }
    ///     }
    /// };
    ///
    /// // ✅ Define the seek callback (position adjustment logic)
    /// let seek_callback = {
    ///     let file = Arc::clone(&file);
    ///     Box::new(move |offset: i64, whence: i32| -> i64 {
    ///         let mut file = file.lock().unwrap();
    ///
    ///         // ✅ Handle AVSEEK_SIZE: FFmpeg asks for the total stream size instead of seeking
    ///         if whence == ffmpeg_sys_next::AVSEEK_SIZE {
    ///             if let Ok(size) = file.metadata().map(|m| m.len() as i64) {
    ///                 return size;
    ///             }
    ///             return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
    ///         }
    ///
    ///         // ✅ Defensive: mask AVSEEK_FORCE (avio_seek strips it before a custom
    ///         // callback). The AVIO layer sends no other whence values (lseek extensions
    ///         // like SEEK_HOLE/SEEK_DATA never reach a custom callback).
    ///         match whence & !ffmpeg_sys_next::AVSEEK_FORCE {
    ///             ffmpeg_sys_next::SEEK_SET => file.seek(SeekFrom::Start(offset as u64)),
    ///             ffmpeg_sys_next::SEEK_CUR => file.seek(SeekFrom::Current(offset)),
    ///             ffmpeg_sys_next::SEEK_END => file.seek(SeekFrom::End(offset)),
    ///             _ => return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64,
    ///         }.map_or(ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64, |pos| pos as i64)
    ///     })
    /// };
    ///
    /// // ✅ Create an output with both callbacks
    /// let output = Output::new_by_write_callback(write_callback)
    ///     .set_format("mp4")
    ///     .set_seek_callback(seek_callback);
    /// ```
    pub fn set_seek_callback<F>(mut self, seek_callback: F) -> Self
    where
        F: FnMut(i64, i32) -> i64 + Send + 'static,
    {
        self.seek_callback =
            Some(Box::new(seek_callback) as Box<dyn FnMut(i64, i32) -> i64 + Send>);
        self
    }

    /// Sets the output format for the container.
    ///
    /// This method allows you to specify the output format for the container. If no format is specified,
    /// FFmpeg will attempt to detect it automatically based on the file extension or output URL.
    ///
    /// ### Parameters:
    /// - `format: &str`: A string specifying the desired output format (e.g., `mp4`, `flv`, `mkv`).
    ///
    /// ### Return Value:
    /// - Returns the `Output` instance with the newly set format.
    pub fn set_format(mut self, format: impl Into<String>) -> Self {
        self.format = Some(format.into());
        self
    }

    /// Sets the **video codec** to be used for encoding.
    ///
    /// # Arguments
    /// * `video_codec` - A string slice representing the desired video codec (e.g., `"h264"`, `"hevc"`).
    ///
    /// # Returns
    /// * `Self` - Returns the modified `Output` struct, allowing for method chaining.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("rtmp://localhost/live/stream")
    ///     .set_video_codec("h264");
    /// ```
    pub fn set_video_codec(mut self, video_codec: impl Into<String>) -> Self {
        self.video_codec = Some(video_codec.into());
        self
    }

    /// Sets the **audio codec** to be used for encoding.
    ///
    /// # Arguments
    /// * `audio_codec` - A string slice representing the desired audio codec (e.g., `"aac"`, `"mp3"`).
    ///
    /// # Returns
    /// * `Self` - Returns the modified `Output` struct, allowing for method chaining.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("rtmp://localhost/live/stream")
    ///     .set_audio_codec("aac");
    /// ```
    pub fn set_audio_codec(mut self, audio_codec: impl Into<String>) -> Self {
        self.audio_codec = Some(audio_codec.into());
        self
    }

    /// Sets the **subtitle codec** to be used for encoding.
    ///
    /// # Arguments
    /// * `subtitle_codec` - A string slice representing the desired subtitle codec
    ///   (e.g., `"mov_text"`, `"webvtt"`).
    ///
    /// # Returns
    /// * `Self` - Returns the modified `Output` struct, allowing for method chaining.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("rtmp://localhost/live/stream")
    ///     .set_subtitle_codec("mov_text");
    /// ```
    pub fn set_subtitle_codec(mut self, subtitle_codec: impl Into<String>) -> Self {
        self.subtitle_codec = Some(subtitle_codec.into());
        self
    }

    /// Replaces the entire frame-processing pipeline with a new sequence
    /// of transformations for **pre-encoding** frames on this `Output`.
    ///
    /// This method clears any previously set pipelines and replaces them with the provided list.
    ///
    /// # Parameters
    /// * `frame_pipelines` - A list of [`FramePipeline`] instances defining the
    ///   transformations to apply before encoding.
    ///
    /// # Returns
    /// * `Self` - Returns the modified `Output`, enabling method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_frame_pipelines(vec![
    ///         FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter("opengl", Box::new(my_filter)),
    ///         // Additional pipelines...
    ///     ]);
    /// ```
    pub fn set_frame_pipelines(mut self, frame_pipelines: Vec<impl Into<FramePipeline>>) -> Self {
        self.frame_pipelines = Some(
            frame_pipelines
                .into_iter()
                .map(|frame_pipeline| frame_pipeline.into())
                .collect(),
        );
        self
    }

    /// Adds a single [`FramePipeline`] to the existing pipeline list.
    ///
    /// If no pipelines are currently defined, this method creates a new pipeline list.
    /// Otherwise, it appends the provided pipeline to the existing transformations.
    ///
    /// # Parameters
    /// * `frame_pipeline` - A [`FramePipeline`] defining a transformation.
    ///
    /// # Returns
    /// * `Self` - Returns the modified `Output`, enabling method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .add_frame_pipeline(FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter("opengl", Box::new(my_filter)).build())
    ///     .add_frame_pipeline(FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_AUDIO).filter("my_custom_filter1", Box::new(...)).filter("my_custom_filter2", Box::new(...)));
    /// ```
    pub fn add_frame_pipeline(mut self, frame_pipeline: impl Into<FramePipeline>) -> Self {
        if self.frame_pipelines.is_none() {
            self.frame_pipelines = Some(vec![frame_pipeline.into()]);
        } else {
            self.frame_pipelines
                .as_mut()
                .unwrap()
                .push(frame_pipeline.into());
        }
        self
    }

    /// Adds a **stream mapping** for a specific stream or stream type,
    /// **re-encoding** it according to this output’s codec settings.
    ///
    /// # Linklabel (FFmpeg-like Specifier)
    ///
    /// This string typically follows `"<input_index>:<media_type>"` syntax:
    /// - **`"0:v"`** – the video stream(s) from input #0.
    /// - **`"1:a?"`** – audio from input #1, **ignore** if none present (due to `?`).
    /// - Other possibilities include `"0:s"`, `"0:d"`, etc. for subtitles/data, optionally with `?`.
    ///
    /// By calling `add_stream_map`, **you force re-encoding** of the chosen stream(s).
    /// If the user wants a bit-for-bit copy, see [`add_stream_map_with_copy`](Self::add_stream_map_with_copy).
    ///
    /// # Parameters
    /// - `linklabel`: An FFmpeg-style specifier referencing the desired input index and
    ///   media type, like `"0:v"`, `"1:a?"`, etc.
    ///
    /// # Returns
    /// * `Self` - for chained method calls.
    ///
    /// # Example
    /// ```rust,ignore
    /// // Re-encode the video stream from input #0 (fail if no video).
    /// let output = Output::from("output.mp4")
    ///     .add_stream_map("0:v");
    /// ```
    pub fn add_stream_map(mut self, linklabel: impl Into<String>) -> Self {
        self.stream_map_specs.push(linklabel.into().into());
        self
    }

    /// Adds a **stream mapping** for a specific stream or stream type,
    /// **copying** it bit-for-bit from the source without re-encoding.
    ///
    /// # Linklabel (FFmpeg-like Specifier)
    ///
    /// Follows the same `"<input_index>:<media_type>"` pattern as [`add_stream_map`](Self::add_stream_map):
    /// - **`"0:a"`** – audio stream(s) from input #0.
    /// - **`"0:a?"`** – same, but ignore errors if no audio exists.
    /// - And so on for video (`v`), subtitles (`s`), attachments (`t`), etc.
    ///
    /// # Copy vs. Re-encode
    ///
    /// Here, `copy = true` by default, meaning the chosen stream(s) are passed through
    /// **without** decoding/encoding. This generally **only** works if the source’s codec
    /// is compatible with the container/format you’re outputting to.
    /// If you require re-encoding (e.g., to ensure compatibility or apply filters),
    /// use [`add_stream_map`](Self::add_stream_map).
    ///
    /// # Parameters
    /// - `linklabel`: An FFmpeg-style specifier referencing the desired input index and
    ///   media type, like `"0:v?"`.
    ///
    /// # Returns
    /// * `Self` - for chained method calls.
    ///
    /// # Example
    /// ```rust,ignore
    /// // Copy the audio stream(s) from input #0 if present, no re-encode:
    /// let output = Output::from("output.mkv")
    ///     .add_stream_map_with_copy("0:a?");
    /// ```
    pub fn add_stream_map_with_copy(mut self, linklabel: impl Into<String>) -> Self {
        self.stream_map_specs.push(StreamMapSpec {
            linklabel: linklabel.into(),
            copy: true,
        });
        self
    }

    /// Sets the **start time** (in microseconds) for output encoding.
    ///
    /// If this is set, FFmpeg will attempt to start encoding from the specified
    /// timestamp in the input stream. This can be used to skip initial content.
    ///
    /// # Parameters
    /// * `start_time_us` - The start time in microseconds.
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_start_time_us(2_000_000); // Start at 2 seconds
    /// ```
    pub fn set_start_time_us(mut self, start_time_us: i64) -> Self {
        self.start_time_us = Some(start_time_us);
        self
    }

    /// Sets the **recording time** (in microseconds) for output encoding.
    ///
    /// This indicates how many microseconds of data should be processed
    /// (i.e., maximum duration to encode). Once this time is reached,
    /// FFmpeg will stop encoding.
    ///
    /// # Parameters
    /// * `recording_time_us` - The maximum duration (in microseconds) to process.
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_recording_time_us(5_000_000); // Record for 5 seconds
    /// ```
    pub fn set_recording_time_us(mut self, recording_time_us: i64) -> Self {
        self.recording_time_us = Some(recording_time_us);
        self
    }

    /// Sets a **stop time** (in microseconds) for output encoding.
    ///
    /// If set, FFmpeg will stop encoding once the input’s timestamp
    /// surpasses this value. Effectively, encoding ends at this timestamp
    /// regardless of remaining data.
    ///
    /// # Parameters
    /// * `stop_time_us` - The timestamp (in microseconds) at which to stop.
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_stop_time_us(10_000_000); // Stop at 10 seconds
    /// ```
    pub fn set_stop_time_us(mut self, stop_time_us: i64) -> Self {
        self.stop_time_us = Some(stop_time_us);
        self
    }

    /// Finish the output when its shortest limiting stream ends (FFmpeg `-shortest`).
    ///
    /// Encoded audio/video are truncated at the **frame** level before encoding
    /// (no B-frame stranding); streamcopy / subtitle / data are truncated at the
    /// **packet** level — the same presentation-time cut FFmpeg makes, with the
    /// same limitation that a copy B-frame near the cut may reference a dropped
    /// later packet. Exact when the shortest→longest gap is within the buffering
    /// window (see [`set_shortest_buf_duration_us`](Self::set_shortest_buf_duration_us),
    /// default 10 s). Default: `false`.
    ///
    /// # Limitations
    /// Any cut stream fed by an input whose read cannot be interrupted mid-packet —
    /// a pipe, a custom IO source, a live device, or a readrate-limited (`-re`)
    /// input — may keep that demuxer alive until its in-flight read returns,
    /// delaying termination. Ordinary seekable file and network inputs are
    /// unaffected, as is a single encoded stream (there is nothing to cut it against).
    ///
    /// When a cut stream also has an output bitstream filter that reorders, buffers,
    /// or rewrites packet timestamps (e.g. `setts`, `pgs_frame_merge`), the
    /// packet-level cut is decided on the pre-filter timestamps. Timestamp-preserving
    /// 1:1 filters (`h264_mp4toannexb`, `aac_adtstoasc`, metadata filters) are unaffected.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4").set_shortest(true);
    /// ```
    pub fn set_shortest(mut self, shortest: bool) -> Self {
        self.shortest = shortest;
        self
    }

    /// Maximum microseconds one stream is buffered waiting for a lagging peer
    /// before it is released anyway (FFmpeg `-shortest_buf_duration`, expressed in
    /// seconds upstream, microseconds here). Bounds `-shortest` memory use and
    /// precision. Values `<= 0` are ignored. Default: `10_000_000` (10 s).
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_shortest(true)
    ///     .set_shortest_buf_duration_us(30_000_000); // tolerate a 30 s gap
    /// ```
    pub fn set_shortest_buf_duration_us(mut self, shortest_buf_duration_us: i64) -> Self {
        if shortest_buf_duration_us > 0 {
            self.shortest_buf_duration_us = shortest_buf_duration_us;
        }
        self
    }

    /// Sets a **target frame rate** for output encoding, as a `num/den`
    /// rational (e.g. `30, 1` for 30 FPS).
    ///
    /// This can force the output to use a specific frame rate (e.g., 30/1 for 30 FPS).
    /// If unset, FFmpeg typically preserves the source frame rate or uses defaults
    /// based on the selected codec/container.
    ///
    /// # Parameters
    /// * `num`: Frame rate numerator (e.g., 30 for 30fps, 24000 for 23.976fps)
    /// * `den`: Frame rate denominator (e.g., 1 for 30fps, 1001 for 23.976fps)
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Errors
    /// The value is validated when the context is built:
    /// `FfmpegContext::builder().build()` fails with
    /// [`OpenOutputError::InvalidOption`](crate::error::OpenOutputError::InvalidOption)
    /// if `num` or `den` is not positive.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_framerate(30, 1);
    /// ```
    pub fn set_framerate(mut self, num: i32, den: i32) -> Self {
        self.framerate = Some(AVRational { num, den });
        self
    }

    /// Sets a **maximum frame rate** cap for output encoding (`-fpsmax`).
    ///
    /// Unlike [`set_framerate`](Self::set_framerate), this does not force a
    /// rate: the output keeps its native frame rate and is only clamped when
    /// that rate exceeds the cap or cannot be determined
    /// (ffmpeg_filter.c choose_out_timebase).
    ///
    /// # Parameters
    /// * `num`: Upper-bound numerator (e.g., 30 for a 30fps cap)
    /// * `den`: Upper-bound denominator
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Errors
    /// Validated when the context is built, like
    /// [`set_framerate`](Self::set_framerate): non-positive values fail with
    /// [`OpenOutputError::InvalidOption`](crate::error::OpenOutputError::InvalidOption).
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_framerate_max(30, 1);
    /// ```
    pub fn set_framerate_max(mut self, num: i32, den: i32) -> Self {
        self.framerate_max = Some(AVRational { num, den });
        self
    }

    /// Sets the **video sync method** to be used during encoding.
    ///
    /// FFmpeg uses a variety of vsync policies to handle frame presentation times,
    /// dropping/duplicating frames as needed. Adjusting this can be useful when
    /// you need strict CFR (constant frame rate), or to pass frames through
    /// without modification (`VsyncPassthrough`).
    ///
    /// # Parameters
    /// * `method` - A variant of [`VSyncMethod`], such as `VsyncCfr` or `VsyncVfr`.
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_vsync_method(VSyncMethod::VsyncCfr);
    /// ```
    pub fn set_vsync_method(mut self, method: VSyncMethod) -> Self {
        self.vsync_method = method;
        self
    }

    /// Sets the **bits per raw sample** for video encoding.
    ///
    /// This value can influence quality or color depth when dealing with
    /// certain pixel formats. Commonly used for high-bit-depth workflows
    /// or specialized encoding scenarios.
    ///
    /// # Parameters
    /// * `bits` - The bits per raw sample (e.g., 8, 10, 12).
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mkv")
    ///     .set_bits_per_raw_sample(10); // e.g., 10-bit
    /// ```
    pub fn set_bits_per_raw_sample(mut self, bits: i32) -> Self {
        self.bits_per_raw_sample = Some(bits);
        self
    }

    /// Sets the **audio sample rate** (in Hz) for output encoding.
    ///
    /// This method allows you to specify the desired audio sample rate for the output.
    /// Common values include 44100 (CD quality), 48000 (standard for digital video),
    /// and 22050 or 16000 (for lower bitrate applications).
    ///
    /// # Parameters
    /// * `audio_sample_rate` - The sample rate in Hertz (e.g., 44100, 48000).
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_audio_sample_rate(48000); // Set to 48kHz
    /// ```
    pub fn set_audio_sample_rate(mut self, audio_sample_rate: i32) -> Self {
        self.audio_sample_rate = Some(audio_sample_rate);
        self
    }

    /// Sets the number of **audio channels** for output encoding.
    ///
    /// Common values include 1 (mono), 2 (stereo), 5.1 (6 channels), and 7.1 (8 channels).
    /// This setting affects the spatial audio characteristics of the output.
    ///
    /// # Parameters
    /// * `audio_channels` - The number of audio channels (e.g., 1 for mono, 2 for stereo).
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_audio_channels(2); // Set to stereo
    /// ```
    pub fn set_audio_channels(mut self, audio_channels: i32) -> Self {
        self.audio_channels = Some(audio_channels);
        self
    }

    /// Sets the **audio sample format** for output encoding, by FFmpeg
    /// format name — the same currency as [`set_pix_fmt`](Self::set_pix_fmt).
    ///
    /// Common names (see `ffmpeg -sample_fmts`):
    /// - `"s16"` (signed 16-bit)
    /// - `"s32"` (signed 32-bit)
    /// - `"flt"` (32-bit float)
    /// - `"fltp"` (32-bit float, planar)
    ///
    /// # Parameters
    /// * `sample_fmt` - The FFmpeg sample format name (e.g., `"s16"`).
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Errors
    /// The name is resolved when the context is built (like
    /// [`set_pix_fmt`](Self::set_pix_fmt)): an unknown name fails with
    /// [`OpenOutputError::UnknownSampleFormat`](crate::error::OpenOutputError::UnknownSampleFormat).
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_audio_sample_fmt("s16"); // signed 16-bit
    /// ```
    pub fn set_audio_sample_fmt(mut self, sample_fmt: impl Into<String>) -> Self {
        self.audio_sample_fmt = Some(sample_fmt.into());
        self
    }

    /// Sets the **video quality scale** (VBR) for encoding.
    ///
    /// This method configures a fixed quality scale for variable bitrate (VBR) video encoding.
    /// Lower values result in higher quality but larger file sizes, while higher values
    /// produce lower quality with smaller file sizes.
    ///
    /// # Note on Modern Usage
    /// While still supported, using fixed quality scale (`-q:v`) is generally not recommended
    /// for modern video encoding workflows with codecs like H.264 and H.265. Instead, consider:
    /// * For H.264/H.265: Use CRF (Constant Rate Factor) via `-crf` parameter
    /// * For two-pass encoding: Use target bitrate settings
    ///
    /// This parameter is primarily useful for older codecs or specific scenarios where
    /// direct quality scale control is needed.
    ///
    /// # Quality Scale Ranges by Codec
    /// * **H.264/H.265**: 0-51 (if needed: 17-28)
    ///   - 17-18: Visually lossless
    ///   - 23: High quality
    ///   - 28: Good quality with reasonable file size
    /// * **MPEG-4/MPEG-2**: 2-31 (recommended: 2-6)
    ///   - Lower values = higher quality
    /// * **VP9**: 0-63 (if needed: 15-35)
    ///
    /// # Parameters
    /// * `video_qscale` - The quality scale value for video encoding.
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// // For MJPEG encoding of image sequences
    /// let output = Output::from("output.jpg")
    ///     .set_video_qscale(2);  // High quality JPEG images
    ///
    /// // For legacy image format conversion
    /// let output = Output::from("output.png")
    ///     .set_video_qscale(3);  // Controls compression level
    /// ```
    pub fn set_video_qscale(mut self, video_qscale: i32) -> Self {
        self.video_qscale = Some(video_qscale);
        self
    }

    /// Force the **video** encoder to emit a keyframe (an IDR request) at the given
    /// absolute output times — the list form of FFmpeg's `-force_key_frames "0,5,10.5"`.
    ///
    /// `spec` is a comma-separated list of times in **seconds** (e.g. `"0,5,10.5"`).
    /// Each token is parsed as a decimal number of seconds and converted to
    /// microseconds. The list is sorted ascending internally, so input order does not
    /// matter; duplicate times are kept (matching FFmpeg).
    ///
    /// # Semantics and limitations
    /// * Times are **absolute** output/encoder presentation timestamps, not offsets
    ///   relative to the first frame.
    /// * `pict_type = I` is a **request**: software encoders such as `mpeg4`,
    ///   `libx264` and `libx265` honor it and emit a keyframe; some hardware encoders
    ///   may ignore it or keep their own GOP cadence. ez-ffmpeg can guarantee no more
    ///   than the FFmpeg CLI does here.
    /// * Applies only to **re-encoded video** streams. Audio, subtitle, and
    ///   stream-copy outputs ignore it; there is no effect if it is never set.
    /// * MVP grammar: only a comma-separated list of decimal **seconds** is supported.
    ///   The `HH:MM:SS`, `expr:`, `source`, and `source_no_drop` forms of FFmpeg's
    ///   option are **not** supported and such tokens return an error.
    /// * **Negative times are rejected** — an ez-ffmpeg MVP choice; a negative forced
    ///   time is meaningless.
    ///
    /// # Errors
    /// The spec is stored as given and validated when the context is built
    /// (like every other deferred option): `FfmpegContext::builder().build()`
    /// fails with [`OpenOutputError::InvalidOption`](crate::error::OpenOutputError::InvalidOption)
    /// if it is empty, contains an empty token, or contains a token that is
    /// not a finite, non-negative decimal number (this rejects `NaN`,
    /// infinities, and values that would overflow `i64` microseconds).
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_video_codec("libx264")
    ///     .set_force_key_frames("0,5,10.5");
    /// ```
    pub fn set_force_key_frames(mut self, spec: impl Into<String>) -> Self {
        self.forced_kf_spec = Some(spec.into());
        self
    }

    /// Sets the **audio quality scale** for encoding.
    ///
    /// This method configures codec-specific audio quality settings. The range, behavior,
    /// and optimal values depend entirely on the audio codec being used.
    ///
    /// # Quality Scale Ranges by Codec
    /// * **MP3 (libmp3lame)**: 0-9 (recommended: 2-5)
    ///   - 0: Highest quality
    ///   - 2: Near-transparent quality (~190-200 kbps)
    ///   - 5: Good quality (~130 kbps)
    ///   - 9: Lowest quality
    /// * **AAC**: 0.1-255 (recommended: 1-5)
    ///   - 1: Highest quality (~250 kbps)
    ///   - 3: Good quality (~160 kbps)
    ///   - 5: Medium quality (~100 kbps)
    /// * **Vorbis**: -1 to 10 (recommended: 3-8)
    ///   - 10: Highest quality
    ///   - 5: Good quality
    ///   - 3: Medium quality
    ///
    /// # Parameters
    /// * `audio_qscale` - The quality scale value for audio encoding.
    ///
    /// # Returns
    /// * `Self` - The modified `Output`, allowing method chaining.
    ///
    /// # Example
    /// ```rust,ignore
    /// // For MP3 encoding at high quality
    /// let output = Output::from("output.mp3")
    ///     .set_audio_codec("libmp3lame")
    ///     .set_audio_qscale(2);
    ///
    /// // For AAC encoding at good quality
    /// let output = Output::from("output.m4a")
    ///     .set_audio_codec("aac")
    ///     .set_audio_qscale(3);
    ///
    /// // For Vorbis encoding at high quality
    /// let output = Output::from("output.ogg")
    ///     .set_audio_codec("libvorbis")
    ///     .set_audio_qscale(7);
    /// ```
    pub fn set_audio_qscale(mut self, audio_qscale: i32) -> Self {
        self.audio_qscale = Some(audio_qscale);
        self
    }

    /// **Sets the maximum number of video frames to encode (`-frames:v`).**
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -frames:v 100 output.mp4
    /// ```
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_max_video_frames(500);
    /// ```
    pub fn set_max_video_frames(mut self, max_frames: impl Into<Option<i64>>) -> Self {
        self.max_video_frames = max_frames.into();
        self
    }

    /// **Sets the maximum number of audio frames to encode (`-frames:a`).**
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -frames:a 500 output.mp4
    /// ```
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_max_audio_frames(500);
    /// ```
    pub fn set_max_audio_frames(mut self, max_frames: impl Into<Option<i64>>) -> Self {
        self.max_audio_frames = max_frames.into();
        self
    }

    /// **Sets the maximum number of subtitle frames to encode (`-frames:s`).**
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -frames:s 200 output.mp4
    /// ```
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_max_subtitle_frames(200);
    /// ```
    pub fn set_max_subtitle_frames(mut self, max_frames: impl Into<Option<i64>>) -> Self {
        self.max_subtitle_frames = max_frames.into();
        self
    }

    // ========== Stream Disable & Format API Methods (P1 Features) ==========
    // These methods replicate FFmpeg's `-vn`, `-an`, `-sn`, `-b:v`, `-b:a`, and `-pix_fmt` options.

    /// Disables video stream mapping (equivalent to `-vn` in FFmpeg).
    ///
    /// Video streams will be excluded from automatic stream mapping.
    /// This is useful when you want to extract only audio from a video file.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -vn output.mp3
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Extract audio only, no video
    /// let output = Output::from("output.mp3")
    ///     .disable_video();
    /// ```
    pub fn disable_video(mut self) -> Self {
        self.video_disable = true;
        self
    }

    /// Disables audio stream mapping (equivalent to `-an` in FFmpeg).
    ///
    /// Audio streams will be excluded from automatic stream mapping.
    /// This is useful when you want to create a silent video.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -an output.mp4
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Create video without audio
    /// let output = Output::from("output.mp4")
    ///     .disable_audio();
    /// ```
    pub fn disable_audio(mut self) -> Self {
        self.audio_disable = true;
        self
    }

    /// Disables subtitle stream mapping (equivalent to `-sn` in FFmpeg).
    ///
    /// Subtitle streams will be excluded from automatic stream mapping.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mkv -sn output.mkv
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Copy video and audio, but exclude subtitles
    /// let output = Output::from("output.mkv")
    ///     .disable_subtitle();
    /// ```
    pub fn disable_subtitle(mut self) -> Self {
        self.subtitle_disable = true;
        self
    }

    /// Disables data stream mapping (equivalent to `-dn` in FFmpeg).
    ///
    /// Data streams (timed metadata, chapter markers, etc.) will be
    /// excluded from automatic stream mapping.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mkv -dn output.mp4
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Copy video and audio, but exclude data streams
    /// let output = Output::from("output.mp4")
    ///     .disable_data();
    /// ```
    pub fn disable_data(mut self) -> Self {
        self.data_disable = true;
        self
    }

    /// Sets the video bitrate (equivalent to `-b:v` in FFmpeg).
    ///
    /// The bitrate string follows FFmpeg conventions:
    /// - `"1M"` or `"1000k"` for 1 Mbps
    /// - `"500k"` for 500 Kbps
    /// - `"2M"` for 2 Mbps
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -b:v 2M output.mp4
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_video_bitrate("2M");
    /// ```
    pub fn set_video_bitrate(self, bitrate: impl Into<String>) -> Self {
        self.set_video_codec_opt("b", bitrate)
    }

    /// Sets the audio bitrate (equivalent to `-b:a` in FFmpeg).
    ///
    /// The bitrate string follows FFmpeg conventions:
    /// - `"128k"` for 128 Kbps
    /// - `"192k"` for 192 Kbps
    /// - `"320k"` for 320 Kbps
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -b:a 192k output.mp4
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_audio_bitrate("192k");
    /// ```
    pub fn set_audio_bitrate(self, bitrate: impl Into<String>) -> Self {
        self.set_audio_codec_opt("b", bitrate)
    }

    /// Sets the output pixel format (equivalent to `-pix_fmt` in FFmpeg).
    ///
    /// Common pixel formats include:
    /// - `"yuv420p"` - Most compatible format for H.264
    /// - `"yuv444p"` - Higher quality, less compatible
    /// - `"rgb24"` - RGB format
    /// - `"nv12"` - Common for hardware encoding
    ///
    /// To see all available formats, run: `ffmpeg -pix_fmts`
    ///
    /// # Behavior
    ///
    /// - **Unknown format name**: Returns [`OpenOutputError::UnknownPixelFormat`] error.
    ///   This matches FFmpeg CLI behavior (e.g., `ffmpeg -pix_fmt foobar` also fails).
    /// - **Format incompatible with encoder**: The filter graph automatically converts
    ///   to a compatible format. For example, specifying `rgb48be` with libx264 will
    ///   auto-convert to `yuv420p`.
    /// - **Stream copy mode**: This setting has no effect when using `-c:v copy`.
    ///
    /// **Equivalent FFmpeg Command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -pix_fmt yuv420p output.mp4
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_pix_fmt("yuv420p");
    /// ```
    ///
    /// [`OpenOutputError::UnknownPixelFormat`]: crate::error::OpenOutputError::UnknownPixelFormat
    pub fn set_pix_fmt(mut self, pix_fmt: impl Into<String>) -> Self {
        self.pix_fmt = Some(pix_fmt.into());
        self
    }

    /// Sets a simple **video** filter chain for this output, equivalent to
    /// FFmpeg `-vf` (`-filter:v`).
    ///
    /// The chain is applied to this output's **re-encoded** video stream: every
    /// simple (non-`filter_complex`) video encode already runs through an
    /// implicit per-output filtergraph whose description defaults to the
    /// passthrough `null` chain, and this method replaces that `null` with the
    /// given description. The filter text is passed to FFmpeg verbatim — the
    /// same string the CLI accepts after `-vf` works here unchanged, e.g.
    /// `"scale=1280:-2"` or `"fps=30,scale=640:360"`.
    ///
    /// Unlike [`FfmpegContextBuilder::filter_desc`], which creates one
    /// context-level graph shared by all outputs, this filter belongs to this
    /// `Output` alone: with several outputs, each can carry its own chain (or
    /// none), matching how the CLI scopes `-vf` to the output file it precedes.
    ///
    /// # Contract
    /// - **Linear chain only**: the description must have exactly one video
    ///   input pad and one video output pad. Splitting/merging descriptions
    ///   (e.g. `split`) fail the build with
    ///   [`OpenOutputError::SimpleFilterInvalidShape`]; non-video chains (e.g.
    ///   `anull`) fail with [`OpenOutputError::SimpleFilterMediaTypeMismatch`].
    ///   Use [`FfmpegContextBuilder::filter_desc`] for complex graphs.
    /// - **Re-encode only**: combining this with `set_video_codec("copy")` or
    ///   a copy stream map covering a video stream fails the build with
    ///   [`OpenOutputError::FilterWithStreamCopy`], matching the CLI's
    ///   "Filtering and streamcopy cannot be used together".
    /// - **Simple xor complex**: if this output's video is fed by a
    ///   context-level filtergraph output, the build fails with
    ///   [`OpenOutputError::SimpleAndComplexFilter`], matching the CLI's rule
    ///   for `-vf` + `-filter_complex` on the same stream.
    /// - **Audio is untouched**: only the video stream runs through this
    ///   chain. There is no per-output audio (`-af`) equivalent yet.
    /// - **Must be consumed**: if the output ends up with no re-encoded
    ///   video stream at all (audio-only input, [`disable_video`], maps that
    ///   match no video stream), the build fails with
    ///   [`OpenOutputError::VideoFilterUnused`] instead of silently dropping
    ///   the chain.
    /// - **VideoWriter**: a [`VideoWriter`](crate::VideoWriter) opening this
    ///   `Output` honors the chain when no builder-level `filter_desc` is
    ///   set; configuring both fails with
    ///   [`WriterError::ConflictingFilterDescriptions`](crate::core::writer::WriterError::ConflictingFilterDescriptions).
    ///
    /// [`disable_video`]: Self::disable_video
    /// [`OpenOutputError::VideoFilterUnused`]: crate::error::OpenOutputError::VideoFilterUnused
    ///
    /// An **empty string is kept** and fails the build like `-vf ""` fails
    /// the CLI (an empty graph parses to zero pads); use
    /// [`clear_video_filter`](Self::clear_video_filter) to remove a
    /// previously set chain. The description itself is validated when the
    /// context is built; an invalid filter name surfaces as a
    /// [`FilterGraphParseError`](crate::error::FilterGraphParseError) from
    /// `build()`, not from this setter.
    ///
    /// **Equivalent FFmpeg command:**
    /// ```sh
    /// ffmpeg -i input.mp4 -vf scale=1280:-2 -c:a copy resized.mp4
    /// ```
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("resized.mp4")
    ///     .set_video_filter("scale=1280:-2") // -vf scale=1280:-2
    ///     .set_audio_codec("copy");          // -c:a copy
    /// ```
    ///
    /// [`FfmpegContextBuilder::filter_desc`]: crate::core::context::ffmpeg_context_builder::FfmpegContextBuilder::filter_desc
    /// [`OpenOutputError::SimpleFilterInvalidShape`]: crate::error::OpenOutputError::SimpleFilterInvalidShape
    /// [`OpenOutputError::SimpleFilterMediaTypeMismatch`]: crate::error::OpenOutputError::SimpleFilterMediaTypeMismatch
    /// [`OpenOutputError::FilterWithStreamCopy`]: crate::error::OpenOutputError::FilterWithStreamCopy
    /// [`OpenOutputError::SimpleAndComplexFilter`]: crate::error::OpenOutputError::SimpleAndComplexFilter
    pub fn set_video_filter(mut self, filter_chain: impl Into<String>) -> Self {
        self.video_filter = Some(filter_chain.into());
        self
    }

    /// Removes a previously set [`set_video_filter`](Self::set_video_filter)
    /// chain, restoring the implicit passthrough (`null`) graph.
    pub fn clear_video_filter(mut self) -> Self {
        self.video_filter = None;
        self
    }

    /// Sets sws (libswscale) options for the `scale` filters libavfilter
    /// **auto-inserts** to convert this output's frames to a format/size the
    /// encoder accepts (pixel format, resolution, color).
    ///
    /// This maps to FFmpeg's graph-level `AVFilterGraph.scale_sws_opts`. It only
    /// affects *auto-inserted* scaling; if you build the filtergraph yourself
    /// with an explicit `scale=...`, that filter's own arguments still apply.
    /// Has no effect on stream-copy (`-c:v copy`) outputs, which are not filtered.
    ///
    /// The string uses FFmpeg option syntax, e.g.
    /// `"flags=lanczos+accurate_rnd"`. To see the available flags, run
    /// `ffmpeg -h filter=scale`.
    ///
    /// # Graph-level, not per-output
    /// FFmpeg applies these options to the whole filtergraph, not a single
    /// output. When one filtergraph drives several outputs, they must not set
    /// *different* non-empty values — that conflict is rejected when the graph is
    /// configured. An explicit [`FilterComplex::set_sws_opts`](crate::core::context::filter_complex::FilterComplex::set_sws_opts)
    /// takes precedence over this per-output value.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_sws_opts("flags=lanczos+accurate_rnd");
    /// ```
    pub fn set_sws_opts(mut self, opts: impl Into<String>) -> Self {
        self.sws_opts = Some(opts.into());
        self
    }

    /// Sets swr (libswresample) options for the `aresample` filters libavfilter
    /// **auto-inserts** to convert this output's audio to a sample
    /// format / rate / channel layout the encoder accepts.
    ///
    /// This maps to FFmpeg's graph-level `AVFilterGraph.aresample_swr_opts`. It
    /// only affects *auto-inserted* resampling; an explicit `aresample=...` in a
    /// hand-written filtergraph keeps its own arguments. Has no effect on
    /// stream-copy outputs.
    ///
    /// The string uses FFmpeg option syntax, e.g.
    /// `"resampler=soxr:precision=28"`.
    ///
    /// # Graph-level, not per-output
    /// See [`set_sws_opts`](Self::set_sws_opts): the value is graph-level and the
    /// same precedence / conflict rules apply.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .set_swr_opts("resampler=soxr:precision=28");
    /// ```
    pub fn set_swr_opts(mut self, opts: impl Into<String>) -> Self {
        self.swr_opts = Some(opts.into());
        self
    }
}

impl From<Box<dyn FnMut(&[u8]) -> i32 + Send>> for Output {
    fn from(write_callback: Box<dyn FnMut(&[u8]) -> i32 + Send>) -> Self {
        Self::with_target(OutputTarget::CustomIo {
            write: write_callback,
        })
    }
}

impl From<crate::core::packet_sink::PacketSink> for Output {
    fn from(sink: crate::core::packet_sink::PacketSink) -> Self {
        Self::with_target(OutputTarget::PacketSink(sink))
    }
}

impl From<String> for Output {
    fn from(url: String) -> Self {
        Self::with_target(OutputTarget::Url(url))
    }
}

impl From<&str> for Output {
    fn from(url: &str) -> Self {
        Self::from(String::from(url))
    }
}

/// Temporary storage for unparsed stream map specifications (user input stage)
/// Equivalent to FFmpeg's command-line parsing before opt_map() expansion
#[derive(Debug, Clone)]
pub(crate) struct StreamMapSpec {
    /// Stream specifier string: "0:v", "1:a:0", "0:v?", "[label]", etc.
    pub(crate) linklabel: String,
    /// Stream copy flag (-c copy)
    pub(crate) copy: bool,
}

impl<T: Into<String>> From<T> for StreamMapSpec {
    fn from(linklabel: T) -> Self {
        Self {
            linklabel: linklabel.into(),
            copy: false,
        }
    }
}

/// Final expanded stream map (matches FFmpeg's StreamMap structure)
/// Created after parsing and expansion in outputs_bind()
/// FFmpeg reference: fftools/ffmpeg.h:134-141
#[derive(Debug, Clone)]
pub(crate) struct StreamMap {
    /// 1 if this mapping is disabled by a negative map (-map -0:v)
    pub(crate) disabled: bool,
    /// Input file index
    pub(crate) file_index: usize,
    /// Input stream index within the file
    pub(crate) stream_index: usize,
    /// Name of an output link, for mapping lavfi outputs (e.g., "[v]", "myout")
    pub(crate) linklabel: Option<String>,
    /// Stream copy flag (-c copy)
    pub(crate) copy: bool,
}

/// Parse an FFmpeg `-force_key_frames` **list-form** spec (e.g. `"0,5,10.5"`) into a
/// sorted `Vec<i64>` of microsecond timestamps (`AV_TIME_BASE_Q` units).
///
/// This is pure, set-time validation — no FFmpeg handle is required. It rejects empty
/// specs, empty tokens, non-numeric tokens (so the `expr:` / `source` forms are
/// refused), negative times, `NaN`/infinite values, and values that would overflow
/// `i64` microseconds. Overflow is rejected explicitly rather than relying on `as i64`
/// saturation. Duplicates are kept; the result is sorted ascending.
pub(crate) fn parse_forced_key_frames(spec: &str) -> Result<Vec<i64>, String> {
    if spec.trim().is_empty() {
        return Err("force_key_frames: empty spec".to_string());
    }

    let mut pts = Vec::new();
    for token in spec.split(',') {
        let token = token.trim();
        if token.is_empty() {
            return Err("force_key_frames: empty time entry".to_string());
        }

        let secs = token
            .parse::<f64>()
            .map_err(|_| format!("force_key_frames: invalid time '{token}'"))?;
        if !secs.is_finite() || secs < 0.0 {
            return Err(format!("force_key_frames: invalid time '{token}'"));
        }

        // Reject out-of-range values instead of relying on `as i64` saturation.
        // `i64::MAX as f64` rounds up to 2^63, so `>=` also rejects the boundary.
        let us = (secs * 1_000_000.0).round();
        if !us.is_finite() || us < 0.0 || us >= i64::MAX as f64 {
            return Err(format!("force_key_frames: time out of range '{token}'"));
        }

        pts.push(us as i64);
    }

    pts.sort_unstable();
    Ok(pts)
}

#[cfg(test)]
mod tests {
    use super::{parse_forced_key_frames, Output};

    #[test]
    fn io_buffer_size_is_unset_until_the_setter_runs() {
        // `None` = "never set"; the effective 64 KiB default is applied at
        // build time. Packet-sink validation needs the distinction.
        assert_eq!(Output::from("out.mp4").io_buffer_size, None);
    }

    #[test]
    fn set_io_buffer_size_valid() {
        assert_eq!(
            Output::from("out.mp4")
                .set_io_buffer_size(1 << 20)
                .io_buffer_size,
            Some(1 << 20)
        );
    }

    #[test]
    fn set_io_buffer_size_stores_invalid_values_for_deferred_validation() {
        let output = Output::new_by_write_callback(|_| 0).set_io_buffer_size(0);
        assert_eq!(output.io_buffer_size, Some(0));
    }

    #[test]
    fn muxing_queue_knobs_default_to_ffmpeg_parity() {
        use crate::core::context::pre_mux_queue::{
            DEFAULT_PRE_MUX_DATA_THRESHOLD, DEFAULT_PRE_MUX_MAX_PACKETS,
        };
        let output = Output::from("out.mp4");
        assert_eq!(output.max_muxing_queue_size, DEFAULT_PRE_MUX_MAX_PACKETS);
        assert_eq!(
            output.muxing_queue_data_threshold,
            DEFAULT_PRE_MUX_DATA_THRESHOLD
        );
    }

    #[test]
    fn set_muxing_queue_knobs_valid() {
        let output = Output::from("out.mp4")
            .set_max_muxing_queue_size(1024)
            .set_muxing_queue_data_threshold(256 * 1024 * 1024);
        assert_eq!(output.max_muxing_queue_size, 1024);
        assert_eq!(output.muxing_queue_data_threshold, 256 * 1024 * 1024);
    }

    #[test]
    fn muxing_queue_setters_store_invalid_values_for_deferred_validation() {
        let output = Output::from("out.mp4")
            .set_max_muxing_queue_size(0)
            .set_muxing_queue_data_threshold(0);
        assert_eq!(output.max_muxing_queue_size, 0);
        assert_eq!(output.muxing_queue_data_threshold, 0);
    }

    #[test]
    fn set_video_filter_stores_chain() {
        let output = Output::from("out.mp4").set_video_filter("scale=1280:-2");
        assert_eq!(output.video_filter.as_deref(), Some("scale=1280:-2"));
    }

    #[test]
    fn set_video_filter_keeps_empty_string() {
        // -vf "" parity: the empty description is preserved and fails the
        // build like the CLI's own empty-graph parse failure.
        let output = Output::from("out.mp4")
            .set_video_filter("scale=1280:-2")
            .set_video_filter("");
        assert_eq!(output.video_filter.as_deref(), Some(""));
    }

    #[test]
    fn clear_video_filter_resets() {
        let output = Output::from("out.mp4")
            .set_video_filter("scale=1280:-2")
            .clear_video_filter();
        assert_eq!(output.video_filter, None);
    }

    #[test]
    fn video_filter_defaults_to_none() {
        assert_eq!(Output::from("out.mp4").video_filter, None);
        assert_eq!(Output::new_by_write_callback(|_| 0).video_filter, None);
    }

    /// Constructor parity for the CLI-only flags: every public construction
    /// path funnels through `with_target`, and the compiler only enforces
    /// field PRESENCE there, not VALUES. A future field whose `with_target`
    /// default were `true` would silently arm strict/uniqueness semantics on
    /// every non-CLI pipeline; this pin turns that mistake into a red test.
    #[test]
    fn cli_only_flags_default_to_off_on_every_construction_path() {
        let outputs = [
            Output::from("out.mp4"),
            Output::new_by_write_callback(|_| 0),
            Output::new_by_packet_sink(crate::core::packet_sink::PacketSink::discard()),
        ];
        for output in outputs {
            assert!(!output.strict_avoptions);
            assert!(!output.require_unique_video_source);
            assert_eq!(output.video_filter, None);
        }
    }

    #[test]
    fn parses_sorted_microseconds() {
        assert_eq!(
            parse_forced_key_frames("0,5,10.5").unwrap(),
            vec![0, 5_000_000, 10_500_000]
        );
    }

    #[test]
    fn sorts_unsorted_input() {
        assert_eq!(
            parse_forced_key_frames("5,0,10").unwrap(),
            vec![0, 5_000_000, 10_000_000]
        );
    }

    #[test]
    fn rounds_fractional_seconds() {
        assert_eq!(parse_forced_key_frames("10.5").unwrap(), vec![10_500_000]);
    }

    #[test]
    fn keeps_duplicates() {
        assert_eq!(
            parse_forced_key_frames("5,5").unwrap(),
            vec![5_000_000, 5_000_000]
        );
    }

    #[test]
    fn tolerates_surrounding_whitespace() {
        assert_eq!(
            parse_forced_key_frames(" 1 , 2 ").unwrap(),
            vec![1_000_000, 2_000_000]
        );
    }

    #[test]
    fn accepts_zero() {
        assert_eq!(parse_forced_key_frames("0").unwrap(), vec![0]);
    }

    #[test]
    fn rejects_garbage_without_panicking() {
        for bad in [
            "",
            "   ",
            "5,,10",
            "abc",
            "expr:gte(t,5)",
            "-1",
            "5,NaN",
            "inf",
            "5,-0.5",
        ] {
            assert!(
                parse_forced_key_frames(bad).is_err(),
                "expected Err for {bad:?}"
            );
        }
    }

    #[test]
    fn rejects_overflow_instead_of_saturating() {
        assert!(parse_forced_key_frames("1e30").is_err());
    }
}
