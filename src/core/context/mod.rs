use crate::error::AllocFrameError;
use ffmpeg_sys_next::AVMediaType::{
    AVMEDIA_TYPE_ATTACHMENT, AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_DATA, AVMEDIA_TYPE_SUBTITLE,
    AVMEDIA_TYPE_VIDEO,
};
// Only SideDataList::push_clone (gated to FFmpeg 8+) clones entries; the
// free side runs on every real lane via Drop. Both symbols are FFmpeg 7.0+,
// and docs.rs generates its bindings against an older apt FFmpeg — so like
// every other >=7.0 symbol in this crate they stay out of docsrs builds.
#[cfg(ffmpeg_8_0)]
use ffmpeg_sys_next::av_frame_side_data_clone;
#[cfg(not(docsrs))]
use ffmpeg_sys_next::av_frame_side_data_free;
use ffmpeg_sys_next::{
    av_freep, av_gettime_relative, avcodec_free_context, avformat_close_input,
    avformat_free_context, avio_closep, avio_context_free, AVCodecContext, AVFormatContext,
    AVFrameSideData, AVIOContext, AVMediaType, AVRational, AVStream, AVFMT_NOFILE,
};
use std::ffi::c_void;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

/// How long output I/O may continue after STATUS_END before the interrupt
/// callback cuts it: long enough for a healthy muxer to finish its trailer,
/// short enough that stop() on a dead network peer returns within a second.
/// While a muxer holds an [`OutputFinalizeGuard`] the window does not apply
/// at all — a `movflags=+faststart` trailer rewrites the whole file and can
/// legitimately take far longer than any fixed grace.
const OUTPUT_END_GRACE_US: i64 = 500_000;

/// Default size of the AVIO buffer backing a custom read/write callback. FFmpeg
/// hands the callback one buffer-sized chunk at a time, so a larger buffer means
/// fewer Rust↔FFmpeg round-trips for sequential/network IO; the 64 KiB default
/// keeps first-packet latency low for live/low-latency use. Configurable per
/// Input/Output via set_io_buffer_size (sys-06).
pub(crate) const DEFAULT_CUSTOM_IO_BUFFER_SIZE: usize = 64 * 1024;

/// Shared state behind the AVIO interrupt callbacks (fftools installs
/// decode_interrupt_cb on inputs and outputs, ffmpeg_mux_init.c:3326,3371).
///
/// Inputs are interrupted as soon as the scheduler is stopping: nothing
/// meaningful is read after that. Outputs distinguish the terminal states:
/// STATUS_ABORT cuts I/O immediately (the caller gave up on the files), while
/// STATUS_END grants a grace window so trailers still get written — only an
/// output that stays blocked past the window (dead network peer) is cut.
pub(crate) struct InterruptState {
    scheduler_status: Arc<AtomicUsize>,
    // Microsecond timestamp of the first output callback that observed
    // STATUS_END; 0 = not observed yet.
    end_grace_start_us: AtomicI64,
    /// Number of mux workers currently inside their finalize window
    /// (av_write_trailer through the output-context free). While > 0,
    /// STATUS_END does not interrupt output I/O — only STATUS_ABORT does: a
    /// graceful stop() must not corrupt a trailer that is being written
    /// (H3), and abort() stays the hard-cancel escape hatch.
    finalizing_outputs: AtomicUsize,
}

impl InterruptState {
    pub(crate) fn new(scheduler_status: Arc<AtomicUsize>) -> Self {
        Self {
            scheduler_status,
            end_grace_start_us: AtomicI64::new(0),
            finalizing_outputs: AtomicUsize::new(0),
        }
    }

    /// Marks one output as finalizing for the guard's lifetime.
    pub(crate) fn begin_output_finalize(self: &Arc<Self>) -> OutputFinalizeGuard {
        self.finalizing_outputs.fetch_add(1, Ordering::AcqRel);
        OutputFinalizeGuard {
            state: Arc::clone(self),
        }
    }

    fn should_interrupt_input(&self) -> bool {
        crate::core::scheduler::ffmpeg_scheduler::is_stopping(
            self.scheduler_status.load(Ordering::Acquire),
        )
    }

    fn should_interrupt_output(&self) -> bool {
        let status = self.scheduler_status.load(Ordering::Acquire);
        if status == crate::core::scheduler::ffmpeg_scheduler::STATUS_ABORT {
            return true;
        }
        if status != crate::core::scheduler::ffmpeg_scheduler::STATUS_END {
            return false;
        }
        if self.finalizing_outputs.load(Ordering::Acquire) > 0 {
            // A trailer (e.g. a faststart moov rewrite) is in flight somewhere
            // on this scheduler; only abort() may cut output I/O now. Checked
            // BEFORE the grace CAS so the clock does not even start while a
            // finalize is running; once the last guard drops, a still-blocked
            // OTHER output starts its grace normally.
            return false;
        }
        let now = unsafe { av_gettime_relative() };
        let start = match self.end_grace_start_us.compare_exchange(
            0,
            now,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => now,
            Err(previous) => previous,
        };
        now - start > OUTPUT_END_GRACE_US
    }
}

/// RAII finalize marker. The decrement is mandatory even on unwind — a stuck
/// counter would exempt every later STATUS_END grace cut for this scheduler,
/// and stop() on a blocked sibling output would hang forever.
pub(crate) struct OutputFinalizeGuard {
    state: Arc<InterruptState>,
}

impl Drop for OutputFinalizeGuard {
    fn drop(&mut self) {
        self.state.finalizing_outputs.fetch_sub(1, Ordering::AcqRel);
    }
}

/// # Safety
/// `opaque` must point to an `InterruptState` that outlives every
/// AVFormatContext carrying this callback (the owning FfmpegContext holds the
/// Arc and outlives all worker threads).
pub(crate) unsafe extern "C" fn input_interrupt_cb(opaque: *mut c_void) -> libc::c_int {
    let state = &*(opaque as *const InterruptState);
    state.should_interrupt_input() as libc::c_int
}

/// # Safety
/// Same contract as [`input_interrupt_cb`].
pub(crate) unsafe extern "C" fn output_interrupt_cb(opaque: *mut c_void) -> libc::c_int {
    let state = &*(opaque as *const InterruptState);
    state.should_interrupt_output() as libc::c_int
}

/// Gate for the muxer's deferred start. fftools: `SchMux.mux_started` +
/// `PreMuxQueue` under `Scheduler.mux_ready_lock` (ffmpeg_sched.c).
///
/// Until the muxer thread is running, encoders park packets in a bounded
/// pre-queue; at start the muxer drains that queue and flips `started`.
/// Without a lock the flip races the send: an encoder that read
/// `started == false` can enqueue into the pre-queue AFTER the drain
/// finished, and that packet is never delivered. Pre-queue sends therefore
/// happen under the same lock as the drain-and-flip.
pub(crate) struct MuxStartGate {
    started: std::sync::atomic::AtomicBool,
    lock: std::sync::Mutex<()>,
}

/// What a gated pre-queue send resolved to.
pub(crate) enum PreSendOutcome {
    /// Parked in the pre-queue; the drain will deliver it.
    Sent,
    /// The gate opened first: send to the live queue instead.
    Started(PacketBox),
    /// Pre-queue full: park on the queue condvar and retry (the gate lock
    /// must not be held across the wait, or the drain could never run).
    Full(PacketBox),
    /// Pre-queue receiver is gone (muxer never started).
    Disconnected(PacketBox),
}

impl MuxStartGate {
    pub(crate) fn new() -> Self {
        Self {
            started: std::sync::atomic::AtomicBool::new(false),
            lock: std::sync::Mutex::new(()),
        }
    }

    pub(crate) fn is_started(&self) -> bool {
        self.started.load(Ordering::Acquire)
    }

    /// Runs the pre-queue drain and opens the gate as one atomic step.
    pub(crate) fn start_with(&self, drain: impl FnOnce()) {
        let _guard = self.lock.lock().unwrap();
        drain();
        self.started.store(true, Ordering::Release);
    }

    /// Attempts a pre-queue send while the gate is verifiably closed.
    pub(crate) fn send_pre(
        &self,
        pre_sender: &pre_mux_queue::PreMuxQueueSender,
        packet_box: PacketBox,
    ) -> PreSendOutcome {
        let _guard = self.lock.lock().unwrap();
        if self.started.load(Ordering::Acquire) {
            return PreSendOutcome::Started(packet_box);
        }
        match pre_sender.try_push(packet_box) {
            pre_mux_queue::PreQueueTryPush::Sent => PreSendOutcome::Sent,
            pre_mux_queue::PreQueueTryPush::Full(pb) => PreSendOutcome::Full(pb),
            pre_mux_queue::PreQueueTryPush::Disconnected(pb) => PreSendOutcome::Disconnected(pb),
        }
    }
}

use ffmpeg_context::{InputOpaque, OutputOpaque};

/// The **ffmpeg_context** module is responsible for assembling FFmpeg’s configuration:
/// inputs, outputs, codecs, filters, and other parameters needed to construct a
/// complete media processing pipeline.
///
/// # Example
/// ```rust,ignore
///
/// // Build an FFmpeg context with one input, some filter settings, and one output
/// let context = FfmpegContext::builder()
///     .input("test.mp4")
///     .filter_desc("hue=s=0")
///     .output("output.mp4")
///     .build()
///     .unwrap();
/// // The context now holds all info needed for an FFmpeg job.
/// ```
pub mod ffmpeg_context;

/// The **ffmpeg_context_builder** module defines the builder pattern for creating
/// [`FfmpegContext`](ffmpeg_context::FfmpegContext) objects.
///
/// It exposes the [`FfmpegContextBuilder`](ffmpeg_context_builder::FfmpegContextBuilder) struct, which allows you to:
/// - Configure multiple [`Input`](input::Input) and
///   [`Output`](output::Output) streams.
/// - Attach filter descriptions via [`FilterComplex`](crate::core::context::filter_complex::FilterComplex)
///   or inline strings (e.g., `"scale=1280:720"`, `"hue=s=0"`).
/// - Produce a finished `FfmpegContext` that can then be executed by
///   [`FfmpegScheduler`](crate::core::scheduler::ffmpeg_scheduler::FfmpegScheduler).
///
/// # Examples
///
/// ```rust,ignore
/// // 1. Create a builder (usually via FfmpegContext::builder())
/// let builder = FfmpegContext::builder();
///
/// // 2. Add inputs, outputs, and filters
/// let ffmpeg_context = builder
///     .input("input.mp4")
///     .filter_desc("hue=s=0")
///     .output("output.mp4")
///     .build()
///     .expect("Failed to build FfmpegContext");
///
/// // 3. Use `ffmpeg_context` with FfmpegScheduler (e.g., `.start()` and `.wait()`).
/// ```
pub mod ffmpeg_context_builder;

/// The **input** module defines the [`Input`](crate::core::context::input::Input) struct,
/// representing an FFmpeg input source. An input can be:
/// - A file path or URL (e.g., `"video.mp4"`, `rtmp://example.com/live/stream`).
/// - A **custom data source** via a `read_callback` (and optionally `seek_callback`) for
///   advanced scenarios like in-memory buffers or network protocols.
///
/// You can also specify **frame pipelines** to apply custom [`FrameFilter`](crate::core::filter::frame_filter::FrameFilter)
/// transformations **after decoding** but **before** the frames move on to the rest of the pipeline.
///
/// # Example
///
/// ```rust,ignore
/// use ez_ffmpeg::core::context::input::Input;
///
/// // Basic file or network URL:
/// let file_input: Input = "example.mp4".into();
///
/// // Or a custom read callback:
/// let custom_input = Input::new_by_read_callback(|buf| {
///     // Fill `buf` with data from your source
///     // Return the number of bytes read, or negative for errors
///     0
/// });
/// ```
pub mod input;

/// The **output** module defines the [`Output`](crate::core::context::output::Output) struct,
/// representing an FFmpeg output destination. An output may be:
/// - A file path or URL (e.g., `"output.mp4"`, `rtmp://...`).
/// - A **custom write callback** that processes encoded data (e.g., storing it
///   in-memory or sending it over a custom network protocol).
///
/// You can specify additional details such as:
/// - **Container format** (e.g., `"mp4"`, `"flv"`, `"mkv"`).
/// - **Video/Audio/Subtitle codecs** (e.g., `"h264"`, `"aac"`, `"mov_text"`).
/// - **Frame pipelines** to apply [`FrameFilter`](crate::core::filter::frame_filter::FrameFilter)
///   transformations **before encoding**.
///
/// # Example
///
/// ```rust,ignore
/// use ez_ffmpeg::core::context::output::Output;
///
/// // Basic file/URL output:
/// let file_output: Output = "output.mp4".into();
///
/// // Or a custom write callback:
/// let custom_output = Output::new_by_write_callback(|encoded_data| {
///     // Write `encoded_data` somewhere
///     encoded_data.len() as i32
/// }).set_format("mp4");
/// ```
pub mod output;

/// The **filter_complex** module defines the [`FilterComplex`](crate::core::context::filter_complex::FilterComplex)
/// struct, which encapsulates one or more FFmpeg filter descriptions (e.g., `"scale=1280:720"`,
/// `"hue=s=0"`, etc.). You can use `FilterComplex` to construct more advanced or multi-step
/// filter graphs than simple inline strings allow.
///
/// `FilterComplex` can also associate a particular hardware device (e.g., for GPU-based
/// filtering) via `hw_device`.
///
/// # Example
///
/// ```rust,ignore
/// use ez_ffmpeg::core::context::filter_complex::FilterComplex;
///
/// // Build a FilterComplex from a string:
/// let my_filters = FilterComplex::from("scale=1280:720");
///
/// // Optionally specify a hardware device (e.g., "cuda"):
/// // my_filters.set_hw_device("cuda");
/// ```
pub mod filter_complex;

pub(super) mod attachment;
pub(super) mod decoder_stream;
pub(super) mod demuxer;
pub(super) mod encoder_stream;
pub(super) mod filter_graph;
pub(super) mod input_filter;
pub(super) mod muxer;
pub(super) mod obj_pool;
pub(super) mod output_filter;
pub(super) mod pre_mux_queue;

/// The **null_output** module provides a custom null output implementation for FFmpeg
/// that discards all data while supporting seeking.
///
/// It exposes the [`create_null_output`](null_output::create_null_output) function, which returns an
/// [`Output`](crate::Output) object configured to:
/// - Discard all written data, behaving like `/dev/null`.
/// - Maintain a seekable position state using atomic operations for thread-safe, high-performance access.
/// - Support scenarios such as testing or processing streaming inputs (e.g., RTMP) where no output file is needed.
///
/// # Usage Scenario
/// This module is useful when processing FFmpeg input streams without generating an output file, such as
/// when handling RTMP streams that require a seekable output format like MP4, even if the output is discarded.
///
/// # Examples
///
/// ```rust,ignore
/// use ez_ffmpeg::Output;
/// let output: Output = create_null_output();
/// // Pass `output` to an FFmpeg context for processing
/// ```
///
/// # Performance
/// - Utilizes `AtomicU64` with `Relaxed` ordering for lock-free position tracking, ensuring efficient concurrent access.
/// - Write and seek operations are optimized to minimize overhead by avoiding locks.
///
/// # Notes
/// - The default output format is "mp4", but this can be modified using `set_format` as needed.
/// - Write operations assume individual buffers do not exceed `i32::MAX` bytes, which aligns with typical FFmpeg usage.
pub mod null_output;

pub(crate) struct CodecContext {
    inner: *mut AVCodecContext,
}

// SAFETY: CodecContext can be sent to another thread. The raw AVCodecContext pointer
// is only accessed from the thread that owns the CodecContext, and the crate ensures
// single-threaded access to codec operations.
unsafe impl Send for CodecContext {}

impl CodecContext {
    pub(crate) fn new(avcodec_context: *mut AVCodecContext) -> Self {
        Self {
            inner: avcodec_context,
        }
    }

    pub(crate) fn null() -> Self {
        Self { inner: null_mut() }
    }

    pub(crate) fn as_mut_ptr(&self) -> *mut AVCodecContext {
        self.inner
    }

    pub(crate) fn as_ptr(&self) -> *const AVCodecContext {
        self.inner as *const AVCodecContext
    }
}

impl Drop for CodecContext {
    fn drop(&mut self) {
        unsafe {
            avcodec_free_context(&mut self.inner);
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) struct Stream {
    pub(crate) inner: *mut AVStream,
}

// SAFETY: Stream can be sent to another thread. The raw AVStream pointer is owned
// by the parent AVFormatContext, and the crate ensures the format context outlives
// all Stream references.
unsafe impl Send for Stream {}

pub(crate) struct FrameBox {
    pub(crate) frame: ffmpeg_next::Frame,
    // stream copy or filtergraph
    pub(crate) frame_data: FrameData,
}

// SAFETY: FrameBox can be sent to another thread. It contains an ffmpeg_next::Frame
// (which wraps AVFrame) and FrameData, both of which are only accessed from the owning thread.
unsafe impl Send for FrameBox {}

pub fn frame_alloc() -> crate::error::Result<ffmpeg_next::Frame> {
    unsafe {
        let frame = ffmpeg_next::Frame::empty();
        if frame.as_ptr().is_null() {
            return Err(AllocFrameError::OutOfMemory.into());
        }
        Ok(frame)
    }
}

pub fn null_frame() -> ffmpeg_next::Frame {
    unsafe { ffmpeg_next::Frame::wrap(null_mut()) }
}

/// Owned array of global (stream-level) side data entries — the Rust shape of
/// the `AVFrameSideData **side_data / int nb_side_data` pairs that fftools
/// threads through InputFilterPriv, OutputFilterPriv and FrameData
/// (ffmpeg_filter.c, commits e61b9d4094 / 7b18beb477). Entries are
/// deep-cloned in and freed exactly once on drop.
pub(crate) struct SideDataList {
    entries: *mut *mut AVFrameSideData,
    count: i32,
}

impl SideDataList {
    pub(crate) fn new() -> Self {
        Self {
            entries: null_mut(),
            count: 0,
        }
    }

    /// Deep-clones one entry onto the list (flags as in
    /// `av_frame_side_data_clone`). Returns 0 or a negative AVERROR.
    #[cfg(ffmpeg_8_0)]
    pub(crate) fn push_clone(&mut self, sd: *const AVFrameSideData, flags: u32) -> i32 {
        unsafe { av_frame_side_data_clone(&mut self.entries, &mut self.count, sd, flags) }
    }

    pub(crate) fn clear(&mut self) {
        // docs.rs bindings predate av_frame_side_data_free (FFmpeg 7.0), but
        // there the list is provably empty — push_clone is ffmpeg_8_0-gated
        // and that cfg never exists on docs.rs — so skipping the call is the
        // exact semantics, not a stub.
        #[cfg(not(docsrs))]
        unsafe {
            av_frame_side_data_free(&mut self.entries, &mut self.count)
        }
    }

    #[cfg(ffmpeg_8_0)]
    pub(crate) fn len(&self) -> i32 {
        self.count
    }

    /// Raw entry array for FFmpeg parameter structs
    /// (e.g. `AVBufferSrcParameters.side_data`); callees deep-copy what they
    /// need, the list keeps ownership. Takes `&mut self` so handing out a
    /// `*mut` view is justified by the signature, not by convention — which
    /// also keeps the Sync claim honest (shared refs never leak mutability).
    #[cfg(ffmpeg_8_0)]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut *mut AVFrameSideData {
        self.entries
    }

    #[cfg(ffmpeg_8_0)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = *const AVFrameSideData> + '_ {
        (0..self.count)
            .map(|i| unsafe { *self.entries.offset(i as isize) as *const AVFrameSideData })
    }
}

impl Drop for SideDataList {
    fn drop(&mut self) {
        self.clear();
    }
}

// SAFETY: the list exclusively owns its deep-cloned entries. All mutation
// happens on the thread that is building it; once frozen behind an Arc it is
// only read, and readers clone entries out instead of mutating — so both
// moving it across threads and sharing references are sound.
unsafe impl Send for SideDataList {}
unsafe impl Sync for SideDataList {}

#[derive(Clone)]
pub(crate) struct FrameData {
    pub(crate) framerate: Option<AVRational>,
    pub(crate) bits_per_raw_sample: i32,
    pub(crate) input_stream_width: i32,
    pub(crate) input_stream_height: i32,
    /// Owned copy of the decoder's subtitle header (e.g. ASS script info),
    /// shared across fan-out sends without reallocation. Owning the bytes
    /// keeps the header valid after the decoder context is freed.
    pub(crate) subtitle_header: Option<Arc<[u8]>>,

    pub(crate) fg_input_index: usize,

    /// Graph-level global side data snapshot, attached by the filtergraph
    /// output for the frame that opens the encoder and consumed by
    /// `enc_open` on FFmpeg 8+ (fftools FrameData.side_data, 7b18beb477).
    /// Always `None` on FFmpeg 7.x, where the encoder scans frame side data
    /// instead (n7.1 behavior) — hence dead there by design.
    #[cfg_attr(not(ffmpeg_8_0), allow(dead_code))]
    pub(crate) side_data: Option<Arc<SideDataList>>,
}
// Send + Sync are auto-derived. For side_data that rests on SideDataList's
// hand-written unsafe impls (frozen-behind-Arc discipline); every other
// field is plain owned data.

pub(crate) struct PacketBox {
    pub(crate) packet: ffmpeg_next::Packet,
    pub(crate) packet_data: PacketData,
}

// SAFETY: PacketBox can be sent to another thread. It contains an ffmpeg_next::Packet
// and PacketData, both only accessed from the owning thread.
unsafe impl Send for PacketBox {}

// optionally attached as opaque_ref to decoded AVFrames
#[derive(Clone, Copy)]
pub(crate) struct PacketData {
    // demuxer-estimated dts in AV_TIME_BASE_Q,
    // to be used when real dts is missing
    pub(crate) dts_est: i64,
    pub(crate) codec_type: AVMediaType,
    pub(crate) output_stream_index: i32,
    pub(crate) is_copy: bool,
}

pub(crate) fn out_fmt_ctx_free(out_fmt_ctx: *mut AVFormatContext, is_set_write_callback: bool) {
    if out_fmt_ctx.is_null() {
        return;
    }
    unsafe {
        if is_set_write_callback {
            free_output_opaque((*out_fmt_ctx).pb);
        } else if (*out_fmt_ctx).flags & AVFMT_NOFILE == 0 {
            let mut pb = (*out_fmt_ctx).pb;
            if !pb.is_null() {
                avio_closep(&mut pb);
            }
        }
        avformat_free_context(out_fmt_ctx);
    }
}

pub(crate) unsafe fn free_output_opaque(mut avio_ctx: *mut AVIOContext) {
    if avio_ctx.is_null() {
        return;
    }
    if !(*avio_ctx).buffer.is_null() {
        av_freep(&mut (*avio_ctx).buffer as *mut _ as *mut c_void);
    }
    let opaque_ptr = (*avio_ctx).opaque as *mut OutputOpaque;
    if !opaque_ptr.is_null() {
        let _ = Box::from_raw(opaque_ptr);
    }
    avio_context_free(&mut avio_ctx);
}

pub(crate) fn in_fmt_ctx_free(mut in_fmt_ctx: *mut AVFormatContext, is_set_read_callback: bool) {
    if in_fmt_ctx.is_null() {
        return;
    }
    unsafe {
        // Close the input FIRST: the demuxer's read_close may still touch
        // s->pb (the official custom-IO example frees the AVIOContext only
        // after avformat_close_input). With AVFMT_FLAG_CUSTOM_IO the close
        // leaves pb alone, so capture it beforehand and free it after.
        let avio_ctx = if is_set_read_callback {
            (*in_fmt_ctx).pb
        } else {
            null_mut()
        };
        avformat_close_input(&mut in_fmt_ctx);
        free_input_opaque(avio_ctx);
    }
}

pub(crate) unsafe fn free_input_opaque(mut avio_ctx: *mut AVIOContext) {
    if !avio_ctx.is_null() {
        let opaque_ptr = (*avio_ctx).opaque as *mut InputOpaque;
        if !opaque_ptr.is_null() {
            let _ = Box::from_raw(opaque_ptr);
        }
        av_freep(&mut (*avio_ctx).buffer as *mut _ as *mut c_void);
        avio_context_free(&mut avio_ctx);
    }
}

/// RAII guard for a partially-initialized `AVFormatContext` (input or output).
///
/// During `open_input_file` / `open_output_file` the raw context (and, for
/// custom-IO, its `AVIOContext` + callback `Box`) is owned by nobody until a
/// [`Demuxer`]/[`Muxer`] takes it. Any `?`/early return in that window used to
/// leak it. [`arm`](FmtCtxGuard::arm) it once the context is valid; on drop it
/// frees via the same [`in_fmt_ctx_free`]/[`out_fmt_ctx_free`] paths a
/// success-path [`crate::raw::FormatContext`] drop uses — unless
/// [`release`](FmtCtxGuard::release) is called when ownership transfers.
///
/// The teardown path is selected by [`crate::raw::Mode`] (the same discriminant
/// `FormatContext` carries), replacing the two former bool-keyed guards
/// (`OutFmtCtxGuard`/`InFmtCtxGuard`).
///
/// Unlike `FormatContext`, this guard is re-armable and covers contexts that are
/// only *partially* initialized (allocated but not yet opened, or mid custom-IO
/// setup) — which is why it stays a separate type rather than reusing
/// `FormatContext`'s already-opened constructors.
pub(crate) struct FmtCtxGuard {
    ctx: *mut AVFormatContext,
    mode: crate::raw::Mode,
}

impl FmtCtxGuard {
    pub(crate) fn disarmed() -> Self {
        // `mode` is irrelevant while `ctx` is null (Drop no-ops on null).
        Self {
            ctx: null_mut(),
            mode: crate::raw::Mode::Input,
        }
    }

    /// Take ownership of a now-valid context so any early return frees it, with
    /// the teardown path selected by `mode`.
    pub(crate) fn arm(&mut self, ctx: *mut AVFormatContext, mode: crate::raw::Mode) {
        self.ctx = ctx;
        self.mode = mode;
    }

    /// Relinquish ownership (a Demuxer/Muxer/FormatContext now owns the context).
    pub(crate) fn release(&mut self) -> *mut AVFormatContext {
        let ctx = self.ctx;
        self.ctx = null_mut();
        ctx
    }
}

impl Drop for FmtCtxGuard {
    fn drop(&mut self) {
        if self.ctx.is_null() {
            return;
        }
        // Same dispatch as `FormatContext::Drop`.
        match self.mode {
            crate::raw::Mode::Input => in_fmt_ctx_free(self.ctx, false),
            crate::raw::Mode::InputCustomIo => in_fmt_ctx_free(self.ctx, true),
            crate::raw::Mode::Output => out_fmt_ctx_free(self.ctx, false),
            crate::raw::Mode::OutputCustomIo => out_fmt_ctx_free(self.ctx, true),
        }
    }
}

#[allow(dead_code)]
pub(crate) fn type_to_linklabel(media_type: AVMediaType, index: usize) -> Option<String> {
    match media_type {
        AVMediaType::AVMEDIA_TYPE_UNKNOWN => None,
        AVMEDIA_TYPE_VIDEO => Some(format!("{index}:v")),
        AVMEDIA_TYPE_AUDIO => Some(format!("{index}:a")),
        AVMEDIA_TYPE_DATA => Some(format!("{index}:d")),
        AVMEDIA_TYPE_SUBTITLE => Some(format!("{index}:s")),
        AVMEDIA_TYPE_ATTACHMENT => Some(format!("{index}:t")),
        AVMediaType::AVMEDIA_TYPE_NB => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::scheduler::ffmpeg_scheduler::{STATUS_ABORT, STATUS_END, STATUS_RUN};

    fn state_with(status: usize) -> Arc<InterruptState> {
        Arc::new(InterruptState::new(Arc::new(AtomicUsize::new(status))))
    }

    /// Rewinds the grace clock so the window is already expired — no sleeps.
    fn expire_grace(state: &InterruptState) {
        let past = unsafe { av_gettime_relative() } - OUTPUT_END_GRACE_US - 1;
        state.end_grace_start_us.store(past, Ordering::Release);
    }

    #[test]
    fn output_grace_cut_fires_after_the_window() {
        let state = state_with(STATUS_END);
        expire_grace(&state);
        assert!(state.should_interrupt_output());
    }

    #[test]
    fn finalize_guard_holds_the_grace_cut_open() {
        let state = state_with(STATUS_END);
        expire_grace(&state);
        let guard = state.begin_output_finalize();
        assert!(
            !state.should_interrupt_output(),
            "a finalizing output must not be cut by the STATUS_END grace"
        );
        drop(guard);
        assert!(
            state.should_interrupt_output(),
            "after the last finalize guard drops, the expired grace cuts again"
        );
    }

    #[test]
    fn abort_cuts_through_a_finalize_window() {
        let state = state_with(STATUS_ABORT);
        let _guard = state.begin_output_finalize();
        assert!(
            state.should_interrupt_output(),
            "abort() is the hard cancel: it must cut even a finalizing output"
        );
    }

    #[test]
    fn running_scheduler_never_cuts_output() {
        let state = state_with(STATUS_RUN);
        assert!(!state.should_interrupt_output());
    }

    #[test]
    fn overlapping_finalize_windows_hold_until_the_last_drops() {
        let state = state_with(STATUS_END);
        expire_grace(&state);
        let g1 = state.begin_output_finalize();
        let g2 = state.begin_output_finalize();
        drop(g1);
        assert!(
            !state.should_interrupt_output(),
            "one of two overlapping finalize windows dropping must keep the hold"
        );
        drop(g2);
        assert!(state.should_interrupt_output());
    }
}
