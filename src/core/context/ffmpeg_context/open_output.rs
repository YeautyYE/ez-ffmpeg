use super::*;

pub(super) fn open_output_files(
    outputs: &mut Vec<Output>,
    copy_ts: bool,
    interrupt_state: &Arc<crate::core::context::InterruptState>,
) -> Result<Vec<Muxer>> {
    let mut muxs = Vec::new();

    for (i, output) in outputs.iter_mut().enumerate() {
        unsafe {
            let result = open_output_file(i, output, copy_ts, interrupt_state);
            if let Err(e) = result {
                // Already-built muxers free their contexts on drop.
                return Err(e);
            }
            let mux = result.unwrap();
            muxs.push(mux)
        }
    }
    Ok(muxs)
}

#[cfg(docsrs)]
unsafe fn open_output_file(
    index: usize,
    output: &mut Output,
    copy_ts: bool,
    interrupt_state: &Arc<crate::core::context::InterruptState>,
) -> Result<Muxer> {
    Err(Error::Bug)
}

#[cfg(not(docsrs))]
unsafe fn open_output_file(
    index: usize,
    output: &mut Output,
    copy_ts: bool,
    interrupt_state: &Arc<crate::core::context::InterruptState>,
) -> Result<Muxer> {
    // Take the target first: an `Output` is single-use, and the typed
    // discriminant — not URL/callback presence — drives validation and the
    // context/RAII mode below.
    let target = std::mem::replace(
        &mut output.target,
        crate::core::context::output::OutputTarget::Consumed,
    );

    // Packet-sink validation runs BEFORE any generic option validation or
    // format lookup, so sink misconfiguration always surfaces as the
    // documented typed `PacketSinkError` (a bad `set_format` name or an
    // invalid IO buffer size must not win with a generic error first).
    if matches!(
        target,
        crate::core::context::output::OutputTarget::PacketSink(_)
    ) {
        validate_packet_sink_options(output)?;
    }

    // Deferred validation of stored builder options (the setters are
    // infallible and store values as given, like every other option).
    for (name, rate) in [
        ("set_framerate", output.framerate),
        ("set_framerate_max", output.framerate_max),
    ] {
        if let Some(AVRational { num, den }) = rate {
            if num <= 0 || den <= 0 {
                return Err(OpenOutputError::InvalidOption(format!(
                    "{name} requires positive numerator and denominator, got {num}/{den}"
                ))
                .into());
            }
        }
    }
    let io_buffer_size = output
        .io_buffer_size
        .unwrap_or(crate::core::context::DEFAULT_CUSTOM_IO_BUFFER_SIZE);
    if io_buffer_size == 0 || io_buffer_size > i32::MAX as usize {
        return Err(OpenOutputError::InvalidOption(format!(
            "set_io_buffer_size must be in 1..=i32::MAX, got {io_buffer_size}"
        ))
        .into());
    }
    if output.max_muxing_queue_size == 0 {
        return Err(
            OpenOutputError::InvalidOption("set_max_muxing_queue_size must be > 0".into()).into(),
        );
    }
    if output.muxing_queue_data_threshold == 0 {
        return Err(OpenOutputError::InvalidOption(
            "set_muxing_queue_data_threshold must be > 0".into(),
        )
        .into());
    }
    // -vf + -c:v copy: the filter would need a decode/encode cycle that copy
    // skips, so the CLI refuses the pair (ffmpeg_mux_init.c streamcopy_init)
    // and so do we — silently dropping the filter would corrupt intent. Copy
    // stream maps covering a video stream are caught later, in map_manual,
    // once specifiers are expanded against the opened inputs.
    if let (Some(filter), Some("copy")) = (&output.video_filter, output.video_codec.as_deref()) {
        return Err(OpenOutputError::FilterWithStreamCopy(filter.clone()).into());
    }

    let mut out_fmt_ctx = null_mut();
    // Frees out_fmt_ctx (and, for custom IO, its AVIO + callback box) on any
    // early return until the Muxer takes ownership below.
    let mut ctx_guard = crate::core::context::FmtCtxGuard::disarmed();
    let format = get_format(&output.format)?;
    // Prepare the context per target. Callback ownership moves into the AVIO
    // opaque here; the URL and the packet sink ride along for the naming /
    // Muxer wiring below.
    let prepared = match target {
        crate::core::context::output::OutputTarget::Consumed => {
            // An Output cannot be built twice; its callbacks are gone.
            error!(target: LOG_TARGET, "output was already consumed by a previous build");
            return Err(OpenOutputError::InvalidSink.into());
        }
        crate::core::context::output::OutputTarget::PacketSink(sink) => {
            // Packet sink: no container is written and no I/O exists.
            // Allocate a real-but-never-written mp4 context purely for
            // parameter plumbing (`avformat_new_stream`, codecpar
            // finalization, time-base adoption). No pb is installed and
            // neither header nor trailer is ever written; the encoder-visible
            // flags come from the explicit PacketSinkPolicy below, never from
            // this container's own flags.
            let dummy = get_format(&Some("mp4".to_string()))?;
            let ret = avformat_alloc_output_context2(&mut out_fmt_ctx, dummy, null(), null());
            if out_fmt_ctx.is_null() {
                warn!(target: LOG_TARGET, "Error initializing the parameter context for packet_sink");
                return Err(AllocOutputContextError::from(ret).into());
            }
            ctx_guard.arm(out_fmt_ctx, crate::raw::Mode::Output);
            PreparedTarget::PacketSink(sink)
        }
        crate::core::context::output::OutputTarget::CustomIo {
            write: write_callback,
        } => {
            let avio_ctx_buffer_size = io_buffer_size;
            let mut avio_ctx_buffer = av_malloc(avio_ctx_buffer_size);
            if avio_ctx_buffer.is_null() {
                return Err(OpenOutputError::OutOfMemory.into());
            }

            let have_seek_callback = output.seek_callback.is_some();
            let input_opaque = Box::new(OutputOpaque {
                write: write_callback,
                seek: output.seek_callback.take(),
                poisoned: false,
            });
            let opaque = Box::into_raw(input_opaque) as *mut libc::c_void;

            let avio_ctx = avio_alloc_context(
                avio_ctx_buffer as *mut libc::c_uchar,
                avio_ctx_buffer_size as i32,
                1,
                opaque,
                None,
                Some(write_packet_wrapper),
                if have_seek_callback {
                    Some(seek_output_packet_wrapper)
                } else {
                    None
                },
            );
            if avio_ctx.is_null() {
                av_freep(&mut avio_ctx_buffer as *mut _ as *mut c_void);
                // avio_alloc_context never took ownership: reclaim the Box.
                let _ = Box::from_raw(opaque as *mut OutputOpaque);
                return Err(OpenOutputError::OutOfMemory.into());
            }

            let ret = avformat_alloc_output_context2(&mut out_fmt_ctx, format, null(), null());
            if out_fmt_ctx.is_null() {
                warn!(target: LOG_TARGET, "Error initializing the muxer for write_callback");
                // Reclaims buffer, callback Box and the AVIOContext itself.
                crate::core::context::free_output_opaque(avio_ctx);
                return Err(AllocOutputContextError::from(ret).into());
            }

            if !have_seek_callback && output_requires_seek(out_fmt_ctx) {
                crate::core::context::free_output_opaque(avio_ctx);
                avformat_free_context(out_fmt_ctx);
                warn!(target: LOG_TARGET, "The output format supports seeking, but no seek callback is provided. This may cause issues.");
                return Err(OpenOutputError::SeekFunctionMissing.into());
            }

            (*out_fmt_ctx).pb = avio_ctx;
            (*out_fmt_ctx).flags |= AVFMT_FLAG_CUSTOM_IO;
            ctx_guard.arm(out_fmt_ctx, crate::raw::Mode::OutputCustomIo);
            PreparedTarget::CustomIo
        }
        crate::core::context::output::OutputTarget::Url(url) => {
            let url_cstr = if url == "-" {
                CString::new("pipe:")?
            } else {
                CString::new(url.as_str())?
            };
            let ret =
                avformat_alloc_output_context2(&mut out_fmt_ctx, format, null(), url_cstr.as_ptr());
            if out_fmt_ctx.is_null() {
                warn!(target: LOG_TARGET, "Error initializing the muxer for {url}");
                return Err(AllocOutputContextError::from(ret).into());
            }
            ctx_guard.arm(out_fmt_ctx, crate::raw::Mode::Output);

            // Install the interrupt callback now so it is already in place when the
            // output file is opened at runtime mux initialization: stop()/abort()
            // must be able to break a blocking network open and any later write on
            // this output (matches ffmpeg_mux_init.c:3326,3371).
            (*out_fmt_ctx).interrupt_callback = ffmpeg_sys_next::AVIOInterruptCB {
                callback: Some(crate::core::context::output_interrupt_cb),
                opaque: Arc::as_ptr(interrupt_state) as *mut c_void,
            };

            // The output file is NOT opened here. avio_open2(AVIO_FLAG_WRITE) creates
            // and truncates the target (the file protocol opens O_CREAT|O_TRUNC), so
            // opening at build() time would destroy an existing output file even when
            // the caller only builds to validate a config, or when a later build
            // check fails. It is opened during runtime mux initialization instead
            // (see open_muxer_output in mux_task — from the mux worker for a streamed
            // output, or from the streamless dispatch for a zero-stream one), keeping
            // build() free of output-file side effects; the still-null `pb` closes as
            // a no-op if the job is torn down before it is opened.
            PreparedTarget::Url(url)
        }
    };

    let recording_time_us = match output.stop_time_us {
        None => output.recording_time_us,
        Some(stop_time_us) => {
            let start_time_us = output.start_time_us.unwrap_or(0);
            if stop_time_us <= start_time_us {
                error!(target: LOG_TARGET, "stop_time_us value smaller than start_time_us; aborting.");
                return Err(OpenOutputError::InvalidArgument.into());
            } else {
                Some(stop_time_us - start_time_us)
            }
        }
    };

    let url = match &prepared {
        PreparedTarget::Url(url) => url.clone(),
        PreparedTarget::CustomIo => format!("write_callback[{index}]"),
        PreparedTarget::PacketSink(_) => format!("packet_sink[{index}]"),
    };

    let video_codec_opts = convert_options(output.video_codec_opts.clone())?;
    let audio_codec_opts = convert_options(output.audio_codec_opts.clone())?;
    let subtitle_codec_opts = convert_options(output.subtitle_codec_opts.clone())?;
    let format_opts = convert_options(output.format_opts.clone())?;
    let format_opts = maybe_enable_image2_update(
        out_fmt_ctx,
        match &prepared {
            PreparedTarget::Url(url) => Some(url.as_str()),
            _ => None,
        },
        output.max_video_frames,
        format_opts,
    );

    // Parse pix_fmt string to AVPixelFormat
    // FFmpeg CLI also fails on invalid format names (e.g., `ffmpeg -pix_fmt foobar` errors with
    // "Unknown pixel format requested: foobar"). Valid but encoder-incompatible formats are
    // auto-converted by the filter graph, matching FFmpeg behavior.
    let pix_fmt = match &output.pix_fmt {
        Some(fmt_str) => {
            let cstr = CString::new(fmt_str.as_str())?;
            let pf = av_get_pix_fmt(cstr.as_ptr());
            if pf == AV_PIX_FMT_NONE {
                return Err(OpenOutputError::UnknownPixelFormat(fmt_str.clone()).into());
            } else {
                Some(pf)
            }
        }
        None => None,
    };

    // Parse the deferred forced-keyframe spec; the setter stores it raw so
    // a typo fails the build here, not mid-chain in user code.
    let forced_kf_pts = match &output.forced_kf_spec {
        Some(spec) => Some(
            crate::core::context::output::parse_forced_key_frames(spec)
                .map_err(OpenOutputError::InvalidOption)?,
        ),
        None => None,
    };

    // Resolve the sample-format name like pix_fmt above: same failure mode
    // as FFmpeg CLI's `-sample_fmt foobar`. A name with an interior NUL is
    // just as unknown as a misspelled one — map it to the same typed error
    // instead of letting the CString conversion pick a generic one.
    let audio_sample_fmt = match &output.audio_sample_fmt {
        Some(name) => {
            let cstr = CString::new(name.as_str())
                .map_err(|_| OpenOutputError::UnknownSampleFormat(name.clone()))?;
            let sf = ffmpeg_sys_next::av_get_sample_fmt(cstr.as_ptr());
            if sf == ffmpeg_sys_next::AVSampleFormat::AV_SAMPLE_FMT_NONE {
                return Err(OpenOutputError::UnknownSampleFormat(name.clone()).into());
            }
            Some(sf)
        }
        None => None,
    };

    // Ownership of out_fmt_ctx transfers to the Muxer. `release_into()` disarms
    // the guard and wraps the pointer in a `FormatContext` carrying the SAME
    // teardown `Mode` the guard was armed with — the mode is decided once, at
    // the target dispatch above, never re-inferred from URL presence (a
    // packet sink has no URL yet is NOT custom AVIO). There is no `?` between
    // here and construction, so the pointer always reaches its next Drop-home.
    // Materialize the per-media-type BSF chains into NUL-validated CStrings
    // now (build time): a NUL byte surfaces as the existing NulError -> Error
    // path here, before any thread is spawned. Empty strings were already
    // normalized to None by the Output setters.
    let bsf_chains = crate::core::context::muxer::StreamBsfChains {
        video: output.video_bsf.as_deref().map(CString::new).transpose()?,
        audio: output.audio_bsf.as_deref().map(CString::new).transpose()?,
        subtitle: output
            .subtitle_bsf
            .as_deref()
            .map(CString::new)
            .transpose()?,
    };

    let fc = ctx_guard.release_into();
    let mut mux = Muxer::new(
        url,
        fc,
        output.frame_pipelines.take(),
        output.stream_map_specs.clone(),
        output.stream_maps.clone(),
        output.video_codec.clone(),
        output.audio_codec.clone(),
        output.subtitle_codec.clone(),
        bsf_chains,
        output.start_time_us,
        recording_time_us,
        output.shortest,
        output.shortest_buf_duration_us,
        output.framerate,
        output.framerate_max,
        output.vsync_method,
        output.bits_per_raw_sample,
        output.audio_sample_rate,
        output.audio_channels,
        audio_sample_fmt,
        output.video_qscale,
        output.audio_qscale,
        forced_kf_pts,
        output.max_video_frames,
        output.max_audio_frames,
        output.max_subtitle_frames,
        video_codec_opts,
        audio_codec_opts,
        subtitle_codec_opts,
        format_opts,
        copy_ts,
        output.global_metadata.clone(),
        output.stream_metadata.clone(),
        output.chapter_metadata.clone(),
        output.program_metadata.clone(),
        output.metadata_map.clone(),
        output.auto_copy_metadata,
        output.video_disable,
        output.audio_disable,
        output.subtitle_disable,
        output.data_disable,
        pix_fmt,
        output.video_filter.clone(),
        crate::core::context::pre_mux_queue::PreMuxQueueConfig {
            max_packets: output.max_muxing_queue_size,
            data_threshold: output.muxing_queue_data_threshold,
        },
        output.sws_opts.clone(),
        output.swr_opts.clone(),
        std::mem::take(&mut output.attachments),
        interrupt_state.clone(),
    );

    if let PreparedTarget::PacketSink(sink) = prepared {
        // PacketSinkPolicy: the encoder-visible muxing flags are pinned by
        // the sink tier and overwrite the dummy container's snapshot —
        // GLOBAL_HEADER (out-of-band codec configuration) and the vsync
        // projection are policy decisions, not container accidents.
        mux.oformat_flags =
            crate::core::packet_sink::PacketSinkPolicy::for_tier(sink.tier).oformat_flags();
        mux.packet_sink = Some(sink);
    }

    Ok(mux)
}

/// The output target after context preparation: callbacks have moved into
/// the AVIO opaque; what remains rides to the Muxer wiring (display name,
/// image2 detection, sink handoff).
enum PreparedTarget {
    Url(String),
    CustomIo,
    PacketSink(crate::core::packet_sink::PacketSink),
}

/// Build-time validation for packet-sink outputs: every option that only
/// makes sense for a written container is a typed configuration error.
/// Runs before the generic option validation, so these typed errors always
/// win. Setter *use* is what is rejected (`set_io_buffer_size` stores
/// `Some`, even when set to the default value).
fn validate_packet_sink_options(output: &Output) -> Result<()> {
    use crate::error::PacketSinkError;
    let unsupported: &[(&'static str, bool)] = &[
        ("set_format", output.format.is_some()),
        ("set_seek_callback", output.seek_callback.is_some()),
        ("set_io_buffer_size", output.io_buffer_size.is_some()),
        ("set_video_bsf", output.video_bsf.is_some()),
        ("set_audio_bsf", output.audio_bsf.is_some()),
        ("set_subtitle_bsf", output.subtitle_bsf.is_some()),
        ("set_format_opt", output.format_opts.is_some()),
        ("add_attachment", !output.attachments.is_empty()),
        ("set_subtitle_codec", output.subtitle_codec.is_some()),
        // A packet sink writes no container, so container metadata can never
        // land anywhere; silently accepting it would misrepresent delivery.
        ("add_metadata", output.global_metadata.is_some()),
        (
            "add_stream_metadata",
            !output.stream_metadata.is_empty(),
        ),
        (
            "add_chapter_metadata",
            !output.chapter_metadata.is_empty(),
        ),
        (
            "add_program_metadata",
            !output.program_metadata.is_empty(),
        ),
        ("map_metadata", !output.metadata_map.is_empty()),
        // auto_copy_metadata only governs container metadata propagation;
        // toggling it on a sink is a configuration mistake like the rest.
        ("disable_auto_copy_metadata", !output.auto_copy_metadata),
    ];
    for (option, set) in unsupported {
        if *set {
            return Err(PacketSinkError::UnsupportedOption(option).into());
        }
    }
    if output.video_codec.as_deref() == Some("copy")
        || output.audio_codec.as_deref() == Some("copy")
    {
        return Err(PacketSinkError::StreamCopyUnsupported.into());
    }
    // The strict tier owns AV_CODEC_FLAG_GLOBAL_HEADER (out-of-band codec
    // configuration is the product). A user-supplied `flags` codec option is
    // applied AFTER the policy flag and can clear or replace it (both
    // `flags=-global_header` and an absolute `flags=x` assignment), so it is
    // rejected up front rather than failing later with MissingExtradata.
    for opts in [&output.video_codec_opts, &output.audio_codec_opts]
        .into_iter()
        .flatten()
    {
        if opts.keys().any(|k| k == "flags") {
            return Err(PacketSinkError::UnsupportedOption(
                "the 'flags' codec option (it can clear the global_header policy)",
            )
            .into());
        }
    }
    Ok(())
}

fn get_format(format_option: &Option<String>) -> Result<*const AVOutputFormat> {
    match format_option {
        None => Ok(null()),
        Some(format_str) => unsafe {
            let mut format_cstr = CString::new(format_str.to_string())?;
            let mut format = av_guess_format(format_cstr.as_ptr(), null(), null());
            if format.is_null() {
                format_cstr = CString::new(format!("tmp.{format_str}"))?;
                format = av_guess_format(null(), format_cstr.as_ptr(), null());
            }
            if format.is_null() {
                return Err(OpenOutputError::FormatUnsupported(format_str.to_string()).into());
            }
            Ok(format)
        },
    }
}

unsafe fn output_requires_seek(fmt_ctx: *mut AVFormatContext) -> bool {
    if fmt_ctx.is_null() {
        return false;
    }

    let mut format_name = "unknown".to_string();

    if !(*fmt_ctx).oformat.is_null() {
        let oformat = (*fmt_ctx).oformat;
        format_name = CStr::from_ptr((*oformat).name)
            .to_string_lossy()
            .into_owned();
        let flags = (*oformat).flags;
        let no_file = flags & AVFMT_NOFILE != 0;
        let global_header = flags & AVFMT_GLOBALHEADER != 0;

        log::debug!(target: LOG_TARGET,
            "Output format '{format_name}' - No file: {}, Global header: {}",
            if no_file { "True" } else { "False" },
            if global_header { "True" } else { "False" }
        );

        // List of formats that typically require seeking
        let format_names: Vec<&str> = format_name.split(',').collect();
        if format_names
            .iter()
            .any(|&f| matches!(f, "mp4" | "mov" | "mkv" | "avi" | "flac" | "ogg" | "webm"))
        {
            log::debug!(target: LOG_TARGET, "Output format '{format_name}' typically requires seeking.");
            return true;
        }

        // List of streaming formats that do not require seeking
        if format_names.iter().any(|&f| {
            matches!(
                f,
                "mpegts" | "hls" | "m3u8" | "udp" | "rtp" | "rtp_mpegts" | "http" | "srt"
            )
        }) {
            log::debug!(target: LOG_TARGET, "Output format '{format_name}' does not typically require seeking.");
            return false;
        }

        // Special handling for FLV format
        if format_name == "flv" {
            log::debug!(target: LOG_TARGET, "Output format 'flv' detected. It is highly recommended to set `seek_callback()` to avoid potential issues with 'Failed to update header with correct duration' and 'Failed to update header with correct filesize'.");
            return false;
        }

        // If AVFMT_NOFILE is set, the format does not use standard file I/O and may not need seeking
        if no_file {
            log::debug!(target: LOG_TARGET,
                "Output format '{format_name}' uses AVFMT_NOFILE. Seeking is likely unnecessary."
            );
            return false;
        }

        // If the format uses global headers, it typically means the codec requires a separate metadata section
        if global_header {
            log::debug!(target: LOG_TARGET,
                "Output format '{format_name}' uses AVFMT_GLOBALHEADER. Seeking may be required."
            );
            return true;
        }
    } else {
        log::debug!(target: LOG_TARGET, "Output format is null. Cannot determine if seeking is required.");
    }

    // Default case: assume seeking is not required
    log::debug!(target: LOG_TARGET, "Output format '{format_name}' does not match any known rules. Assuming seeking is not required.");
    false
}

pub(in crate::core::context) struct OutputOpaque {
    pub(super) write: Box<dyn FnMut(&[u8]) -> i32 + Send>,
    pub(super) seek: Option<Box<dyn FnMut(i64, i32) -> i64 + Send>>,
    /// See [`InputOpaque::poisoned`].
    pub(super) poisoned: bool,
}

pub(super) unsafe extern "C" fn write_packet_wrapper(
    opaque: *mut libc::c_void,
    buf: *const u8,
    buf_size: libc::c_int,
) -> libc::c_int {
    // buf_size is a C int: a non-positive value must not be cast to usize
    // (it would produce a giant slice length).
    if buf.is_null() || buf_size <= 0 {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
    }
    let context = &mut *(opaque as *mut OutputOpaque);
    if context.poisoned {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
    }

    let slice = std::slice::from_raw_parts(buf, buf_size as usize);

    // The user write closure runs across the extern "C" boundary: an unwinding
    // panic here is UB (or a hard process abort under the default Rust ABI), so
    // contain it and surface a normal I/O error to FFmpeg instead. The panic
    // also poisons the opaque: the closure's state is torn, and some muxers
    // ignore individual I/O failures (mov ignores the trailer seek result), so
    // only failing every subsequent callback reliably fails the job.
    //
    // Short writes are resubmitted: avio treats any non-negative return as
    // "the whole buffer went out", so a closure that wrote only part of the
    // slice (an io::Write-style sink, a socket under pressure) used to lose
    // the remainder SILENTLY — a corrupt file that still reports success.
    // Loop on the remainder like io::Write::write_all; zero-progress and
    // over-claimed returns become EIO.
    let mut written = 0usize;
    while written < slice.len() {
        let remaining = &slice[written..];
        let ret = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (context.write)(remaining)
        })) {
            Ok(ret) => ret,
            Err(_) => {
                context.poisoned = true;
                return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
            }
        };
        if ret < 0 {
            return ret;
        }
        if ret == 0 || ret as usize > remaining.len() {
            // No progress (write_all semantics: a Ok(0) sink is broken) or a
            // forged length past the buffer: both are I/O faults.
            return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
        }
        written += ret as usize;
    }
    buf_size
}

unsafe extern "C" fn seek_output_packet_wrapper(
    opaque: *mut libc::c_void,
    offset: i64,
    whence: libc::c_int,
) -> i64 {
    let context = &mut *(opaque as *mut OutputOpaque);
    if context.poisoned {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
    }

    if let Some(seek_func) = &mut context.seek {
        // Contain panics across the extern "C" boundary and poison the opaque;
        // EIO instead of ESPIPE for the same reasons as the input-side seek.
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            (*seek_func)(offset, whence)
        })) {
            Ok(ret) => ret,
            Err(_) => {
                context.poisoned = true;
                ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64
            }
        }
    } else {
        ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64
    }
}

/// Single-image convenience, equivalent to `ffmpeg ... -frames:v 1 -update 1`.
///
/// Writing one frame to an image2 output whose filename has no `%d` sequence
/// pattern makes the muxer warn on the first frame and hard-fail on a second
/// (libavformat/img2enc.c). With `max_video_frames == Some(1)` the
/// single-image intent is explicit, so enable the muxer's `update` mode
/// automatically — unless the user already configured a conflicting image2
/// option (`update`, `strftime`, `frame_pts`) themselves. Multi-frame
/// outputs are left untouched, keeping FFmpeg's missing-pattern protection.
fn maybe_enable_image2_update(
    out_fmt_ctx: *mut AVFormatContext,
    url: Option<&str>,
    max_video_frames: Option<i64>,
    format_opts: Option<HashMap<CString, CString>>,
) -> Option<HashMap<CString, CString>> {
    if max_video_frames != Some(1) {
        return format_opts;
    }
    // A filename carrying a sequence pattern ('%03d', strftime '%'-codes, …)
    // must keep image2's pattern expansion: update mode would write the
    // pattern string as a literal filename. Only plain URLs qualify;
    // write-callback outputs (no URL) are left untouched.
    let Some(url) = url else {
        return format_opts;
    };
    if url.contains('%') {
        return format_opts;
    }
    // SAFETY: out_fmt_ctx was successfully allocated by
    // avformat_alloc_output_context2 earlier in open_output_file and is not
    // freed before Muxer::new takes ownership; oformat/name are read-only
    // static muxer metadata.
    let is_image2 = unsafe {
        let oformat = (*out_fmt_ctx).oformat;
        !oformat.is_null()
            && !(*oformat).name.is_null()
            && std::ffi::CStr::from_ptr((*oformat).name).to_bytes() == b"image2"
    };
    if !is_image2 {
        return format_opts;
    }

    let mut opts = format_opts.unwrap_or_default();
    let user_configured = opts
        .keys()
        .any(|key| matches!(key.to_bytes(), b"update" | b"strftime" | b"frame_pts"));
    if !user_configured {
        info!(target: LOG_TARGET, "single-image output detected (max_video_frames=1): enabling image2 'update' mode");
        // Both literals are NUL-free; unwrap cannot fail.
        opts.insert(CString::new("update").unwrap(), CString::new("1").unwrap());
    }
    Some(opts)
}
