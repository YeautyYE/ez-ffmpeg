use super::*;

pub(in crate::core::context) struct InputOpaque {
    pub(super) read: Box<dyn FnMut(&mut [u8]) -> i32 + Send>,
    pub(super) seek: Option<Box<dyn FnMut(i64, i32) -> i64 + Send>>,
    /// Set when a user callback panicked. `catch_unwind` keeps the panic from
    /// crossing the extern "C" boundary, but the closure's captured state is
    /// torn mid-update; calling it again could silently corrupt the stream
    /// (e.g. a cursor advanced before the panic). Once poisoned, every later
    /// callback fails with EIO so the job errors out instead.
    pub(super) poisoned: bool,
}

pub(super) unsafe extern "C" fn read_packet_wrapper(
    opaque: *mut libc::c_void,
    buf: *mut u8,
    buf_size: libc::c_int,
) -> libc::c_int {
    // buf_size is a C int: a non-positive value must not be cast to usize
    // (it would produce a giant slice length).
    if buf.is_null() || buf_size <= 0 {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
    }

    let context = &mut *(opaque as *mut InputOpaque);
    if context.poisoned {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
    }

    let slice = std::slice::from_raw_parts_mut(buf, buf_size as usize);

    // Contain panics across the extern "C" boundary and poison the opaque
    // (see write_packet_wrapper).
    let ret = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (context.read)(slice)))
    {
        Ok(ret) => ret,
        Err(_) => {
            context.poisoned = true;
            return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
        }
    };
    // A read callback that reports more bytes than the buffer holds would make
    // FFmpeg advance buf_end past the allocation and read out of bounds; clamp
    // a bogus positive length to an I/O error. Negative error codes and any
    // length within the buffer pass through unchanged.
    if ret > buf_size {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO);
    }
    ret
}

pub(super) unsafe extern "C" fn seek_input_packet_wrapper(
    opaque: *mut libc::c_void,
    offset: i64,
    whence: libc::c_int,
) -> i64 {
    let context = &mut *(opaque as *mut InputOpaque);
    if context.poisoned {
        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
    }

    if let Some(seek_func) = &mut context.seek {
        // Contain panics across the extern "C" boundary and poison the opaque
        // (see write_packet_wrapper). A panic maps to EIO, not ESPIPE: ESPIPE
        // means "not seekable" and lets FFmpeg fall back to non-seeking modes,
        // which would mask the torn closure state instead of failing the job.
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

pub(super) fn open_input_files(
    inputs: &mut Vec<Input>,
    copy_ts: bool,
    interrupt_state: &Arc<crate::core::context::InterruptState>,
) -> Result<Vec<Demuxer>> {
    let mut demuxs = Vec::new();
    for (i, input) in inputs.iter_mut().enumerate() {
        unsafe {
            let result = open_input_file(i, input, copy_ts, interrupt_state);
            if let Err(e) = result {
                // Already-built demuxers free their contexts on drop.
                return Err(e);
            }
            let demux = result.unwrap();
            demuxs.push(demux)
        }
    }
    Ok(demuxs)
}

#[cfg(docsrs)]
unsafe fn open_input_file(
    index: usize,
    input: &mut Input,
    copy_ts: bool,
    interrupt_state: &Arc<crate::core::context::InterruptState>,
) -> Result<Demuxer> {
    Err(Error::Bug)
}

#[cfg(not(docsrs))]
unsafe fn open_input_file(
    index: usize,
    input: &mut Input,
    copy_ts: bool,
    interrupt_state: &Arc<crate::core::context::InterruptState>,
) -> Result<Demuxer> {
    // Deferred validation of stored builder options (the setters are
    // infallible and store values as given, like every other option).
    if let Some((num, den)) = input.framerate {
        if num <= 0 || den <= 0 {
            return Err(OpenInputError::InvalidOption(format!(
                "set_framerate requires positive numerator and denominator, got {num}/{den}"
            ))
            .into());
        }
    }
    if let Some(scale) = input.ts_scale {
        if !scale.is_finite() || scale <= 0.0 {
            return Err(OpenInputError::InvalidOption(format!(
                "set_ts_scale requires a positive finite value, got {scale}"
            ))
            .into());
        }
    }
    if input.io_buffer_size == 0 || input.io_buffer_size > i32::MAX as usize {
        return Err(OpenInputError::InvalidOption(format!(
            "set_io_buffer_size must be in 1..=i32::MAX, got {}",
            input.io_buffer_size
        ))
        .into());
    }

    let mut in_fmt_ctx = avformat_alloc_context();
    if in_fmt_ctx.is_null() {
        return Err(OpenInputError::OutOfMemory.into());
    }
    // Frees in_fmt_ctx on early returns in the pre-open window (before the
    // branches' own close_input paths take over). Disarmed at each handoff.
    let mut ctx_guard = crate::core::context::FmtCtxGuard::disarmed();
    ctx_guard.arm(in_fmt_ctx, crate::raw::Mode::Input);

    // Interrupt callback: lets stop()/abort() break a blocking open, read or
    // find_stream_info on this input (fftools decode_interrupt_cb).
    (*in_fmt_ctx).interrupt_callback = ffmpeg_sys_next::AVIOInterruptCB {
        callback: Some(crate::core::context::input_interrupt_cb),
        opaque: Arc::as_ptr(interrupt_state) as *mut c_void,
    };

    let recording_time_us = match input.stop_time_us {
        None => input.recording_time_us,
        Some(stop_time_us) => {
            let start_time_us = input.start_time_us.unwrap_or(0);
            if stop_time_us <= start_time_us {
                error!(target: LOG_TARGET, "stop_time_us value smaller than start_time_us; aborting.");
                return Err(OpenOutputError::InvalidArgument.into());
            } else {
                Some(stop_time_us - start_time_us)
            }
        }
    };

    let file_iformat = if let Some(format) = &input.format {
        let format_cstr = CString::new(format.clone())?;

        let file_iformat = ffmpeg_sys_next::av_find_input_format(format_cstr.as_ptr());
        if file_iformat.is_null() {
            error!(target: LOG_TARGET, "Unknown input format: '{format}'");
            return Err(OpenInputError::InvalidFormat(format.clone()).into());
        }
        file_iformat
    } else {
        null()
    };

    let input_opts = convert_options(input.input_opts.clone())?;
    // Guard owns the dict on every path: avformat_open_input reallocates it
    // to hold unrecognized entries, which leaked on all early returns.
    let mut input_opts = DictGuard::new(hashmap_to_avdictionary(&input_opts));

    let mut injected_scan_all_pmts = false;
    // The branches below own their cleanup on error; release before entering.
    ctx_guard.release();
    match &input.url {
        None => {
            if input.read_callback.is_none() {
                error!(target: LOG_TARGET, "input url and read_callback is none.");
                avformat_close_input(&mut in_fmt_ctx);
                return Err(OpenInputError::InvalidSource.into());
            }

            let avio_ctx_buffer_size = input.io_buffer_size;
            let mut avio_ctx_buffer = av_malloc(avio_ctx_buffer_size);
            if avio_ctx_buffer.is_null() {
                avformat_close_input(&mut in_fmt_ctx);
                return Err(OpenInputError::OutOfMemory.into());
            }

            let have_seek_callback = input.seek_callback.is_some();
            let input_opaque = Box::new(InputOpaque {
                read: input.read_callback.take().unwrap(),
                seek: input.seek_callback.take(),
                poisoned: false,
            });
            let opaque = Box::into_raw(input_opaque) as *mut libc::c_void;

            let avio_ctx = avio_alloc_context(
                avio_ctx_buffer as *mut libc::c_uchar,
                avio_ctx_buffer_size as i32,
                0,
                opaque,
                Some(read_packet_wrapper),
                None,
                if have_seek_callback {
                    Some(seek_input_packet_wrapper)
                } else {
                    None
                },
            );
            if avio_ctx.is_null() {
                av_freep(&mut avio_ctx_buffer as *mut _ as *mut c_void);
                // avio_alloc_context never took ownership: reclaim the Box.
                let _ = Box::from_raw(opaque as *mut InputOpaque);
                avformat_close_input(&mut in_fmt_ctx);
                return Err(OpenInputError::OutOfMemory.into());
            }

            (*in_fmt_ctx).pb = avio_ctx;
            (*in_fmt_ctx).flags = AVFMT_FLAG_CUSTOM_IO;

            let ret = avformat_open_input(
                &mut in_fmt_ctx,
                null(),
                file_iformat,
                input_opts.as_double_ptr(),
            );
            if ret < 0 {
                // close_input first: read_close may still touch s->pb. The
                // helper also reclaims the callback Box, which leaked here.
                avformat_close_input(&mut in_fmt_ctx);
                crate::core::context::free_input_opaque(avio_ctx);
                return Err(OpenInputError::from(ret).into());
            }

            if let Err(e) = find_stream_info_if_enabled(in_fmt_ctx, input, index) {
                avformat_close_input(&mut in_fmt_ctx);
                crate::core::context::free_input_opaque(avio_ctx);
                return Err(e);
            }

            // The rawvideo demuxer over a read callback is purely sequential and
            // never seeks on its own. For such an input the generic probe in
            // `input_requires_seek` would run a destructive seek test that reads
            // and discards frames through the callback — blocking a live push
            // source at open, or mis-reporting a finite one depending on
            // buffer/frame sizes. Skip the probe for rawvideo and reject it only
            // when the caller asked for a reposition (`start_time_us` /
            // `stream_loop`) that cannot be honored without a seek callback.
            if !have_seek_callback {
                if format_is_rawvideo(in_fmt_ctx) {
                    let wants_reposition =
                        input.start_time_us.is_some() || input.stream_loop.unwrap_or(0) != 0;
                    if wants_reposition {
                        avformat_close_input(&mut in_fmt_ctx);
                        crate::core::context::free_input_opaque(avio_ctx);
                        warn!(target: LOG_TARGET, "A rawvideo callback input requested repositioning (start_time_us/stream_loop), which requires a seek callback that was not provided.");
                        return Err(OpenInputError::SeekFunctionMissing.into());
                    }
                } else if input_requires_seek(in_fmt_ctx) {
                    avformat_close_input(&mut in_fmt_ctx);
                    crate::core::context::free_input_opaque(avio_ctx);
                    warn!(target: LOG_TARGET, "The input format supports seeking, but no seek callback is provided. This may cause issues.");
                    return Err(OpenInputError::SeekFunctionMissing.into());
                }
            }
        }
        Some(url) => {
            // Guard the pre-open window (CString/dict `?`); released before the
            // manual close_input paths below take over.
            ctx_guard.arm(in_fmt_ctx, crate::raw::Mode::Input);
            let url_cstr = CString::new(url.as_str())?;

            let scan_all_pmts_key = CString::new("scan_all_pmts")?;
            if ffmpeg_sys_next::av_dict_get(
                input_opts.as_ptr(),
                scan_all_pmts_key.as_ptr(),
                null(),
                ffmpeg_sys_next::AV_DICT_MATCH_CASE,
            )
            .is_null()
            {
                let scan_all_pmts_value = CString::new("1")?;
                ffmpeg_sys_next::av_dict_set(
                    input_opts.as_double_ptr(),
                    scan_all_pmts_key.as_ptr(),
                    scan_all_pmts_value.as_ptr(),
                    ffmpeg_sys_next::AV_DICT_DONT_OVERWRITE,
                );
                injected_scan_all_pmts = true;
            };
            (*in_fmt_ctx).flags |= ffmpeg_sys_next::AVFMT_FLAG_NONBLOCK;

            // From here the manual close_input paths below own cleanup.
            ctx_guard.release();
            let ret = avformat_open_input(
                &mut in_fmt_ctx,
                url_cstr.as_ptr(),
                file_iformat,
                input_opts.as_double_ptr(),
            );
            if ret < 0 {
                avformat_close_input(&mut in_fmt_ctx);
                return Err(OpenInputError::from(ret).into());
            }

            if let Err(e) = find_stream_info_if_enabled(in_fmt_ctx, input, index) {
                avformat_close_input(&mut in_fmt_ctx);
                return Err(e);
            }
        }
    }

    // Open succeeded. Take ownership of the raw context in a `FormatContext` now:
    // it owns the pointer (and, for custom IO, its AVIO + callback box) through the
    // fallible tail below (option cleanup, seek, `Demuxer::new`). Any early return
    // here drops `fc`, freeing exactly once — this replaces the old re-armed guard.
    // The custom-IO variant (no url == read_callback path) frees the AVIO + box.
    // SAFETY: in_fmt_ctx is a successfully-opened input context; ownership moves in.
    let fc = if input.url.is_none() {
        crate::raw::FormatContext::from_input_custom_io(in_fmt_ctx)
    } else {
        crate::raw::FormatContext::from_input(in_fmt_ctx)
    };

    // A zero-stream input cannot feed anything downstream. This mostly bites
    // with probing disabled (`set_find_stream_info(false)`) on formats that
    // reveal streams only during `avformat_find_stream_info` — reject it here
    // instead of failing obscurely later. Dropping `fc` frees the context
    // (custom-IO variant included) exactly once.
    // SAFETY: fc owns the successfully opened input context.
    if (*fc.as_ptr()).nb_streams == 0 {
        error!(target: LOG_TARGET, "No streams found in input {index}");
        return Err(FindStreamError::NoStreamFound.into());
    }

    // Options no demuxer consumed are user typos; report them instead of
    // silently swallowing (fftools check_avoptions aborts here — we warn).
    // The auto-injected scan_all_pmts must not be blamed on the user.
    if injected_scan_all_pmts {
        input_opts.remove(&CString::new("scan_all_pmts")?);
    }
    for key in input_opts.leftover_keys() {
        warn!(target: LOG_TARGET, "Option '{key}' was not recognized by input {index}");
    }

    let mut timestamp = input.start_time_us.unwrap_or(0);
    /* add the stream start time */
    // SAFETY: fc owns the opened input context; as_ptr yields the live pointer.
    let in_fmt_ctx = fc.as_ptr();
    if (*in_fmt_ctx).start_time != ffmpeg_sys_next::AV_NOPTS_VALUE {
        timestamp += (*in_fmt_ctx).start_time;
    }

    /* if seeking requested, we execute it */
    if let Some(start_time_us) = input.start_time_us {
        let mut seek_timestamp = timestamp;

        if (*(*in_fmt_ctx).iformat).flags & ffmpeg_sys_next::AVFMT_SEEK_TO_PTS == 0 {
            let mut dts_heuristic = false;
            let stream_count = (*in_fmt_ctx).nb_streams;

            for i in 0..stream_count {
                let stream = *(*in_fmt_ctx).streams.add(i as usize);
                let par = (*stream).codecpar;
                if (*par).video_delay != 0 {
                    dts_heuristic = true;
                    break;
                }
            }
            if dts_heuristic {
                seek_timestamp -= 3 * AV_TIME_BASE as i64 / 23;
            }
        }
        let ret = ffmpeg_sys_next::avformat_seek_file(
            in_fmt_ctx,
            -1,
            i64::MIN,
            seek_timestamp,
            seek_timestamp,
            0,
        );
        if ret < 0 {
            warn!(target: LOG_TARGET,
                "could not seek to position {:.3}",
                start_time_us as f64 / AV_TIME_BASE as f64
            );
        }
    }

    let url = input
        .url
        .clone()
        .unwrap_or_else(|| format!("read_callback[{index}]"));

    // Per-media decoder options (Input::set_*_codec_opt). Converted here —
    // like the output-side encoder opts — and applied later at decoder open
    // (dec_task); an invalid C string drops `fc`, freeing the context once.
    let video_codec_opts = convert_options(input.video_codec_opts.clone())?;
    let audio_codec_opts = convert_options(input.audio_codec_opts.clone())?;
    let subtitle_codec_opts = convert_options(input.subtitle_codec_opts.clone())?;

    let demux = Demuxer::new(
        url,
        fc,
        0 - if copy_ts { 0 } else { timestamp },
        input.frame_pipelines.take(),
        input.video_codec.clone(),
        input.audio_codec.clone(),
        input.subtitle_codec.clone(),
        video_codec_opts,
        audio_codec_opts,
        subtitle_codec_opts,
        input.readrate,
        input.start_time_us,
        recording_time_us,
        input.exit_on_error,
        input.stream_loop,
        input.hwaccel.clone(),
        input.hwaccel_device.clone(),
        input.hwaccel_output_format.clone(),
        copy_ts,
        input.autorotate.unwrap_or(true), // Default to true (enabled)
        input.ts_scale.unwrap_or(1.0),    // Default to 1.0 (no scaling)
        match input.framerate {
            // Default to {0, 0} (use packet duration)
            Some((num, den)) => AVRational { num, den },
            None => AVRational { num: 0, den: 0 },
        },
        input.log_level_offset.unwrap_or(0),
    )?;

    // `Demuxer` now owns the context (via its `Option<FormatContext>`); a failure
    // above dropped `fc` exactly once. No guard to disarm.
    Ok(demux)
}

/// RAII owner of the per-stream `AVDictionary` options array handed to
/// `avformat_find_stream_info` (one dict per stream, null when a stream has
/// no configured opts — the layout the C API expects). The probing call
/// rewrites each slot to hold the entries no probing codec recognized; Drop
/// frees every slot on all paths, mirroring fftools' setup/cleanup of
/// find-stream-info options (cmdutils.c `setup_find_stream_info_opts`).
#[cfg(not(docsrs))]
struct FindStreamInfoOptions {
    dicts: Vec<*mut ffmpeg_sys_next::AVDictionary>,
}

#[cfg(not(docsrs))]
impl FindStreamInfoOptions {
    /// Builds the array for `nb_streams` streams from the user's per-stream
    /// probing opts (`Input::set_find_stream_info_codec_opt`). A configured
    /// index `>= nb_streams` is a user error and fails with
    /// `FindStreamError::InvalidArgument`; Drop frees any dicts already built
    /// on that path.
    fn new(
        nb_streams: usize,
        configured: Option<&HashMap<usize, HashMap<String, String>>>,
    ) -> Result<Self> {
        let mut this = Self {
            dicts: vec![null_mut(); nb_streams],
        };
        if let Some(configured) = configured {
            for (&stream_index, opts) in configured {
                if stream_index >= nb_streams {
                    error!(target: LOG_TARGET,
                        "find_stream_info codec opts reference stream {stream_index}, \
                         but the input only has {nb_streams} stream(s)"
                    );
                    return Err(FindStreamError::InvalidArgument.into());
                }
                for (key, value) in opts {
                    let key = CString::new(key.as_str())?;
                    let value = CString::new(value.as_str())?;
                    // SAFETY: the slot is a valid (possibly null) dict pointer
                    // owned by `this`; av_dict_set allocates or extends it in
                    // place.
                    unsafe {
                        ffmpeg_sys_next::av_dict_set(
                            &mut this.dicts[stream_index],
                            key.as_ptr(),
                            value.as_ptr(),
                            0,
                        );
                    }
                }
            }
        }
        Ok(this)
    }

    /// Pointer to the dict array for `avformat_find_stream_info`, or null when
    /// the input exposes no streams yet (the array must then not be touched).
    fn as_mut_ptr_or_null(&mut self) -> *mut *mut ffmpeg_sys_next::AVDictionary {
        if self.dicts.is_empty() {
            null_mut()
        } else {
            self.dicts.as_mut_ptr()
        }
    }

    /// Leftover `(stream_index, option key)` pairs after probing — the entries
    /// no probing codec recognized (mirrors `DictGuard::leftover_keys`).
    fn leftover_keys(&self) -> Vec<(usize, String)> {
        let mut keys = Vec::new();
        for (stream_index, dict) in self.dicts.iter().enumerate() {
            let mut entry = null();
            // SAFETY: each slot is null or a live dict owned by self;
            // av_dict_iterate tolerates null and yields entries owned by the
            // dict.
            unsafe {
                loop {
                    entry = ffmpeg_sys_next::av_dict_iterate(*dict, entry);
                    if entry.is_null() {
                        break;
                    }
                    keys.push((
                        stream_index,
                        CStr::from_ptr((*entry).key).to_string_lossy().into_owned(),
                    ));
                }
            }
        }
        keys
    }
}

#[cfg(not(docsrs))]
impl Drop for FindStreamInfoOptions {
    fn drop(&mut self) {
        for dict in &mut self.dicts {
            // SAFETY: each slot is null or a live dict owned by self;
            // av_dict_free tolerates null and nulls the slot.
            unsafe { ffmpeg_sys_next::av_dict_free(dict) };
        }
    }
}

/// Runs `avformat_find_stream_info` on a freshly opened input unless the user
/// disabled probing (`Input::set_find_stream_info(false)`), applying any
/// per-stream probing codec opts. The caller owns cleanup of `in_fmt_ctx` on
/// error (branch-specific: the custom-IO branch also frees its AVIO callback
/// state).
#[cfg(not(docsrs))]
unsafe fn find_stream_info_if_enabled(
    in_fmt_ctx: *mut AVFormatContext,
    input: &Input,
    index: usize,
) -> Result<()> {
    if !input.find_stream_info {
        if input.find_stream_info_codec_opts.is_some() {
            warn!(target: LOG_TARGET,
                "find_stream_info is disabled for input {index}; \
                 ignoring its find_stream_info codec opts"
            );
        }
        return Ok(());
    }
    let mut probe_opts = FindStreamInfoOptions::new(
        (*in_fmt_ctx).nb_streams as usize,
        input.find_stream_info_codec_opts.as_ref(),
    )?;
    let ret = avformat_find_stream_info(in_fmt_ctx, probe_opts.as_mut_ptr_or_null());
    if ret < 0 {
        return Err(FindStreamError::from(ret).into());
    }
    // Probing consumed the options it recognized; whatever is left is a user
    // typo (fftools check_avoptions errors out — we warn, matching the
    // input_opts leftover handling in open_input_file). Exception:
    // avformat_find_stream_info injects "threads"/"lowres"/"codec_whitelist" into
    // every per-stream opts dict (FFmpeg demux.c ~2679-2686); a stream it never
    // opens a decoder for leaves them unconsumed. They are FFmpeg's, not the
    // user's, so reporting them as unrecognized is a false positive.
    for (stream_index, key) in probe_opts.leftover_keys() {
        if matches!(key.as_str(), "threads" | "lowres" | "codec_whitelist") {
            continue;
        }
        warn!(target: LOG_TARGET,
            "Option '{key}' was not recognized while probing stream {stream_index} of input {index}"
        );
    }
    Ok(())
}

#[cfg(all(test, not(docsrs)))]
mod find_stream_info_tests {
    use super::FindStreamInfoOptions;
    use crate::error::{Error, FindStreamError};
    use std::collections::HashMap;
    use std::ffi::{CStr, CString};

    fn probing_opts(
        entries: &[(usize, &[(&str, &str)])],
    ) -> HashMap<usize, HashMap<String, String>> {
        entries
            .iter()
            .map(|(index, pairs)| {
                (
                    *index,
                    pairs
                        .iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                )
            })
            .collect()
    }

    fn dict_value(dict: *mut ffmpeg_sys_next::AVDictionary, key: &str) -> Option<String> {
        let key = CString::new(key).unwrap();
        // SAFETY: dict is null or a live dict owned by the options array under
        // test; av_dict_get tolerates null.
        unsafe {
            let entry = ffmpeg_sys_next::av_dict_get(
                dict,
                key.as_ptr(),
                std::ptr::null(),
                ffmpeg_sys_next::AV_DICT_MATCH_CASE,
            );
            if entry.is_null() {
                None
            } else {
                Some(
                    CStr::from_ptr((*entry).value)
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        }
    }

    #[test]
    fn no_configured_opts_yields_all_null_dicts() {
        let mut opts = FindStreamInfoOptions::new(3, None).unwrap();
        assert_eq!(opts.dicts.len(), 3);
        assert!(opts.dicts.iter().all(|dict| dict.is_null()));
        assert!(!opts.as_mut_ptr_or_null().is_null());
        assert!(opts.leftover_keys().is_empty());
    }

    #[test]
    fn zero_streams_passes_null_array() {
        let mut opts = FindStreamInfoOptions::new(0, None).unwrap();
        assert!(opts.as_mut_ptr_or_null().is_null());
    }

    #[test]
    fn sparse_indices_leave_null_dicts_for_missing_streams() {
        let configured = probing_opts(&[
            (0, &[("skip_frame", "nokey")]),
            (2, &[("lowres", "1"), ("skip_frame", "all")]),
        ]);
        let opts = FindStreamInfoOptions::new(3, Some(&configured)).unwrap();
        assert!(!opts.dicts[0].is_null());
        assert!(
            opts.dicts[1].is_null(),
            "unconfigured stream must stay null"
        );
        assert!(!opts.dicts[2].is_null());
        assert_eq!(
            dict_value(opts.dicts[0], "skip_frame").as_deref(),
            Some("nokey")
        );
        assert_eq!(dict_value(opts.dicts[2], "lowres").as_deref(), Some("1"));
        assert_eq!(
            dict_value(opts.dicts[2], "skip_frame").as_deref(),
            Some("all")
        );
        // Before probing runs, every configured entry is still "left over".
        let mut leftovers = opts.leftover_keys();
        leftovers.sort();
        assert_eq!(
            leftovers,
            vec![
                (0, "skip_frame".to_string()),
                (2, "lowres".to_string()),
                (2, "skip_frame".to_string()),
            ]
        );
        // Drop must free dict 0 and dict 2 and tolerate the null slot.
        drop(opts);
    }

    #[test]
    fn out_of_range_stream_index_is_invalid_argument() {
        let configured = probing_opts(&[(0, &[("skip_frame", "nokey")]), (5, &[("lowres", "1")])]);
        // Asserts the out-of-range index is rejected. The error path still
        // drops the partially built array (HashMap order decides whether
        // stream 0's dict was built before index 5 tripped the error, so this
        // does not assert ordering); the deterministic drop-frees-real-dicts
        // coverage lives in `sparse_indices_leave_null_dicts_for_missing_streams`.
        let err = match FindStreamInfoOptions::new(2, Some(&configured)) {
            Ok(_) => panic!("expected InvalidArgument for out-of-range stream index"),
            Err(err) => err,
        };
        assert!(
            matches!(err, Error::FindStream(FindStreamError::InvalidArgument)),
            "expected InvalidArgument, got {err:?}"
        );
    }

    #[test]
    fn drop_after_partial_build_is_leak_free() {
        // Deterministic partial-build-then-drop: a valid stream 0 dict IS
        // built (asserted non-null) before this array drops, so ASAN/valgrind
        // in the sanitizer job observes the error-path Drop freeing a real
        // dict rather than only null slots.
        let configured = probing_opts(&[(0, &[("skip_frame", "nokey")])]);
        let opts = FindStreamInfoOptions::new(1, Some(&configured)).unwrap();
        assert!(!opts.dicts[0].is_null());
        drop(opts);
    }
}

/// True when the opened input is the rawvideo demuxer — a purely sequential
/// elementary-stream format that never seeks on its own. Used to skip the
/// destructive generic seek probe for callback-backed rawvideo inputs.
unsafe fn format_is_rawvideo(fmt_ctx: *mut AVFormatContext) -> bool {
    if fmt_ctx.is_null() || (*fmt_ctx).iformat.is_null() {
        return false;
    }
    let name = CStr::from_ptr((*(*fmt_ctx).iformat).name).to_string_lossy();
    name.split(',').any(|f| f == "rawvideo")
}

unsafe fn input_requires_seek(fmt_ctx: *mut AVFormatContext) -> bool {
    if fmt_ctx.is_null() {
        return false;
    }

    let mut format_name = "unknown".to_string();
    let mut format_names: Vec<&str> = Vec::with_capacity(0);

    if !(*fmt_ctx).iformat.is_null() {
        let iformat = (*fmt_ctx).iformat;
        format_name = CStr::from_ptr((*iformat).name)
            .to_string_lossy()
            .into_owned();
        let flags = (*iformat).flags;
        let no_binsearch = flags & AVFMT_NOBINSEARCH != 0;
        let no_gensearch = flags & AVFMT_NOGENSEARCH != 0;

        log::debug!(target: LOG_TARGET,
            "Input format '{format_name}' - Binary search: {}, Generic search: {}",
            if no_binsearch { "Disabled" } else { "Enabled" },
            if no_gensearch { "Disabled" } else { "Enabled" }
        );

        format_names = format_name.split(',').collect();

        if format_names.iter().any(|&f| {
            matches!(
                f,
                "mp4" | "mkv" | "avi" | "mov" | "flac" | "wav" | "aac" | "ogg" | "mp3" | "webm"
            )
        }) && !no_binsearch
            && !no_gensearch
        {
            return true;
        }

        if format_names.iter().any(|&f| {
            matches!(
                f,
                "hls" | "m3u8" | "mpegts" | "mms" | "udp" | "rtp" | "rtp_mpegts" | "http" | "srt"
            )
        }) {
            log::debug!(target: LOG_TARGET, "Live stream detected ({format_name}). Seeking is not possible.");
            return false;
        }

        if no_binsearch && no_gensearch {
            log::debug!(target: LOG_TARGET, "Input format '{format_name}' has both NOBINSEARCH and NOGENSEARCH set. Seeking is likely restricted.");
        }
    }

    let format_duration = (*fmt_ctx).duration;

    if format_names.contains(&"flv") {
        if format_duration <= 0 {
            log::debug!(target: LOG_TARGET,
                "Input format 'flv' detected with no valid duration. Seeking is not possible."
            );
        } else {
            log::warn!(target: LOG_TARGET, "Input format 'flv' detected with a valid duration. While seeking may still be possible, it is highly recommended to add a `seek_callback()` for optimal input handling, especially when seeking or random access to specific segments is required.");
        }
        return false;
    }

    if format_duration > 0 {
        log::debug!(target: LOG_TARGET, "Format '{format_name}' has a duration of {format_duration}. Seeking is likely possible.");
        return true;
    }

    let mut video_stream_index = -1;
    for i in 0..(*fmt_ctx).nb_streams {
        let stream = *(*fmt_ctx).streams.offset(i as isize);
        if (*stream).codecpar.is_null() {
            continue;
        }
        if (*(*stream).codecpar).codec_type == AVMEDIA_TYPE_VIDEO {
            video_stream_index = i as i32;
            break;
        }
    }

    let stream_index = if video_stream_index >= 0 {
        video_stream_index
    } else {
        -1
    };

    let original_pos = if !(*fmt_ctx).pb.is_null() {
        (*(*fmt_ctx).pb).pos
    } else {
        -1
    };

    if original_pos >= 0 {
        let seek_target = AV_TIME_BASE as i64;
        let seek_result = av_seek_frame(fmt_ctx, stream_index, seek_target, AVSEEK_FLAG_BACKWARD);

        if seek_result >= 0 {
            log::debug!(target: LOG_TARGET, "Seek test successful.");

            (*(*fmt_ctx).pb).pos = original_pos;
            avformat_flush(fmt_ctx);
            log::debug!(target: LOG_TARGET, "Restored fmt_ctx.pb.pos to {original_pos} and flushed format context.",);
            return true;
        } else {
            log::debug!(target: LOG_TARGET, "Seek test failed (return code {seek_result}). This format likely does not support seeking.");
        }
    }

    false
}
