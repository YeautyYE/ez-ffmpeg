//! Loads file/SRT subtitle sources through lavformat/lavcodec into the
//! pure-Rust ASS engine.
//!
//! Faithful port of FFmpeg's `vf_subtitles` `init_subtitles()` (verified
//! against tag n7.1.3): open with avformat, pick the subtitle stream, collect
//! container font attachments, decode text subtitles with avcodec (which
//! normalizes SRT/VTT markup into ASS dialogue lines), and feed the script
//! parser via its `process_codec_private` + `process_chunk` equivalents.

use super::ass::{Script, ScriptParser};
use super::options::{path_cstring, SubtitleError};
use crate::util::ffmpeg_utils::{av_err2str, string_to_cstring};
use ffmpeg_sys_next::{
    av_dict_free, av_dict_get, av_dict_set, av_find_best_stream, av_free, av_freep, av_malloc,
    av_packet_alloc, av_packet_free, av_packet_unref, av_read_frame, av_rescale_q,
    avcodec_alloc_context3, avcodec_decode_subtitle2, avcodec_descriptor_get, avcodec_find_decoder,
    avcodec_free_context, avcodec_get_name, avcodec_open2, avcodec_parameters_to_context,
    avformat_alloc_context, avformat_close_input, avformat_find_stream_info, avformat_open_input,
    avio_alloc_context, avio_context_free, avsubtitle_free, AVCodecContext, AVCodecID,
    AVDictionary, AVFormatContext, AVIOContext, AVMediaType, AVPacket, AVRational, AVStream,
    AVSubtitle, AVERROR_EOF, AVFMT_FLAG_CUSTOM_IO, AVSEEK_FORCE, AVSEEK_SIZE,
    AV_CODEC_PROP_TEXT_SUB, AV_DICT_MATCH_CASE, AV_TIME_BASE_Q,
};
use std::ffi::CStr;
use std::os::raw::{c_int, c_void};
use std::path::Path;
use std::ptr::{addr_of_mut, null, null_mut};

/// Font attachment mimetypes accepted by FFmpeg's subtitles filter (n7.1.3).
const FONT_MIMETYPES: [&str; 10] = [
    "font/ttf",
    "font/otf",
    "font/sfnt",
    "font/woff",
    "font/woff2",
    "application/font-sfnt",
    "application/font-woff",
    "application/x-truetype-font",
    "application/vnd.ms-opentype",
    "application/x-font-ttf",
];

// c"..." literals need Rust 1.77; the crate supports older toolchains.
// SAFETY (both): the literals contain exactly one NUL, at the end.
#[allow(clippy::manual_c_str_literals)]
const MIMETYPE_KEY: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"mimetype\0") };
#[allow(clippy::manual_c_str_literals)]
const FILENAME_KEY: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"filename\0") };

pub(crate) enum LoaderInput<'a> {
    Path(&'a Path),
    Memory(&'a [u8]),
}

pub(crate) struct LoaderOptions<'a> {
    pub(crate) charenc: Option<&'a str>,
    /// Index among the source's *subtitle* streams (0 = first subtitle
    /// stream), mirroring the `si`/`stream_index` option of FFmpeg's
    /// subtitles filter.
    pub(crate) stream_index: Option<usize>,
}

/// Everything the builder needs from a decoded subtitle source.
pub(crate) struct LoadedSubtitles {
    pub(crate) script: Script,
    /// Container font attachments (filename, bytes), FFmpeg MIME rules.
    pub(crate) attachments: Vec<(String, Vec<u8>)>,
    /// True when the stream was native ASS (FFmpeg's wrap_unicode auto rule
    /// enables Unicode wrapping for everything else).
    pub(crate) native_ass: bool,
}

pub(crate) fn load_subtitles(
    input: LoaderInput<'_>,
    options: &LoaderOptions<'_>,
) -> Result<LoadedSubtitles, SubtitleError> {
    // SAFETY: load_impl only dereferences pointers produced (and null-checked)
    // by the FFmpeg calls right before each use, and every allocated resource
    // is owned by a RAII guard so early returns unwind cleanly.
    unsafe { load_impl(input, options) }
}

unsafe fn load_impl(
    input: LoaderInput<'_>,
    options: &LoaderOptions<'_>,
) -> Result<LoadedSubtitles, SubtitleError> {
    let source = open_input(&input)?;
    let fmt = source.fmt;

    let ret = avformat_find_stream_info(fmt, null_mut());
    if ret < 0 {
        return Err(SubtitleError::Open(format!(
            "cannot read subtitle stream info: {}",
            av_err2str(ret)
        )));
    }

    let nb_streams = (*fmt).nb_streams as usize;

    // Stream selection: explicit index counts subtitle streams only
    // (0 = first subtitle stream), otherwise av_find_best_stream.
    let sid = match options.stream_index {
        None => {
            let ret = av_find_best_stream(
                fmt,
                AVMediaType::AVMEDIA_TYPE_SUBTITLE,
                -1,
                -1,
                null_mut(),
                0,
            );
            if ret < 0 {
                return Err(SubtitleError::NoSubtitleStream(
                    "no subtitle stream found in source".to_string(),
                ));
            }
            ret as usize
        }
        Some(wanted) => {
            let mut nth = 0usize;
            let mut found = None;
            for j in 0..nb_streams {
                let stream = *(*fmt).streams.add(j);
                if (*(*stream).codecpar).codec_type == AVMediaType::AVMEDIA_TYPE_SUBTITLE {
                    if nth == wanted {
                        found = Some(j);
                        break;
                    }
                    nth += 1;
                }
            }
            found.ok_or_else(|| {
                SubtitleError::NoSubtitleStream(format!(
                    "subtitle stream index {wanted} not found in source"
                ))
            })?
        }
    };

    // Container font attachments (FFmpeg parity: fonts embedded in e.g. MKV).
    let mut attachments: Vec<(String, Vec<u8>)> = Vec::new();
    for j in 0..nb_streams {
        let stream = *(*fmt).streams.add(j);
        let par = (*stream).codecpar;
        if (*par).codec_type != AVMediaType::AVMEDIA_TYPE_ATTACHMENT
            || (*par).extradata.is_null()
            || (*par).extradata_size <= 0
            || !attachment_is_font(stream)
        {
            continue;
        }
        match dict_get_cstr((*stream).metadata, FILENAME_KEY) {
            Some(name) => {
                log::debug!("loading attached font: {}", name.to_string_lossy());
                let data =
                    std::slice::from_raw_parts((*par).extradata, (*par).extradata_size as usize);
                attachments.push((name.to_string_lossy().into_owned(), data.to_vec()));
            }
            None => log::warn!("font attachment has no filename, ignored"),
        }
    }

    let stream = *(*fmt).streams.add(sid);
    let par = (*stream).codecpar;
    let codec_id = (*par).codec_id;

    let decoder = avcodec_find_decoder(codec_id);
    if decoder.is_null() {
        return Err(SubtitleError::Decode(format!(
            "no decoder available for subtitle codec {}",
            codec_name(codec_id)
        )));
    }
    let descriptor = avcodec_descriptor_get(codec_id);
    if !descriptor.is_null() && ((*descriptor).props & AV_CODEC_PROP_TEXT_SUB) == 0 {
        return Err(SubtitleError::BitmapSubtitles(codec_name(codec_id)));
    }

    let dec_guard = DecoderGuard(avcodec_alloc_context3(decoder));
    if dec_guard.0.is_null() {
        return Err(SubtitleError::Decode(
            "cannot allocate subtitle decoder context".to_string(),
        ));
    }
    let dec_ctx = dec_guard.0;

    let ret = avcodec_parameters_to_context(dec_ctx, par);
    if ret < 0 {
        return Err(SubtitleError::Decode(format!(
            "cannot copy subtitle codec parameters: {}",
            av_err2str(ret)
        )));
    }
    // Required so lavc can rescale decoded AVSubtitle pts into AV_TIME_BASE.
    (*dec_ctx).pkt_timebase = (*stream).time_base;

    let mut dict_guard = DictGuard(null_mut());
    if let Some(charenc) = options.charenc {
        let key = string_to_cstring("sub_charenc").expect("static key");
        let value = string_to_cstring(charenc).map_err(SubtitleError::InvalidOption)?;
        av_dict_set(&mut dict_guard.0, key.as_ptr(), value.as_ptr(), 0);
    }
    let ret = avcodec_open2(dec_ctx, null(), &mut dict_guard.0);
    if ret < 0 {
        return Err(SubtitleError::Decode(format!(
            "cannot open subtitle decoder: {}",
            av_err2str(ret)
        )));
    }

    let mut parser = ScriptParser::new();
    if !(*dec_ctx).subtitle_header.is_null() && (*dec_ctx).subtitle_header_size > 0 {
        let header = std::slice::from_raw_parts(
            (*dec_ctx).subtitle_header,
            (*dec_ctx).subtitle_header_size as usize,
        );
        // lavcodec subtitle headers are ASS text and UTF-8 by contract.
        parser.process_codec_private(&String::from_utf8_lossy(header));
    } else {
        // No header: the fallback event format still applies (libass
        // ass_process_codec_private behavior on empty input).
        parser.process_codec_private("");
    }

    let pkt_guard = PacketGuard(av_packet_alloc());
    if pkt_guard.0.is_null() {
        return Err(SubtitleError::Decode("cannot allocate packet".to_string()));
    }
    let pkt = pkt_guard.0;

    let mut fed_events = 0usize;
    while av_read_frame(fmt, pkt) >= 0 {
        if (*pkt).stream_index as usize == sid {
            let mut sub: AVSubtitle = std::mem::zeroed();
            let mut got: c_int = 0;
            let ret = avcodec_decode_subtitle2(dec_ctx, &mut sub, &mut got, pkt);
            let _sub_guard = SubtitleGuard(&mut sub);
            if ret < 0 {
                // Lenient like FFmpeg: a bad cue is skipped, not fatal.
                log::warn!("subtitle decode error (event skipped): {}", av_err2str(ret));
            } else if got != 0 {
                // start/end_display_time are millisecond offsets from sub.pts
                // (FFmpeg vf_subtitles semantics): the cue starts at
                // pts + start_display_time and lasts end - start.
                let pts_ms =
                    av_rescale_q(sub.pts, AV_TIME_BASE_Q, AVRational { num: 1, den: 1000 });
                let start_ms = pts_ms + i64::from(sub.start_display_time);
                let duration_ms =
                    (i64::from(sub.end_display_time) - i64::from(sub.start_display_time)).max(0);
                for i in 0..sub.num_rects as usize {
                    let rect = *sub.rects.add(i);
                    let line = (*rect).ass;
                    if line.is_null() {
                        break;
                    }
                    let line = CStr::from_ptr(line).to_string_lossy();
                    parser.process_chunk(&line, start_ms, duration_ms);
                    fed_events += 1;
                }
            }
        }
        av_packet_unref(pkt);
    }

    if fed_events == 0 {
        log::warn!("subtitle source contained no dialogue events; nothing will be rendered");
    }
    Ok(LoadedSubtitles {
        script: parser.into_script(),
        attachments,
        native_ass: codec_id == AVCodecID::AV_CODEC_ID_ASS,
    })
}

/// Owns whatever `open_input` allocated; drop order matters (format context
/// first — with `AVFMT_FLAG_CUSTOM_IO` it does not free our AVIO context).
struct OpenInput {
    fmt: *mut AVFormatContext,
    avio: *mut AVIOContext,
    cursor: *mut MemCursor,
}

impl Drop for OpenInput {
    fn drop(&mut self) {
        // SAFETY: each handle is freed at most once and only when set; the io
        // buffer may have been reallocated by lavformat, so free whatever the
        // context currently owns before freeing the context itself.
        unsafe {
            if !self.fmt.is_null() {
                avformat_close_input(&mut self.fmt);
            }
            if !self.avio.is_null() {
                av_freep(addr_of_mut!((*self.avio).buffer) as *mut c_void);
                avio_context_free(&mut self.avio);
            }
            if !self.cursor.is_null() {
                drop(Box::from_raw(self.cursor));
            }
        }
    }
}

unsafe fn open_input(input: &LoaderInput<'_>) -> Result<OpenInput, SubtitleError> {
    match input {
        LoaderInput::Path(path) => {
            let path_c = path_cstring(path).map_err(SubtitleError::Open)?;
            let mut fmt = null_mut();
            let ret = avformat_open_input(&mut fmt, path_c.as_ptr(), null(), null_mut());
            if ret < 0 {
                return Err(SubtitleError::Open(format!(
                    "cannot open {}: {}",
                    path.display(),
                    av_err2str(ret)
                )));
            }
            Ok(OpenInput {
                fmt,
                avio: null_mut(),
                cursor: null_mut(),
            })
        }
        LoaderInput::Memory(data) => {
            const IO_BUFFER_SIZE: usize = 4096;
            let mut guard = OpenInput {
                fmt: null_mut(),
                avio: null_mut(),
                cursor: null_mut(),
            };
            guard.cursor = Box::into_raw(Box::new(MemCursor {
                data: data.as_ptr(),
                len: data.len(),
                pos: 0,
            }));

            let io_buffer = av_malloc(IO_BUFFER_SIZE);
            if io_buffer.is_null() {
                return Err(SubtitleError::Open(
                    "out of memory allocating IO buffer".to_string(),
                ));
            }
            guard.avio = avio_alloc_context(
                io_buffer as *mut u8,
                IO_BUFFER_SIZE as c_int,
                0,
                guard.cursor as *mut c_void,
                Some(mem_read),
                None,
                Some(mem_seek),
            );
            if guard.avio.is_null() {
                av_free(io_buffer);
                return Err(SubtitleError::Open(
                    "cannot allocate in-memory IO context".to_string(),
                ));
            }

            let mut fmt = avformat_alloc_context();
            if fmt.is_null() {
                return Err(SubtitleError::Open(
                    "cannot allocate format context".to_string(),
                ));
            }
            (*fmt).pb = guard.avio;
            (*fmt).flags |= AVFMT_FLAG_CUSTOM_IO;
            // On failure avformat_open_input frees `fmt`; the guard still owns
            // the AVIO context and cursor.
            let ret = avformat_open_input(&mut fmt, null(), null(), null_mut());
            if ret < 0 {
                return Err(SubtitleError::Open(format!(
                    "cannot probe in-memory subtitles: {}",
                    av_err2str(ret)
                )));
            }
            guard.fmt = fmt;
            Ok(guard)
        }
    }
}

/// Read-only cursor the custom AVIO callbacks walk over the caller's buffer.
/// The pointed-to data is owned by the builder and outlives the loader call.
struct MemCursor {
    data: *const u8,
    len: usize,
    pos: usize,
}

unsafe extern "C" fn mem_read(opaque: *mut c_void, buf: *mut u8, buf_size: c_int) -> c_int {
    // SAFETY: `opaque` is the Box<MemCursor> installed by open_input and lives
    // until the OpenInput guard drops (after the format context closes).
    let cursor = unsafe { &mut *(opaque as *mut MemCursor) };
    if buf_size <= 0 {
        return 0;
    }
    if cursor.pos >= cursor.len {
        return AVERROR_EOF;
    }
    let n = (cursor.len - cursor.pos).min(buf_size as usize);
    // SAFETY: lavformat guarantees `buf` holds `buf_size` bytes; the source
    // range is bounds-checked above.
    unsafe { std::ptr::copy_nonoverlapping(cursor.data.add(cursor.pos), buf, n) };
    cursor.pos += n;
    n as c_int
}

unsafe extern "C" fn mem_seek(opaque: *mut c_void, offset: i64, whence: c_int) -> i64 {
    // SAFETY: see mem_read.
    let cursor = unsafe { &mut *(opaque as *mut MemCursor) };
    if whence & AVSEEK_SIZE != 0 {
        return cursor.len as i64;
    }
    let base = match whence & !AVSEEK_FORCE {
        libc::SEEK_SET => 0,
        libc::SEEK_CUR => cursor.pos as i64,
        libc::SEEK_END => cursor.len as i64,
        _ => return -(libc::EINVAL as i64),
    };
    match base.checked_add(offset) {
        Some(target) if target >= 0 && target as usize <= cursor.len => {
            cursor.pos = target as usize;
            target
        }
        _ => -(libc::EINVAL as i64),
    }
}

struct DecoderGuard(*mut AVCodecContext);
impl Drop for DecoderGuard {
    fn drop(&mut self) {
        // SAFETY: owned handle, freed once (null-safe).
        unsafe { avcodec_free_context(&mut self.0) };
    }
}

struct DictGuard(*mut AVDictionary);
impl Drop for DictGuard {
    fn drop(&mut self) {
        // SAFETY: owned dictionary, freed once (null-safe).
        unsafe { av_dict_free(&mut self.0) };
    }
}

struct PacketGuard(*mut AVPacket);
impl Drop for PacketGuard {
    fn drop(&mut self) {
        // SAFETY: owned packet, freed once (null-safe).
        unsafe { av_packet_free(&mut self.0) };
    }
}

struct SubtitleGuard(*mut AVSubtitle);
impl Drop for SubtitleGuard {
    fn drop(&mut self) {
        // SAFETY: points to a live stack AVSubtitle; avsubtitle_free resets it
        // and is safe on a zeroed struct.
        unsafe { avsubtitle_free(self.0) };
    }
}

unsafe fn attachment_is_font(stream: *mut AVStream) -> bool {
    match dict_get_cstr((*stream).metadata, MIMETYPE_KEY) {
        Some(mime) => {
            let mime = mime.to_string_lossy();
            FONT_MIMETYPES.iter().any(|m| m.eq_ignore_ascii_case(&mime))
        }
        None => false,
    }
}

unsafe fn dict_get_cstr<'a>(dict: *mut AVDictionary, key: &CStr) -> Option<&'a CStr> {
    let entry = av_dict_get(dict, key.as_ptr(), null(), AV_DICT_MATCH_CASE);
    if entry.is_null() || (*entry).value.is_null() {
        None
    } else {
        Some(CStr::from_ptr((*entry).value))
    }
}

unsafe fn codec_name(id: AVCodecID) -> String {
    let name = avcodec_get_name(id);
    if name.is_null() {
        format!("{id:?}")
    } else {
        CStr::from_ptr(name).to_string_lossy().into_owned()
    }
}
