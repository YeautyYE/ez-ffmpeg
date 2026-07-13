//! Container attachment streams (FFmpeg `-attach`).
//!
//! An attachment (a font, a cover image, …) is **not** a media packet. Like
//! FFmpeg's `of_add_attachments` (`fftools/ffmpeg_mux_init.c`), we read the whole
//! file into an FFmpeg-owned `extradata` buffer on a new
//! `AVMEDIA_TYPE_ATTACHMENT` stream and tag it with `filename`/`mimetype`; the
//! muxer writes that payload into the container header at
//! `avformat_write_header`. No packet is ever emitted for an attachment.
//!
//! This mirrors — in reverse — how [`crate::subtitle`] *reads* container font
//! attachments (`src/subtitle/loader.rs`): both treat an attachment stream as an
//! `extradata`/`extradata_size` blob with `filename`/`mimetype` metadata.
//!
//! Attachments are reliably supported only by **Matroska/WebM**. Other muxers
//! (MP4, MPEG-TS, …) reject the extra stream at header-write time; we do not
//! pre-validate the muxer (matching FFmpeg) but log a hint.

use crate::core::context::muxer::Muxer;
use crate::error::{OpenOutputError, Result};
use ffmpeg_sys_next::AVCodecID::{AV_CODEC_ID_NONE, AV_CODEC_ID_OTF, AV_CODEC_ID_TTF};
use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_ATTACHMENT;
use ffmpeg_sys_next::{
    av_dict_set, av_mallocz, avformat_new_stream, AVCodecID, AVStream, AV_INPUT_BUFFER_PADDING_SIZE,
};
use std::ffi::{CStr, CString};
use std::fs::File;
use std::io::Read;
use std::path::Path;

/// Largest attachment payload we will read into memory. Each attachment becomes
/// a single heap buffer handed to FFmpeg; we cap the read at this size with a
/// bounded `Read::take` and refuse anything larger, so a hostile or accidental
/// huge — or unbounded, e.g. `/dev/zero` — path cannot OOM the process. 100 MiB
/// comfortably covers fonts and cover art while staying far below `i32::MAX`
/// (the `extradata_size` field is a C `int`).
pub(crate) const MAX_ATTACHMENT_SIZE: u64 = 100 * 1024 * 1024;

// The metadata keys FFmpeg (and this crate's subtitle loader) use for
// attachments.
const FILENAME_KEY: &CStr = c"filename";
const MIMETYPE_KEY: &CStr = c"mimetype";

/// Create one `AVMEDIA_TYPE_ATTACHMENT` output stream per `-attach` request on
/// `mux`.
///
/// Called from `outputs_bind` AFTER every mapped/encoded stream has been created
/// and BEFORE metadata processing, exactly where FFmpeg runs `of_add_attachments`.
/// Attachment streams are created with the raw [`avformat_new_stream`] — **not**
/// [`Muxer::new_stream`], which would register a scheduler node and bump the mux
/// worker's stream count, making it wait forever for packets an attachment never
/// produces. They therefore take the highest context indices
/// `[mux.nb_streams, oc.nb_streams)` and stay outside the packet path entirely.
///
/// # Errors
/// A missing / unreadable / empty / oversized file, a NUL byte in the filename or
/// mimetype, an empty explicit mimetype, or an FFmpeg allocation failure all
/// surface as an `Err` (propagated out of the context build). Never panics.
///
/// # Safety
/// `mux.out_fmt_ctx_ptr()` must point at a live output `AVFormatContext` still
/// owned by `mux` — true throughout `outputs_bind`, before the mux worker takes
/// ownership.
pub(crate) unsafe fn create_attachment_streams(mux: &Muxer) -> Result<()> {
    if mux.attachments.is_empty() {
        return Ok(());
    }

    let oc = mux.out_fmt_ctx_ptr();
    debug_assert!(!oc.is_null(), "attachment: output context already taken");

    // Invariant the index math depends on: every stream so far came from
    // `Muxer::new_stream`, which increments `nb_streams` in lockstep with
    // `avformat_new_stream`. If a future change creates a stream after this
    // point without updating `nb_streams`, this catches it in debug builds.
    debug_assert_eq!(
        (*oc).nb_streams as usize,
        mux.nb_streams,
        "attachment streams must be created after every mapped stream"
    );

    warn_if_unsupported_muxer(oc);

    const PAD: usize = AV_INPUT_BUFFER_PADDING_SIZE as usize;

    for spec in &mux.attachments {
        let path_display = spec.path.display().to_string();

        // (a+b) Read the file into memory with a hard upper bound, rejecting
        //     non-regular paths (`/dev/zero`, FIFOs, char devices) that report
        //     size 0 yet read forever. See `read_attachment_bytes`.
        let bytes = read_attachment_bytes(&spec.path, &path_display)?;
        let len = bytes.len();

        // (c) Resolve the mimetype (explicit override or extension guess).
        //     Matroska REQUIRES a non-empty mimetype tag, so reject an empty
        //     override up front rather than failing at header write.
        let mimetype = match &spec.mimetype {
            Some(m) if m.is_empty() => {
                return Err(OpenOutputError::AttachmentEmptyMimetype(path_display).into());
            }
            Some(m) => m.clone(),
            None => guess_mimetype(&spec.path),
        };

        // NUL-validate the dynamic metadata strings BEFORE any FFI allocation,
        // so an interior NUL is a clean `Err` with nothing to unwind.
        let basename = spec
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("attachment");
        let filename_c = CString::new(basename)?;
        let mimetype_c = CString::new(mimetype.as_str())?;

        // (d) Create the stream FIRST. If a later step fails, a stream with a
        //     null `extradata` is freed cleanly by `avformat_free_context` when
        //     the context drops — nothing dangles, nothing double-frees.
        let st = avformat_new_stream(oc, std::ptr::null());
        if st.is_null() {
            return Err(OpenOutputError::OutOfMemory.into());
        }
        let par = (*st).codecpar;

        // (e) `av_mallocz` the extradata WITH padding, fully zero-initialized so
        //     the trailing AV_INPUT_BUFFER_PADDING_SIZE bytes are zero per the
        //     `extradata` contract. It MUST come from the av_malloc family:
        //     FFmpeg frees it via `avcodec_parameters_free` -> `av_free`. Never a
        //     Rust allocation. `len <= MAX_ATTACHMENT_SIZE`, so `len + PAD` cannot
        //     overflow `usize` nor exceed `i32::MAX`.
        let total = len + PAD;
        let buf = av_mallocz(total) as *mut u8;
        if buf.is_null() {
            // `st` is owned by `oc` and freed with the context later. No leak.
            return Err(OpenOutputError::OutOfMemory.into());
        }
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, len);

        // (f) OWNERSHIP TRANSFER: from here FFmpeg owns `buf`. We never free it.
        (*par).extradata = buf;
        (*par).extradata_size = len as i32;
        (*par).codec_type = AVMEDIA_TYPE_ATTACHMENT;
        (*par).codec_id = guess_codec_id(&spec.path);

        // (g) `filename` + `mimetype` tags (flag 0 = overwrite). `process_metadata`
        //     runs after this, so a user-supplied `-metadata:s:t mimetype=...`
        //     still overrides our guess — the FFmpeg-parity "user wins" behavior.
        set_stream_tag(st, FILENAME_KEY, &filename_c)?;
        set_stream_tag(st, MIMETYPE_KEY, &mimetype_c)?;
    }

    Ok(())
}

/// Read an attachment file into memory with a hard size bound.
///
/// Opens the path once (not `stat`-then-`read`) and rejects anything that is not
/// a regular file: a non-regular path such as `/dev/zero`, a FIFO, or a char
/// device reports length 0, slips past a size preflight, then reads without
/// bound — a hang / OOM the preflight cannot see. Opening first and checking the
/// file type closes that hole and the TOCTOU window where a file grows between
/// the stat and the read. The read itself is capped at `MAX_ATTACHMENT_SIZE + 1`
/// bytes via `Read::take`, so an oversized (or size-lying) file cannot OOM;
/// reaching `MAX + 1` bytes means it is over the cap. Returns `Err` for a
/// missing/unreadable/non-regular/empty/oversized file — never blocks unbounded.
fn read_attachment_bytes(path: &Path, path_display: &str) -> Result<Vec<u8>> {
    let file = File::open(path)
        .map_err(|e| OpenOutputError::AttachmentRead(path_display.to_string(), e))?;
    let meta = file
        .metadata()
        .map_err(|e| OpenOutputError::AttachmentRead(path_display.to_string(), e))?;
    if !meta.file_type().is_file() {
        let e = std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a regular file");
        return Err(OpenOutputError::AttachmentRead(path_display.to_string(), e).into());
    }

    let mut bytes = Vec::new();
    file.take(MAX_ATTACHMENT_SIZE + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| OpenOutputError::AttachmentRead(path_display.to_string(), e))?;

    if bytes.is_empty() {
        return Err(OpenOutputError::AttachmentEmpty(path_display.to_string()).into());
    }
    check_attachment_size(path_display, bytes.len() as u64)?;
    Ok(bytes)
}

/// Refuse attachments larger than [`MAX_ATTACHMENT_SIZE`]. Split out so the cap
/// is unit-testable without allocating a giant file.
fn check_attachment_size(path_display: &str, len: u64) -> Result<()> {
    if len > MAX_ATTACHMENT_SIZE {
        return Err(OpenOutputError::AttachmentTooLarge(
            path_display.to_string(),
            len,
            MAX_ATTACHMENT_SIZE,
        )
        .into());
    }
    Ok(())
}

/// MIME type guessed from the file extension. The two font types match values
/// the crate's subtitle loader round-trips (`src/subtitle/loader.rs`), so an
/// attachment written here loads back as a font.
fn guess_mimetype(path: &Path) -> String {
    match extension_lower(path).as_deref() {
        Some("ttf") => "application/x-truetype-font".to_string(),
        Some("otf") => "application/vnd.ms-opentype".to_string(),
        _ => "application/octet-stream".to_string(),
    }
}

/// `codec_id` for the attachment, set for fidelity (the load-bearing value for
/// Matroska is the `mimetype` tag). Matches FFmpeg's TTF/OTF handling.
fn guess_codec_id(path: &Path) -> AVCodecID {
    match extension_lower(path).as_deref() {
        Some("ttf") => AV_CODEC_ID_TTF,
        Some("otf") => AV_CODEC_ID_OTF,
        _ => AV_CODEC_ID_NONE,
    }
}

/// Lower-cased file extension, if any. Uses [`Path::extension`] (never byte-index
/// `&str` slicing), so multibyte paths are safe.
fn extension_lower(path: &Path) -> Option<String> {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
}

/// `av_dict_set(&st->metadata, key, value, 0)` with error propagation. Ignoring
/// a negative return would silently drop the tag and defer the failure to
/// header write, so we surface it here.
unsafe fn set_stream_tag(st: *mut AVStream, key: &CStr, value: &CStr) -> Result<()> {
    let ret = av_dict_set(&mut (*st).metadata, key.as_ptr(), value.as_ptr(), 0);
    if ret < 0 {
        return Err(OpenOutputError::from(ret).into());
    }
    Ok(())
}

/// Non-blocking UX hint: warn when the output format is not Matroska/WebM (the
/// only muxers that reliably accept attachment streams). We do NOT hard-reject —
/// FFmpeg lets the muxer decide at header write, and so do we.
///
/// # Safety
/// `oc` must be a live output `AVFormatContext`.
unsafe fn warn_if_unsupported_muxer(oc: *mut ffmpeg_sys_next::AVFormatContext) {
    let oformat = (*oc).oformat;
    if oformat.is_null() || (*oformat).name.is_null() {
        return;
    }
    let name = CStr::from_ptr((*oformat).name).to_string_lossy();
    if !(name.contains("matroska") || name.contains("webm")) {
        log::warn!(
            "output format '{name}' may not support attachment streams; \
             attachments are reliably supported only by Matroska/WebM, and the \
             muxer may reject them when writing the header"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    // A non-regular path (`/dev/zero` reads forever, reports size 0) must be
    // rejected immediately by the open+is_file guard, never slurped into memory.
    // If this regresses, `read_to_end` would hang instead of returning.
    #[cfg(unix)]
    #[test]
    fn rejects_non_regular_file_without_hanging() {
        let err = read_attachment_bytes(Path::new("/dev/zero"), "/dev/zero").unwrap_err();
        assert!(
            matches!(
                err,
                Error::OpenOutput(OpenOutputError::AttachmentRead(_, _))
            ),
            "expected AttachmentRead for a non-regular file, got {err:?}"
        );
    }

    #[test]
    fn rejects_missing_file() {
        let err = read_attachment_bytes(
            Path::new("/nonexistent/ez-ffmpeg/attachment.bin"),
            "/nonexistent/ez-ffmpeg/attachment.bin",
        )
        .unwrap_err();
        assert!(
            matches!(
                err,
                Error::OpenOutput(OpenOutputError::AttachmentRead(_, _))
            ),
            "expected AttachmentRead for a missing file, got {err:?}"
        );
    }

    #[test]
    fn oversized_attachment_is_rejected_without_allocating() {
        // One byte over the cap must error purely from the length — no file, no
        // allocation.
        let err = check_attachment_size("big.bin", MAX_ATTACHMENT_SIZE + 1).unwrap_err();
        assert!(
            matches!(
                err,
                Error::OpenOutput(OpenOutputError::AttachmentTooLarge(_, len, cap))
                    if len == MAX_ATTACHMENT_SIZE + 1 && cap == MAX_ATTACHMENT_SIZE
            ),
            "expected AttachmentTooLarge, got {err:?}"
        );
    }

    #[test]
    fn exactly_at_cap_is_allowed() {
        assert!(check_attachment_size("ok.bin", MAX_ATTACHMENT_SIZE).is_ok());
        assert!(check_attachment_size("zero.bin", 0).is_ok());
    }

    #[test]
    fn mimetype_guessing_is_case_insensitive_and_matches_font_table() {
        assert_eq!(
            guess_mimetype(Path::new("DejaVuSans.ttf")),
            "application/x-truetype-font"
        );
        assert_eq!(
            guess_mimetype(Path::new("FONT.OTF")),
            "application/vnd.ms-opentype"
        );
        assert_eq!(
            guess_mimetype(Path::new("cover.png")),
            "application/octet-stream"
        );
        assert_eq!(
            guess_mimetype(Path::new("noextension")),
            "application/octet-stream"
        );
    }

    #[test]
    fn codec_id_guessing_matches_extension() {
        assert_eq!(guess_codec_id(Path::new("a.ttf")), AV_CODEC_ID_TTF);
        assert_eq!(guess_codec_id(Path::new("a.OtF")), AV_CODEC_ID_OTF);
        assert_eq!(guess_codec_id(Path::new("a.bin")), AV_CODEC_ID_NONE);
    }
}
