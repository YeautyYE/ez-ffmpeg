//! Safe RAII ownership of a raw `*mut AVBSFContext` plus its receive-side
//! scratch `*mut AVPacket`.
//!
//! [`BitStreamFilter`] owns one parsed bitstream-filter chain
//! (`av_bsf_list_parse_str`) and the `AVPacket` shell that
//! `av_bsf_receive_packet` drains into — the same pairing fftools keeps in
//! `MuxStream::{bsf_ctx,bsf_pkt}` (ffmpeg_mux.c). Move-only single owner,
//! like [`FormatContext`](super::FormatContext): Drop frees both exactly once.
//!
//! ## Why the FFI surface is declared locally
//!
//! `ffmpeg-sys-next` does not bind `libavcodec/bsf.h`: its bindgen input for
//! libavcodec names only `avcodec.h`, `dv_profile.h` and `vorbis_parser.h`,
//! and since FFmpeg 5.0 `avcodec.h` no longer includes `bsf.h` — so no
//! `av_bsf_*` symbol and no `AVBSFContext` exist in the generated bindings.
//! The symbols themselves are exported by every linked libavcodec in the
//! crate's supported range (FFmpeg 7.0–8.x), so this module declares the
//! five entry points it needs and mirrors the public `AVBSFContext` layout,
//! which is part of FFmpeg's public ABI and has been field-for-field
//! identical since FFmpeg 5.0 (`libavcodec/bsf.h`: av_class, filter,
//! priv_data, par_in, par_out, time_base_in, time_base_out).

use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_void};
use std::ptr::null_mut;

use ffmpeg_sys_next::{
    av_packet_alloc, av_packet_free, AVCodecParameters, AVPacket, AVRational, AVERROR, ENOMEM,
};

/// Mirror of the public `AVBSFContext` from `libavcodec/bsf.h` (FFmpeg
/// 5.0+ layout, unchanged through 8.x). `av_class` and `filter` are opaque
/// here: only the parameter/time-base fields are touched from Rust.
#[repr(C)]
pub(crate) struct AVBSFContext {
    pub(crate) av_class: *const c_void,
    pub(crate) filter: *const c_void,
    pub(crate) priv_data: *mut c_void,
    /// Input stream parameters; filled by the caller before `av_bsf_init`.
    pub(crate) par_in: *mut AVCodecParameters,
    /// Output stream parameters; set by the filter in `av_bsf_init`.
    pub(crate) par_out: *mut AVCodecParameters,
    /// Timebase of input packets; set by the caller before `av_bsf_init`.
    pub(crate) time_base_in: AVRational,
    /// Timebase of output packets; set by the filter in `av_bsf_init`.
    pub(crate) time_base_out: AVRational,
}

extern "C" {
    fn av_bsf_list_parse_str(str_: *const c_char, bsf: *mut *mut AVBSFContext) -> c_int;
    fn av_bsf_init(ctx: *mut AVBSFContext) -> c_int;
    fn av_bsf_send_packet(ctx: *mut AVBSFContext, pkt: *mut AVPacket) -> c_int;
    fn av_bsf_receive_packet(ctx: *mut AVBSFContext, pkt: *mut AVPacket) -> c_int;
    fn av_bsf_free(ctx: *mut *mut AVBSFContext);
}

/// Sole owner of one bitstream-filter chain and its receive scratch packet.
///
/// Move-only (no `Clone`/`Copy`): a single owner means a single [`Drop`], so
/// `av_bsf_free`/`av_packet_free` run exactly once — including on every
/// early-error path between `parse` and a successful `av_bsf_init`.
pub(crate) struct BitStreamFilter {
    ctx: *mut AVBSFContext,
    pkt: *mut AVPacket,
}

// SAFETY: same reasoning as `FormatContext`'s `unsafe impl Send`. Both
// pointers are only dereferenced from the thread that owns the value; the
// muxer builds it on the scheduler thread and moves it into the mux worker,
// never sharing it. `Send` only, never `Sync`.
unsafe impl Send for BitStreamFilter {}

impl BitStreamFilter {
    /// Parse an FFmpeg BSF chain string (single name or comma-separated,
    /// `av_bsf_list_parse_str` syntax) and allocate the receive packet.
    ///
    /// The returned value owns the un-initialized context: the caller fills
    /// `par_in`/`time_base_in` and calls [`init`](Self::init) before any
    /// packet I/O. On error the raw averror code is returned and nothing
    /// leaks.
    pub(crate) fn parse(chain: &CStr) -> Result<Self, c_int> {
        let mut ctx: *mut AVBSFContext = null_mut();
        // SAFETY: `chain` is NUL-terminated; `ctx` is a valid out-param.
        // On success ownership of the allocated context transfers to `this`.
        let ret = unsafe { av_bsf_list_parse_str(chain.as_ptr(), &mut ctx) };
        if ret < 0 || ctx.is_null() {
            return Err(if ret < 0 { ret } else { AVERROR(ENOMEM) });
        }
        let mut this = Self {
            ctx,
            pkt: null_mut(),
        };
        // SAFETY: plain allocation; `this` already owns `ctx`, so an
        // allocation failure drops (and frees) it.
        let pkt = unsafe { av_packet_alloc() };
        if pkt.is_null() {
            return Err(AVERROR(ENOMEM));
        }
        this.pkt = pkt;
        Ok(this)
    }

    /// Borrow the raw context for field access (`par_in`, `time_base_in`,
    /// ...) or FFI. Must not be freed and must not outlive `self`.
    pub(crate) fn as_ptr(&self) -> *mut AVBSFContext {
        self.ctx
    }

    /// Borrow the owned receive-side scratch packet. Must not be freed and
    /// must not outlive `self`.
    pub(crate) fn pkt_ptr(&self) -> *mut AVPacket {
        self.pkt
    }

    /// `av_bsf_init`: prepare the chain after `par_in`/`time_base_in` were
    /// filled. Returns the raw averror code.
    ///
    /// # Safety
    /// `par_in` must hold valid codec parameters and `time_base_in` a valid
    /// timebase for the stream this filter will process.
    pub(crate) unsafe fn init(&mut self) -> c_int {
        av_bsf_init(self.ctx)
    }

    /// `av_bsf_send_packet`. A null `pkt` (or empty packet) signals EOF and
    /// starts the flush. On success FFmpeg takes ownership of the packet's
    /// contents and resets it.
    ///
    /// # Safety
    /// `pkt` must be null or point at a valid packet owned by the caller;
    /// [`init`](Self::init) must have succeeded.
    pub(crate) unsafe fn send_packet(&mut self, pkt: *mut AVPacket) -> c_int {
        av_bsf_send_packet(self.ctx, pkt)
    }

    /// `av_bsf_receive_packet` into the wrapper's own scratch packet, which
    /// the caller must leave clean (moved-from or unreffed) after each use.
    ///
    /// # Safety
    /// [`init`](Self::init) must have succeeded and the scratch packet must
    /// be clean from the previous iteration.
    pub(crate) unsafe fn receive_packet(&mut self) -> c_int {
        av_bsf_receive_packet(self.ctx, self.pkt)
    }

    /// Timebase of the filter's output packets (valid after a successful
    /// [`init`](Self::init)).
    pub(crate) fn time_base_out(&self) -> AVRational {
        // SAFETY: `ctx` is non-null for the lifetime of `self` (invariant
        // established by `parse`) and the field is plain data.
        unsafe { (*self.ctx).time_base_out }
    }

    /// Timebase the filter expects input packets in (valid after a
    /// successful [`init`](Self::init)).
    pub(crate) fn time_base_in(&self) -> AVRational {
        // SAFETY: as in `time_base_out`.
        unsafe { (*self.ctx).time_base_in }
    }
}

impl Drop for BitStreamFilter {
    fn drop(&mut self) {
        // SAFETY: both frees are null-safe on the inner pointer and null the
        // location, so a partially-constructed value (pkt still null) and a
        // fully-built one are both freed exactly once.
        unsafe {
            av_packet_free(&mut self.pkt);
            av_bsf_free(&mut self.ctx);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::{
        av_new_packet, av_packet_alloc, av_packet_free, AVCodecID, AVMediaType, AVERROR_EOF, EAGAIN,
    };
    use std::ffi::CString;

    /// The `"null"` chain must hand every packet through unchanged
    /// (payload, pts/dts/duration) and return EOF after a flush — the
    /// identity contract every other chain builds on.
    #[test]
    fn null_bsf_is_an_identity_filter() {
        let chain = CString::new("null").unwrap();
        let mut bsf = BitStreamFilter::parse(&chain).expect("parsing 'null' chain failed");

        unsafe {
            let ctx = bsf.as_ptr();
            (*(*ctx).par_in).codec_type = AVMediaType::AVMEDIA_TYPE_VIDEO;
            (*(*ctx).par_in).codec_id = AVCodecID::AV_CODEC_ID_H264;
            (*ctx).time_base_in = AVRational { num: 1, den: 1000 };

            let ret = bsf.init();
            assert!(ret >= 0, "av_bsf_init failed: {ret}");
            assert_eq!(bsf.time_base_out().num, 1);
            assert_eq!(bsf.time_base_out().den, 1000);

            // Nothing sent yet: receive must report EAGAIN, not invent data.
            assert_eq!(bsf.receive_packet(), AVERROR(EAGAIN));

            let payload: [u8; 5] = [1, 2, 3, 4, 5];
            let mut input = av_packet_alloc();
            assert!(!input.is_null());
            assert!(av_new_packet(input, payload.len() as i32) >= 0);
            std::ptr::copy_nonoverlapping(payload.as_ptr(), (*input).data, payload.len());
            (*input).pts = 100;
            (*input).dts = 90;
            (*input).duration = 10;

            assert!(bsf.send_packet(input) >= 0, "send_packet failed");
            // send_packet took the contents; the shell is ours to free.
            av_packet_free(&mut input);

            assert!(bsf.receive_packet() >= 0, "receive_packet failed");
            let out = bsf.pkt_ptr();
            assert_eq!((*out).pts, 100);
            assert_eq!((*out).dts, 90);
            assert_eq!((*out).duration, 10);
            assert_eq!((*out).size, payload.len() as i32);
            let out_payload = std::slice::from_raw_parts((*out).data, payload.len());
            assert_eq!(out_payload, &payload);
            ffmpeg_sys_next::av_packet_unref(out);

            // Exactly one output per input for the identity chain.
            assert_eq!(bsf.receive_packet(), AVERROR(EAGAIN));

            // Flush: NULL send, then drain reports EOF (bsf.h flush contract).
            assert!(bsf.send_packet(std::ptr::null_mut()) >= 0);
            assert_eq!(bsf.receive_packet(), AVERROR_EOF);
        }
    }

    /// An unknown filter name must fail at parse time with a clear error,
    /// not defer the failure to init/packet time.
    #[test]
    fn unknown_chain_fails_at_parse() {
        let chain = CString::new("definitely_not_a_bsf").unwrap();
        let err = match BitStreamFilter::parse(&chain) {
            Err(code) => code,
            Ok(_) => panic!("parsing an unknown BSF name must fail"),
        };
        assert!(err < 0);
    }
}
