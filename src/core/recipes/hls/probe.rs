//! Ladder-specific trial `avcodec_open2` for auto encoder selection.

use std::collections::HashMap;
use std::ffi::{c_void, CString};
use std::ptr::null_mut;

use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_NONE;
use ffmpeg_sys_next::{
    av_buffer_ref, av_get_pix_fmt, av_opt_set_dict2, avcodec_alloc_context3,
    avcodec_find_encoder_by_name, avcodec_get_hw_config, avcodec_open2, AVRational, AVERROR,
    AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX, AV_OPT_SEARCH_CHILDREN, ENOMEM,
};

use crate::core::context::CodecContext;
use crate::hwaccel::hw_device_get_by_type;
use crate::util::ffmpeg_utils::{av_err2str, hashmap_to_avdictionary, DictGuard};

use super::encoder::{EncoderOpener, EncoderProbePlan, OpenFail, RenditionProbe};

pub(super) struct FfmpegOpener;

impl EncoderOpener for FfmpegOpener {
    fn try_open(&self, plan: &EncoderProbePlan<'_>) -> std::result::Result<(), OpenFail> {
        crate::core::initialize_ffmpeg();
        trial_open(plan)
    }
}

/// Trial-open every rendition of one encoder candidate.
///
/// Successfully opened codec contexts stay alive until **all** rungs of this
/// candidate have opened, then they are dropped together. That hold is not a
/// performance shortcut: the real ladder encodes rungs concurrently through
/// a `split` filter, so hardware session occupancy (consumer NVENC session
/// limits in particular) is only proven if the probe holds those sessions at
/// the same time. Sequential open/close would false-pass a host that can
/// open one session at a time but cannot hold the full ladder.
fn trial_open(plan: &EncoderProbePlan<'_>) -> std::result::Result<(), OpenFail> {
    let name = CString::new(plan.encoder_name).map_err(|_| OpenFail {
        width: plan.renditions.first().map(|r| r.width).unwrap_or(0),
        height: plan.renditions.first().map(|r| r.height).unwrap_or(0),
        raw_code: AVERROR(ffmpeg_sys_next::EINVAL),
        message: "encoder name contains an interior NUL".to_string(),
    })?;

    let codec = unsafe { avcodec_find_encoder_by_name(name.as_ptr()) };
    if codec.is_null() {
        let r = plan.renditions.first();
        return Err(OpenFail {
            width: r.map(|r| r.width).unwrap_or(0),
            height: r.map(|r| r.height).unwrap_or(0),
            raw_code: ffmpeg_sys_next::AVERROR_ENCODER_NOT_FOUND,
            message: av_err2str(ffmpeg_sys_next::AVERROR_ENCODER_NOT_FOUND),
        });
    }

    let pix_cstr = CString::new(plan.pixel_format).map_err(|_| {
        fail_rung(
            plan.renditions.first(),
            AVERROR(ffmpeg_sys_next::EINVAL),
            "invalid pixel format",
        )
    })?;
    let pix_fmt = unsafe { av_get_pix_fmt(pix_cstr.as_ptr()) };
    if pix_fmt == AV_PIX_FMT_NONE {
        return Err(fail_rung(
            plan.renditions.first(),
            AVERROR(ffmpeg_sys_next::EINVAL),
            "unknown pixel format",
        ));
    }

    let (fps_num, fps_den) = plan.fps;
    if fps_num <= 0 || fps_den <= 0 {
        return Err(fail_rung(
            plan.renditions.first(),
            AVERROR(ffmpeg_sys_next::EINVAL),
            "invalid frame rate",
        ));
    }

    // Keep every successfully opened context alive until all rungs succeed.
    let mut sessions: Vec<CodecContext> = Vec::with_capacity(plan.renditions.len());
    for rung in &plan.renditions {
        match open_rung(codec, pix_fmt, fps_num, fps_den, rung) {
            Ok(ctx) => sessions.push(ctx),
            Err(fail) => return Err(fail),
        }
    }
    drop(sessions);
    Ok(())
}

fn fail_rung(rung: Option<&RenditionProbe>, raw_code: i32, message: &str) -> OpenFail {
    OpenFail {
        width: rung.map(|r| r.width).unwrap_or(0),
        height: rung.map(|r| r.height).unwrap_or(0),
        raw_code,
        message: message.to_string(),
    }
}

fn open_rung(
    codec: *const ffmpeg_sys_next::AVCodec,
    pix_fmt: ffmpeg_sys_next::AVPixelFormat,
    fps_num: i64,
    fps_den: i64,
    rung: &RenditionProbe,
) -> std::result::Result<CodecContext, OpenFail> {
    let ctx_ptr = unsafe { avcodec_alloc_context3(codec) };
    if ctx_ptr.is_null() {
        return Err(OpenFail {
            width: rung.width,
            height: rung.height,
            raw_code: AVERROR(ENOMEM),
            message: av_err2str(AVERROR(ENOMEM)),
        });
    }
    let ctx = CodecContext::new(ctx_ptr);

    let mut gop_applied = false;
    unsafe {
        (*ctx.as_mut_ptr()).width = i32::try_from(rung.width).unwrap_or(i32::MAX);
        (*ctx.as_mut_ptr()).height = i32::try_from(rung.height).unwrap_or(i32::MAX);
        (*ctx.as_mut_ptr()).pix_fmt = pix_fmt;
        (*ctx.as_mut_ptr()).time_base = AVRational {
            num: i32::try_from(fps_den).unwrap_or(1),
            den: i32::try_from(fps_num).unwrap_or(1),
        };
        (*ctx.as_mut_ptr()).framerate = AVRational {
            num: i32::try_from(fps_num).unwrap_or(1),
            den: i32::try_from(fps_den).unwrap_or(1),
        };
        (*ctx.as_mut_ptr()).bit_rate = rung.bit_rate as _;
        (*ctx.as_mut_ptr()).rc_max_rate = rung.max_rate as _;
        if rung.buffer_size > 0 {
            (*ctx.as_mut_ptr()).rc_buffer_size = rung.buffer_size as _;
        }
        if let Some((_, g)) = rung.options.iter().find(|(k, _)| k == "g") {
            if let Ok(gop) = g.parse::<i32>() {
                (*ctx.as_mut_ptr()).gop_size = gop;
                gop_applied = true;
            }
        }
        (*ctx.as_mut_ptr()).max_b_frames = 0;
        (*ctx.as_mut_ptr()).sample_aspect_ratio = AVRational { num: 1, den: 1 };
    }

    let mut map = HashMap::new();
    for (k, v) in &rung.options {
        let ck = CString::new(k.as_str()).map_err(|_| OpenFail {
            width: rung.width,
            height: rung.height,
            raw_code: AVERROR(ffmpeg_sys_next::EINVAL),
            message: format!("option key '{k}' contains an interior NUL"),
        })?;
        let cv = CString::new(v.as_str()).map_err(|_| OpenFail {
            width: rung.width,
            height: rung.height,
            raw_code: AVERROR(ffmpeg_sys_next::EINVAL),
            message: format!("option value for '{k}' contains an interior NUL"),
        })?;
        map.insert(ck, cv);
    }
    // Mirror Output::set_video_bitrate: AVOption `b` uses the original
    // bitrate string (e.g. "2800k"), not the numeric bit_rate field.
    if let (Ok(bk), Ok(bv)) = (CString::new("b"), CString::new(rung.b_opt.as_str())) {
        map.insert(bk, bv);
    }

    let mut dict = DictGuard::new(hashmap_to_avdictionary(&Some(map)));
    let ret = unsafe {
        av_opt_set_dict2(
            ctx.as_mut_ptr() as *mut c_void,
            dict.as_double_ptr(),
            AV_OPT_SEARCH_CHILDREN,
        )
    };
    if ret < 0 {
        return Err(OpenFail {
            width: rung.width,
            height: rung.height,
            raw_code: ret,
            message: av_err2str(ret),
        });
    }
    // Context fields already carry bitrate and max_b_frames. Leftover `b`/`bf`
    // are redundant with those fields. Leftover `g` is redundant only when
    // `gop_size` was actually applied — an unparseable `g` must fail closed.
    let leftover: Vec<String> = dict
        .leftover_keys()
        .into_iter()
        .filter(|k| match k.as_str() {
            "b" | "bf" => false,
            "g" => !gop_applied,
            _ => true,
        })
        .collect();
    if !leftover.is_empty() {
        return Err(OpenFail {
            width: rung.width,
            height: rung.height,
            raw_code: AVERROR(ffmpeg_sys_next::EINVAL),
            message: format!(
                "encoder did not consume HLS trial option(s): {}",
                leftover.join(", ")
            ),
        });
    }

    unsafe {
        attach_hw_device(ctx.as_mut_ptr());
        // Match enc_task: libavcodec defaults to thread_count=1. Production
        // opens set 0 (auto) unless the caller passed `threads`.
        if !rung.options.iter().any(|(k, _)| k == "threads") {
            (*ctx.as_mut_ptr()).thread_count = 0;
        }
        let ret = avcodec_open2(ctx.as_mut_ptr(), codec, null_mut());
        if ret < 0 {
            return Err(OpenFail {
                width: rung.width,
                height: rung.height,
                raw_code: ret,
                message: av_err2str(ret),
            });
        }
    }
    Ok(ctx)
}

unsafe fn attach_hw_device(enc_ctx: *mut ffmpeg_sys_next::AVCodecContext) {
    // Same no-frames path as `enc_task::hw_device_setup_for_encode`: scan
    // every hw config, reuse the first already-registered device, and do
    // not `hw_device_init_from_type`. Creating a device here would let the
    // trial succeed with a context the real open would not have (it only
    // calls `hw_device_get_by_type`). A missing device leaves
    // `hw_device_ctx` null; `avcodec_open2` then reports the encoder error.
    let mut dev = None;
    let mut i = 0;
    loop {
        let config = avcodec_get_hw_config((*enc_ctx).codec, i);
        if config.is_null() {
            break;
        }
        if dev.is_none() && (*config).methods & AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX as i32 != 0
        {
            dev = hw_device_get_by_type((*config).device_type);
        }
        i += 1;
    }
    if let Some(dev) = dev {
        (*enc_ctx).hw_device_ctx = av_buffer_ref(dev.device_ref());
    }
}

#[cfg(test)]
mod leftover_tests {
    use super::super::encoder::{EncoderProbePlan, RenditionProbe};
    use super::*;

    fn mpeg4_plan(options: Vec<(String, String)>) -> EncoderProbePlan<'static> {
        EncoderProbePlan {
            encoder_name: "mpeg4",
            fps: (25, 1),
            pixel_format: "yuv420p",
            renditions: vec![RenditionProbe {
                width: 320,
                height: 240,
                bit_rate: 200_000,
                max_rate: 200_000,
                buffer_size: 400_000,
                b_opt: "200k".into(),
                options,
            }],
        }
    }

    #[test]
    fn trial_open_rejects_unconsumed_encoder_options() {
        crate::core::initialize_ffmpeg();
        let err = trial_open(&mpeg4_plan(vec![("no_such_hls_opt".into(), "1".into())]))
            .expect_err("leftover option must fail closed");
        assert!(
            err.message.contains("no_such_hls_opt"),
            "leftover message must name the key, got {}",
            err.message
        );
    }

    #[test]
    fn trial_open_rejects_unparseable_gop_option() {
        crate::core::initialize_ffmpeg();
        let err = trial_open(&mpeg4_plan(vec![("g".into(), "not-a-gop".into())]))
            .expect_err("unparseable g must fail closed when leftover");
        assert!(
            err.message.contains('g') || err.message.contains("not-a-gop") || err.raw_code != 0,
            "unparseable g must not be treated as applied, got {}",
            err.message
        );
    }

    #[test]
    fn trial_open_does_not_register_hw_devices() {
        crate::core::initialize_ffmpeg();
        let _registry = crate::hwaccel::HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = crate::hwaccel::hw_device_registry_len();
        trial_open(&mpeg4_plan(Vec::new())).expect("mpeg4 trial-open must succeed");
        let after_ok = crate::hwaccel::hw_device_registry_len();
        assert_eq!(
            before, after_ok,
            "successful software trial-open must not call hw_device_init_from_type"
        );
        let _ = trial_open(&mpeg4_plan(vec![("no_such_hls_opt".into(), "1".into())]));
        let after_err = crate::hwaccel::hw_device_registry_len();
        assert_eq!(
            before, after_err,
            "failed trial-open must not register hardware devices"
        );
    }
}
