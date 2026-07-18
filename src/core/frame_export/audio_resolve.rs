//! Crate-private audio stream resolver and `aformat` graph assembly.
//!
//! Runs as a deferred closure against the already-opened demuxer (S8): it
//! selects the audio stream with `av_find_best_stream` semantics (or validates
//! an explicit index) and builds the `aformat` graph description that pins
//! packed `f32` and applies any requested resample (rate) / downmix (channel
//! layout). The description references the *resolved absolute* stream index,
//! never the `[0:a]` first-match linklabel.

use super::audio_options::Channels;
use super::error::FrameExportError;
use ffmpeg_sys_next::{av_find_best_stream, AVFormatContext, AVMediaType::AVMEDIA_TYPE_AUDIO};
use std::ptr::null_mut;

/// Configuration the audio resolver needs, captured from the builder.
pub(crate) struct AudioResolvePlan {
    pub(crate) stream_index: Option<usize>,
    pub(crate) sample_rate: Option<u32>,
    pub(crate) channels: Option<Channels>,
}

/// Resolves the audio stream against `fmt_ctx` and returns the assembled
/// `filter_complex` string.
///
/// # Safety
/// `fmt_ctx` must be a valid, opened `AVFormatContext` pointer for the duration
/// of this call.
pub(crate) unsafe fn resolve_and_build_desc(
    fmt_ctx: *mut AVFormatContext,
    plan: &AudioResolvePlan,
) -> crate::error::Result<String> {
    let stream_index = resolve_stream_index(fmt_ctx, plan.stream_index)?;
    Ok(build_filter_desc(stream_index, plan))
}

/// Selects the audio stream: an explicit index is range/type-validated; the
/// default uses `av_find_best_stream` (which honors the default disposition).
///
/// # Safety
/// `fmt_ctx` must be a valid, opened `AVFormatContext` pointer.
unsafe fn resolve_stream_index(
    fmt_ctx: *mut AVFormatContext,
    explicit: Option<usize>,
) -> crate::error::Result<usize> {
    let nb_streams = (*fmt_ctx).nb_streams as usize;
    if let Some(index) = explicit {
        if index >= nb_streams {
            return Err(FrameExportError::AudioStreamIndexOutOfBounds {
                index,
                count: nb_streams,
            }
            .into());
        }
        let stream = *(*fmt_ctx).streams.add(index);
        let codecpar = (*stream).codecpar;
        if codecpar.is_null() || (*codecpar).codec_type != AVMEDIA_TYPE_AUDIO {
            return Err(FrameExportError::NotAnAudioStream { index }.into());
        }
        Ok(index)
    } else {
        let ret = av_find_best_stream(fmt_ctx, AVMEDIA_TYPE_AUDIO, -1, -1, null_mut(), 0);
        if ret < 0 {
            return Err(FrameExportError::NoAudioStream.into());
        }
        Ok(ret as usize)
    }
}

/// Assembles the `aformat` `filter_complex` string for the resolved stream.
///
/// `sample_fmts=flt` is ALWAYS present — it is the packed-`f32` half of the tap
/// contract, the twin of the `set_audio_codec("pcm_f32le")` pin. Without the pin
/// the null muxer's default `pcm_s16le` would append its own `aformat=s16` to the
/// graph output and the tap would see `s16` regardless of this clause (the graph
/// negotiates toward the encoder). Rate and channel-layout clauses are added only
/// when the caller asked for them, so the default preserves the source rate and
/// layout — no silent resample for music/analysis users.
fn build_filter_desc(stream_index: usize, plan: &AudioResolvePlan) -> String {
    // `[0:{idx}]` = input 0, ABSOLUTE stream index. `[{idx}:a]` would be wrong:
    // there the leading number is the INPUT-file index, so a resolved audio at
    // stream 1 of a single input would reference a nonexistent input 1.
    let mut desc = format!("[0:{stream_index}]aformat=sample_fmts=flt");
    if let Some(rate) = plan.sample_rate {
        desc.push_str(&format!(":sample_rates={rate}"));
    }
    if let Some(ch) = plan.channels {
        desc.push_str(&format!(":channel_layouts={}", ch.layout_name()));
    }
    desc.push_str("[export]");
    desc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(sample_rate: Option<u32>, channels: Option<Channels>) -> AudioResolvePlan {
        AudioResolvePlan {
            stream_index: None,
            sample_rate,
            channels,
        }
    }

    #[test]
    fn default_pins_flt_only() {
        let d = build_filter_desc(0, &plan(None, None));
        assert_eq!(d, "[0:0]aformat=sample_fmts=flt[export]");
    }

    #[test]
    fn absolute_stream_index_is_used() {
        let d = build_filter_desc(3, &plan(None, None));
        assert!(d.starts_with("[0:3]aformat=sample_fmts=flt"), "{d}");
    }

    #[test]
    fn rate_and_layout_clauses_are_appended_in_order() {
        let d = build_filter_desc(1, &plan(Some(16000), Some(Channels::Mono)));
        assert_eq!(
            d,
            "[0:1]aformat=sample_fmts=flt:sample_rates=16000:channel_layouts=mono[export]"
        );
    }

    #[test]
    fn rate_only_omits_layout_clause() {
        let d = build_filter_desc(0, &plan(Some(48000), None));
        assert_eq!(d, "[0:0]aformat=sample_fmts=flt:sample_rates=48000[export]");
    }

    #[test]
    fn stereo_layout_token() {
        let d = build_filter_desc(0, &plan(None, Some(Channels::Stereo)));
        assert_eq!(
            d,
            "[0:0]aformat=sample_fmts=flt:channel_layouts=stereo[export]"
        );
    }
}
