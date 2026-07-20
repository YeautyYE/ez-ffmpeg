//! IR -> builder plan. ONE lowering: [`lower`] produces a [`LoweredJob`],
//! and both consumers — [`LoweredJob::into_context`] (execution) and the
//! emitter (`super::emit`) — read the same value field by field. There is no
//! second lowering path for either side to drift through.
//!
//! Every mapping below was checked against the builder implementation, not
//! its docs: times are microseconds end to end; `-to` becomes
//! `set_stop_time_us`, and the builder derives `recording = stop - start`
//! exactly like fftools does per scope (open_input.rs / open_output.rs);
//! `-crf`/`-preset` ride the encoder AVOption dictionary
//! (`set_video_codec_opt`); `-movflags +faststart` and the `hls_*` keys ride
//! the muxer dictionary (`set_format_opt`) with their CLI values verbatim.

use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;

use super::ir::{CliIr, DurationKind, Noop};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LoweredInput {
    pub(crate) url: String,
    pub(crate) format: Option<String>,
    pub(crate) start_time_us: Option<i64>,
    pub(crate) recording_time_us: Option<i64>,
    pub(crate) stop_time_us: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LoweredOutput {
    pub(crate) url: String,
    pub(crate) format: Option<String>,
    pub(crate) start_time_us: Option<i64>,
    pub(crate) recording_time_us: Option<i64>,
    pub(crate) stop_time_us: Option<i64>,
    pub(crate) video_disable: bool,
    pub(crate) audio_disable: bool,
    /// Codec name, including the literal `copy`.
    pub(crate) video_codec: Option<String>,
    pub(crate) audio_codec: Option<String>,
    pub(crate) video_bitrate: Option<String>,
    pub(crate) audio_bitrate: Option<String>,
    /// Encoder AVOptions in command order (`crf`, `preset`).
    pub(crate) video_codec_opts: Vec<(String, String)>,
    /// Muxer AVOptions in command order (`movflags`, `hls_*`).
    pub(crate) format_opts: Vec<(String, String)>,
    pub(crate) pix_fmt: Option<String>,
    pub(crate) audio_sample_rate: Option<i32>,
    pub(crate) audio_channels: Option<i32>,
    pub(crate) max_video_frames: Option<i64>,
    pub(crate) video_filter: Option<String>,
    /// `(specifier, copy)` per `-map`, in command order.
    pub(crate) stream_maps: Vec<(String, bool)>,
}

/// The complete builder plan for one command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoweredJob {
    pub(crate) input: LoweredInput,
    pub(crate) output: LoweredOutput,
    /// Recognized no-op globals, for the emitter's comments.
    pub(crate) noops: Vec<Noop>,
}

pub(crate) fn lower(ir: &CliIr) -> LoweredJob {
    let mut input = LoweredInput {
        url: ir.input.url.clone(),
        format: ir.input.format.clone(),
        start_time_us: ir.input.start_time_us,
        recording_time_us: None,
        stop_time_us: None,
    };
    match ir.input.duration {
        Some(d) if d.kind == DurationKind::Duration => input.recording_time_us = Some(d.us),
        Some(d) => input.stop_time_us = Some(d.us),
        None => {}
    }

    let out = &ir.output;
    let mut output = LoweredOutput {
        url: out.url.clone(),
        format: out.format.clone(),
        start_time_us: out.start_time_us,
        recording_time_us: None,
        stop_time_us: None,
        video_disable: out.video_disabled,
        audio_disable: out.audio_disabled,
        video_codec: match (&out.video_codec, out.video_copy) {
            (_, true) => Some("copy".to_string()),
            (codec, false) => codec.clone(),
        },
        audio_codec: match (&out.audio_codec, out.audio_copy) {
            (_, true) => Some("copy".to_string()),
            (codec, false) => codec.clone(),
        },
        video_bitrate: out.video_bitrate.clone(),
        audio_bitrate: out.audio_bitrate.clone(),
        video_codec_opts: Vec::new(),
        format_opts: Vec::new(),
        pix_fmt: out.pix_fmt.clone(),
        audio_sample_rate: out.audio_sample_rate,
        audio_channels: out.audio_channels,
        max_video_frames: out.frames_v,
        video_filter: out.video_filter.clone(),
        stream_maps: Vec::new(),
    };
    match out.duration {
        Some(d) if d.kind == DurationKind::Duration => output.recording_time_us = Some(d.us),
        Some(d) => output.stop_time_us = Some(d.us),
        None => {}
    }
    if let Some(crf) = &out.crf {
        output
            .video_codec_opts
            .push(("crf".to_string(), crf.clone()));
    }
    if let Some(preset) = &out.preset {
        output
            .video_codec_opts
            .push(("preset".to_string(), preset.clone()));
    }
    if out.movflags_faststart {
        // The CLI value verbatim; the mp4 muxer accepts the `+flag` spelling.
        output
            .format_opts
            .push(("movflags".to_string(), "+faststart".to_string()));
    }
    for (key, value) in [
        ("hls_time", &out.hls_time),
        ("hls_playlist_type", &out.hls_playlist_type),
        ("hls_list_size", &out.hls_list_size),
        ("hls_segment_filename", &out.hls_segment_filename),
    ] {
        if let Some(value) = value {
            output.format_opts.push((key.to_string(), value.clone()));
        }
    }
    for map in &out.maps {
        // Media-qualified maps inherit their media type's copy flag; the
        // parser already rejected unqualified maps combined with copy.
        let copy =
            (map_selects(map, 'v') && out.video_copy) || (map_selects(map, 'a') && out.audio_copy);
        output.stream_maps.push((map.clone(), copy));
    }

    LoweredJob {
        input,
        output,
        noops: ir.globals.noops.clone(),
    }
}

/// Whether a validated basic map (`0`, `0:v`, `0:a:1`, `0:1`) names the given
/// media selector.
pub(crate) fn map_selects(map: &str, media: char) -> bool {
    map.split(':').nth(1).and_then(|s| s.chars().next()) == Some(media)
}

impl LoweredJob {
    /// Applies the plan to the real builder. Only callers that already
    /// passed shape verification and the runtime-profile gate may invoke
    /// this. CLI-initiated pipelines run with strict AVOption handling: a
    /// leftover option the target component did not consume fails the run
    /// instead of warning (fftools check_avoptions parity).
    pub(crate) fn into_context(self) -> crate::error::Result<FfmpegContext> {
        let mut input = Input::from(self.input.url);
        input.strict_avoptions = true;
        if let Some(format) = self.input.format {
            input = input.set_format(format);
        }
        if let Some(us) = self.input.start_time_us {
            input = input.set_start_time_us(us);
        }
        if let Some(us) = self.input.recording_time_us {
            input = input.set_recording_time_us(us);
        }
        if let Some(us) = self.input.stop_time_us {
            input = input.set_stop_time_us(us);
        }

        let mut output = Output::from(self.output.url);
        output.strict_avoptions = true;
        // The hard simple-filter prerequisite rides the SAME opening the
        // pipeline executes with (outputs_bind checks the built demuxers).
        output.require_unique_video_source = self.output.video_filter.is_some();
        if let Some(format) = self.output.format {
            output = output.set_format(format);
        }
        if let Some(us) = self.output.start_time_us {
            output = output.set_start_time_us(us);
        }
        if let Some(us) = self.output.recording_time_us {
            output = output.set_recording_time_us(us);
        }
        if let Some(us) = self.output.stop_time_us {
            output = output.set_stop_time_us(us);
        }
        if self.output.video_disable {
            output = output.disable_video();
        }
        if self.output.audio_disable {
            output = output.disable_audio();
        }
        if let Some(codec) = self.output.video_codec {
            output = output.set_video_codec(codec);
        }
        if let Some(codec) = self.output.audio_codec {
            output = output.set_audio_codec(codec);
        }
        if let Some(bitrate) = self.output.video_bitrate {
            output = output.set_video_bitrate(bitrate);
        }
        if let Some(bitrate) = self.output.audio_bitrate {
            output = output.set_audio_bitrate(bitrate);
        }
        for (key, value) in self.output.video_codec_opts {
            output = output.set_video_codec_opt(key, value);
        }
        for (key, value) in self.output.format_opts {
            output = output.set_format_opt(key, value);
        }
        if let Some(pix_fmt) = self.output.pix_fmt {
            output = output.set_pix_fmt(pix_fmt);
        }
        if let Some(rate) = self.output.audio_sample_rate {
            output = output.set_audio_sample_rate(rate);
        }
        if let Some(channels) = self.output.audio_channels {
            output = output.set_audio_channels(channels);
        }
        if let Some(frames) = self.output.max_video_frames {
            output = output.set_max_video_frames(frames);
        }
        if let Some(filter) = self.output.video_filter {
            output = output.set_video_filter(filter);
        }
        for (map, copy) in self.output.stream_maps {
            output = if copy {
                output.add_stream_map_with_copy(map)
            } else {
                output.add_stream_map(map)
            };
        }

        FfmpegContext::builder().input(input).output(output).build()
    }
}
