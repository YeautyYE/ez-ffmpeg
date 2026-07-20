### CLI-to-API mapping

The table below maps common `ffmpeg` command-line flags and patterns to their
ez-ffmpeg equivalents. The **Kind** column tells you *how* the mapping works:

- **typed** — a dedicated builder method with a typed signature.
- **option** — a string key/value forwarded to FFmpeg's option system
  (`AVOption`); the same names the CLI accepts, without the leading dash.
- **filter** — expressed as an FFmpeg filtergraph string via
  [`filter_desc`](crate::core::context::ffmpeg_context_builder::FfmpegContextBuilder::filter_desc).
- **recipe** — a one-shot helper that owns the whole workflow.
- **gap** — not implemented; the notes state the failure mode and workaround.

One global convention: CLI *seconds* become *microseconds* in `_us` methods
(`-ss 10` → `10_000_000`).

| FFmpeg CLI | Kind | ez-ffmpeg API | Status / notes |
|------------|------|---------------|----------------|
| `-i <file/URL>` | typed | [`Input::from`](crate::Input), builder [`input`](crate::core::context::ffmpeg_context_builder::FfmpegContextBuilder::input) | Files, network URLs, device strings. |
| read/write custom I/O (`pipe:`) | typed | [`Input::new_by_read_callback`](crate::Input::new_by_read_callback), [`Output::new_by_write_callback`](crate::Output::new_by_write_callback) | In-memory / streaming I/O; add seek callbacks for seekable media. |
| `-f <fmt>` (force format) | typed | [`Input::set_format`](crate::Input::set_format), [`Output::set_format`](crate::Output::set_format) | E.g. `"lavfi"`, `"hls"`, `"segment"`, `"null"`. |
| `-ss` / `-t` / `-to` (before `-i`) | typed | [`Input::set_start_time_us`](crate::Input::set_start_time_us) / [`set_recording_time_us`](crate::Input::set_recording_time_us) / [`set_stop_time_us`](crate::Input::set_stop_time_us) | Input-side seek/trim. See anchor below. |
| `-ss` / `-t` / `-to` (before output) | typed | [`Output::set_start_time_us`](crate::Output::set_start_time_us) / [`set_recording_time_us`](crate::Output::set_recording_time_us) / [`set_stop_time_us`](crate::Output::set_stop_time_us) | Output-side trim (decode-then-discard semantics). |
| `-stream_loop N` | typed | [`Input::set_stream_loop`](crate::Input::set_stream_loop) | `-1` loops forever. |
| `-re` | typed | [`Input::set_readrate`](crate::Input::set_readrate) | `1.0` reads at native speed. |
| `-shortest` | typed | [`Output::set_shortest`](crate::Output::set_shortest) | Frame-accurate for encoded streams; bound the buffering window with [`set_shortest_buf_duration_us`](crate::Output::set_shortest_buf_duration_us). See anchor below. |
| `-map <spec>` | typed | [`Output::add_stream_map`](crate::Output::add_stream_map) (re-encode), [`add_stream_map_with_copy`](crate::Output::add_stream_map_with_copy) (streamcopy) | Accepts `"0:v"`-style input specs or filtergraph link labels like `"[vout]"`. |
| `-vn` / `-an` / `-sn` / `-dn` | typed | [`Output::disable_video`](crate::Output::disable_video) / [`disable_audio`](crate::Output::disable_audio) / [`disable_subtitle`](crate::Output::disable_subtitle) / [`disable_data`](crate::Output::disable_data) | Per-output stream suppression. |
| `-frames:v` / `-frames:a` / `-frames:s` | typed | [`Output::set_max_video_frames`](crate::Output::set_max_video_frames) / [`set_max_audio_frames`](crate::Output::set_max_audio_frames) / [`set_max_subtitle_frames`](crate::Output::set_max_subtitle_frames) | Stop after N frames. |
| `-c:v` / `-c:a` / `-c:s <enc>` | typed | [`Output::set_video_codec`](crate::Output::set_video_codec) / [`set_audio_codec`](crate::Output::set_audio_codec) / [`set_subtitle_codec`](crate::Output::set_subtitle_codec) | Encoder by FFmpeg name (`"libx264"`, `"aac"`, ...). |
| `-c copy` (streamcopy) | typed | `set_video_codec("copy")` and siblings, or [`add_stream_map_with_copy`](crate::Output::add_stream_map_with_copy) | No decode/encode; codec must fit the target container. See anchor below. |
| `-c:v <dec>` (before `-i`, force decoder) | typed | [`Input::set_video_codec`](crate::Input::set_video_codec) / [`set_audio_codec`](crate::Input::set_audio_codec) | For streams the probe misidentifies. |
| `-crf`, `-preset`, `-profile`, `x264-params`, ... | option | [`Output::set_video_codec_opt`](crate::Output::set_video_codec_opt) / [`set_audio_codec_opt`](crate::Output::set_audio_codec_opt) | Any encoder `AVOption`, e.g. `("crf", "23")`, `("preset", "fast")`. |
| `-b:v` / `-b:a` | typed | [`Output::set_video_bitrate`](crate::Output::set_video_bitrate) / [`set_audio_bitrate`](crate::Output::set_audio_bitrate) | FFmpeg size syntax: `"2500k"`, `"5M"`. |
| `-q:v` / `-q:a` | typed | [`Output::set_video_qscale`](crate::Output::set_video_qscale) / [`set_audio_qscale`](crate::Output::set_audio_qscale) | Fixed quality scale. |
| `-r` / `-fpsmax` (output) | typed | [`Output::set_framerate`](crate::Output::set_framerate) / [`set_framerate_max`](crate::Output::set_framerate_max) | Rational `num, den` (`30, 1`; `24000, 1001`). |
| `-fps_mode` / `-vsync` | typed | [`Output::set_vsync_method`](crate::Output::set_vsync_method) | [`VSyncMethod`](crate::core::context::output::VSyncMethod) enum (auto / CFR / VFR / passthrough / vscfr). |
| `-pix_fmt` | typed | [`Output::set_pix_fmt`](crate::Output::set_pix_fmt) | By name, e.g. `"yuv420p"`. |
| `-ar` / `-ac` / `-sample_fmt` | typed | [`Output::set_audio_sample_rate`](crate::Output::set_audio_sample_rate) / [`set_audio_channels`](crate::Output::set_audio_channels) / [`set_audio_sample_fmt`](crate::Output::set_audio_sample_fmt) | Audio resample/layout parameters. |
| `-force_key_frames 0,5,10` | typed | [`Output::set_force_key_frames`](crate::Output::set_force_key_frames) | Comma-separated absolute times in **seconds** only; the `expr:` / `HH:MM:SS` / `source` forms are rejected. |
| `-bsf:v` / `-bsf:a` / `-bsf:s` | typed | [`Output::set_video_bsf`](crate::Output::set_video_bsf) / [`set_audio_bsf`](crate::Output::set_audio_bsf) / [`set_subtitle_bsf`](crate::Output::set_subtitle_bsf) | Single filter or comma-separated chain (`"h264_mp4toannexb"`). |
| `-tag:v hvc1` (FourCC / codec tag) | gap | — | Not exposed. Setting `("tag", ...)` as a codec option does not reach `codec_tag`; re-tag with an external remux for now. |
| `-movflags +faststart` | option | [`Output::set_format_opt`](crate::Output::set_format_opt)`("movflags", "faststart")` | Muxer option. See anchor below. |
| muxer options (`-hls_time`, `-segment_time`, `-hls_list_size`, ...) | option | [`Output::set_format_opt`](crate::Output::set_format_opt) / [`set_format_opts`](crate::Output::set_format_opts) | Container-level `AVOption`s for the selected muxer. |
| demuxer/protocol options (`-rtsp_transport tcp`, `-headers`, `-loop 1`, `-probesize`) | option | [`Input::set_format_opt`](crate::Input::set_format_opt) / [`set_format_opts`](crate::Input::set_format_opts) | Applied at `avformat_open_input` time; covers protocol, demuxer and device options. |
| `-metadata`, `-metadata:s:v`, `-map_metadata` | typed | [`Output::add_metadata`](crate::Output::add_metadata), [`add_stream_metadata`](crate::Output::add_stream_metadata), [`map_metadata_from_input`](crate::Output::map_metadata_from_input) | Global, per-stream, chapter and program metadata; note `add_stream_metadata` returns `Result`. |
| `-vf` / `-filter:v` (simple per-output chain) | filter | [`Output::set_video_filter`](crate::Output::set_video_filter) | One linear video chain per output, applied to that output's re-encoded video stream; conflicts with stream copy and complex graphs are typed build errors. |
| `-af` (per-output audio chain) | gap | — | No per-output audio filter API yet; a context-level graph via `filter_desc` is the workaround. |
| `-filter_complex` | filter | builder [`filter_desc`](crate::core::context::ffmpeg_context_builder::FfmpegContextBuilder::filter_desc) | Full filtergraph syntax including labels and multiple inputs. |
| `-vf scale=1280:-2` (resize) | filter | `Output::from("out.mp4").set_video_filter("scale=1280:-2")` | Any scale expression works verbatim. |
| watermark (`overlay`) | filter | `.filter_desc("[1:v]scale=100:-1[wm];[0:v][wm]overlay=10:10")` | Second input is the watermark; see `examples/watermarking`. |
| concat several files | filter | multiple `.input(...)` + `.filter_desc("concat=n=3:v=1:a=1")` | Re-encodes; see `examples/video_merging`. |
| `-hwaccel`, `-hwaccel_device`, `-hwaccel_output_format` | typed | [`Input::set_hwaccel`](crate::Input::set_hwaccel) / [`set_hwaccel_device`](crate::Input::set_hwaccel_device) / [`set_hwaccel_output_format`](crate::Input::set_hwaccel_output_format) | `"cuda"`, `"vaapi"`, `"videotoolbox"`, ... or `"auto"`; see the [`hwaccel`](crate::hwaccel) module. |
| `silencedetect` → parsed output | recipe | [`Analysis`](crate::analysis::runner::Analysis) + [`AudioDetector::Silence`](crate::analysis::detector::AudioDetector) | Typed silence ranges instead of scraping logs. See anchor below. |
| `blackdetect` / `scdet` / `cropdetect` / `ebur128` | recipe | [`VideoDetector`](crate::analysis::detector::VideoDetector) / [`AudioDetector`](crate::analysis::detector::AudioDetector) variants | One decode pass, folded [`AnalysisReport`](crate::analysis::report::AnalysisReport). |
| `-f null -` (discard output) | typed | `Output::from("-").set_format("null")` | Run a pipeline for its side effects only. |
| `ffprobe` streams / duration | typed | [`stream_info::find_all_stream_infos`](crate::stream_info::find_all_stream_infos), [`container_info::get_duration_us`](crate::container_info::get_duration_us) | Includes per-stream metadata (title, language, rotation, ...). |
| `ffprobe -show_packets` | typed | [`packet_scanner`](crate::packet_scanner) | Iterate packet pts/dts/size/keyframe flags without decoding. |
| single thumbnail / sprite sheet | recipe | [`thumbnail`](crate::recipes::thumbnail::thumbnail), [`sprite_sheet`](crate::recipes::thumbnail::sprite_sheet) | Owns seek + scale + select + tile. See anchor below. |
| fastest thumbnail (`-skip_frame nokey`) | option | [`Input::set_video_codec_opt`](crate::Input::set_video_codec_opt)`("skip_frame", "nokey")` + input seek | Decodes keyframes only; snaps to the next keyframe. See `examples/thumbnail_extraction`. |
| GIF export (`palettegen`/`paletteuse`) | recipe | [`animated_gif`](crate::recipes::gif::animated_gif) | Two-pass palette workflow in one call. |
| HLS VOD (`-f hls -hls_time 6 ...`) | option | `set_format("hls")` + [`set_format_opt`](crate::Output::set_format_opt) | See anchor below. |
| HLS ABR ladder (multi-rendition) | recipe | [`HlsLadder`](crate::recipes::hls::HlsLadder) | One decode, N renditions, master playlist; CFR VOD only. |
| split audio into WAV chunks (`-f segment`) | option | `set_format("segment")` + `("segment_time", "10")` | Numbered output pattern `out_%03d.wav`; see `examples/split_video_to_wav`. |
| still video from image (`-loop 1 -i img -t 10`) | option | [`Input::set_format_opt`](crate::Input::set_format_opt)`("loop", "1")` + [`Output::set_recording_time_us`](crate::Output::set_recording_time_us) | See `examples/still_video_from_image`. |
| burn in subtitles (`-vf subtitles=subs.srt`) | recipe | `SubtitleFilter` frame pipeline (feature `subtitle`) | Pure-Rust renderer, works without libass; `filter_desc("subtitles=...")` also works if your FFmpeg links libass. See anchor below. |
| capture camera/mic (`-f avfoundation -i "0:0"`) | typed | [`Input::set_format`](crate::Input::set_format) + [`device`](crate::device) queries | Platform device demuxers (`avfoundation`/`dshow`/`v4l2`, ...); see `examples/capture_camera_mic`. |
| `q` keypress (stop early, finalize container) | typed | [`FfmpegScheduler`](crate::FfmpegScheduler)`::stop()` | Prompt teardown: the normal path attempts the trailer (usually a valid container), but in-flight and queued frames may be dropped, so the tail can be truncated — not a full drain. `pause()`/`resume()`/`abort()` also available. |
| `-loglevel` / reading FFmpeg's own messages | typed | [`set_ffmpeg_log_level`](crate::set_ffmpeg_log_level), [`Input::set_log_level_offset`](crate::Input::set_log_level_offset) | Forwarded to the Rust `log` facade; see the Logging section below. |
| `-progress` / `-stats` | gap | — | No stats reporting. Workaround: a `FrameFilter` observing frame timestamps against total duration (`examples/processing_progress`) — it sees decoded frames, not the CLI's encoder statistics. |
| two-pass encoding (`-pass 1/2`) | gap | — | No built-in orchestration for stats files across runs. |
| sub2video, `-fix_sub_duration` | gap | — | Not implemented; such pipelines fail with explicit errors. |

### Compile-checked CLI equivalents

The most-asked translations, as minimal compilable programs. Each block is a
doctest, so the code stays in sync with the current API.

Transcode with H.264/CRF —
`ffmpeg -i input.mkv -c:v libx264 -crf 23 -c:a aac output.mp4`:

```rust,no_run
use ez_ffmpeg::{FfmpegContext, Output};

FfmpegContext::builder()
    .input("input.mkv")
    .output(Output::from("output.mp4")
        .set_video_codec("libx264")
        .set_video_codec_opt("crf", "23")
        .set_audio_codec("aac"))
    .build()?
    .start()?
    .wait()?;
# Ok::<(), ez_ffmpeg::error::Error>(())
```

Clip without re-seeking surprises —
`ffmpeg -ss 10 -t 5 -i input.mp4 clip.mp4`:

```rust,no_run
use ez_ffmpeg::{FfmpegContext, Input};

FfmpegContext::builder()
    .input(Input::from("input.mp4")
        .set_start_time_us(10_000_000)      // -ss 10
        .set_recording_time_us(5_000_000))  // -t 5
    .output("clip.mp4")
    .build()?
    .start()?
    .wait()?;
# Ok::<(), ez_ffmpeg::error::Error>(())
```

Remux for instant web playback —
`ffmpeg -i input.mp4 -c copy -movflags faststart output.mp4`:

```rust,no_run
use ez_ffmpeg::{FfmpegContext, Output};

FfmpegContext::builder()
    .input("input.mp4")
    .output(Output::from("output.mp4")
        .set_video_codec("copy")
        .set_audio_codec("copy")
        .set_format_opt("movflags", "faststart"))
    .build()?
    .start()?
    .wait()?;
# Ok::<(), ez_ffmpeg::error::Error>(())
```

Stop when the shortest stream ends —
`ffmpeg -i video.mp4 -i audio.mp3 -map 0:v -map 1:a -shortest out.mp4`:

```rust,no_run
use ez_ffmpeg::{FfmpegContext, Output};

FfmpegContext::builder()
    .input("video.mp4")
    .input("audio.mp3")
    .output(Output::from("out.mp4")
        .add_stream_map("0:v")
        .add_stream_map("1:a")
        .set_shortest(true))
    .build()?
    .start()?
    .wait()?;
# Ok::<(), ez_ffmpeg::error::Error>(())
```

One thumbnail at a timestamp —
`ffmpeg -ss 12 -i input.mp4 -frames:v 1 -vf scale=320:-1 thumb.jpg`:

```rust,no_run
use ez_ffmpeg::recipes::{thumbnail, At, ThumbnailOptions};

thumbnail("input.mp4", "thumb.jpg", ThumbnailOptions {
    at: At::Sec(12.0),
    width: Some(320),
    ..ThumbnailOptions::default()
})?;
# Ok::<(), ez_ffmpeg::error::Error>(())
```

Silence detection as typed data —
`ffmpeg -i input.mp4 -af silencedetect=noise=-30dB:d=0.5 -f null -`:

```rust,no_run
use ez_ffmpeg::analysis::{Analysis, AudioDetector};

let report = Analysis::new("input.mp4")
    .audio_detector(AudioDetector::Silence {
        noise_db: -30.0,     // noise=-30dB
        min_duration_s: 0.5, // d=0.5
        mono: false,
    })
    .run()?;
println!("silence ranges: {:?}", report.silence);
# Ok::<(), ez_ffmpeg::error::Error>(())
```

HLS VOD packaging —
`ffmpeg -i input.mp4 -f hls -hls_time 6 -hls_playlist_type vod out/playlist.m3u8`:

```rust,no_run
use ez_ffmpeg::{FfmpegContext, Output};

FfmpegContext::builder()
    .input("input.mp4")
    .output(Output::from("out/playlist.m3u8")
        .set_format("hls")
        .set_format_opt("hls_time", "6")
        .set_format_opt("hls_playlist_type", "vod"))
    .build()?
    .start()?
    .wait()?;
# Ok::<(), ez_ffmpeg::error::Error>(())
```

Burn in subtitles without libass (feature `subtitle`) —
`ffmpeg -i input.mp4 -vf subtitles=subs.srt output.mp4`:

```rust,no_run
# #[cfg(feature = "subtitle")]
# fn burn_in() -> Result<(), Box<dyn std::error::Error>> {
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::subtitle::SubtitleFilter;
use ez_ffmpeg::{AVMediaType, FfmpegContext, Output};

let filter = SubtitleFilter::builder().file("subs.srt").build()?;
let pipeline: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
FfmpegContext::builder()
    .input("input.mp4")
    .output(Output::from("output.mp4")
        .add_frame_pipeline(pipeline.filter("subtitles", Box::new(filter))))
    .build()?
    .start()?
    .wait()?;
# Ok(()) }
# fn main() {}
```
