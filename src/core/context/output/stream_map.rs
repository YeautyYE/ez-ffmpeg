//! Per-map stream mapping parameters: the [`StreamMap`] parameter object
//! accepted by [`Output::add_stream_map`] and
//! [`Output::add_stream_map_with_copy`].
//!
//! A plain specifier string keeps working (`add_stream_map("0:v")`); the
//! parameter object adds the FFmpeg per-output-stream encoder selection
//! (`-c:v:0 libx264 -c:v:1 libx265`) and per-stream encoder options
//! (`-b:v:0 4M`, `-preset:v:0 slow`) that a per-media-type `Output` setter
//! cannot express.
//!
//! [`Output::add_stream_map`]: super::Output::add_stream_map
//! [`Output::add_stream_map_with_copy`]: super::Output::add_stream_map_with_copy

use crate::error::OpenOutputError;
use std::collections::HashMap;
use std::ffi::CString;

/// Parameters for one stream mapping of an [`Output`](super::Output):
/// an FFmpeg-style specifier plus optional per-map encoder selection.
///
/// This is the builder-API equivalent of the FFmpeg CLI's indexed
/// per-stream options. One `-map` plus its matching `-c`/`-b`/... options
/// become one `StreamMap`:
///
/// ```text
/// -map "[v0]" -c:v:0 libx264 -b:v:0 4M -preset:v:0 slow
/// -map "[v1]" -c:v:1 libx265 -crf:v:1 22
/// -map 0:a:0  -c:a:0 aac     -b:a:0 128k
/// -map 0:a:1  -c:a:1 libopus -b:a:1 96k
/// ```
///
/// ```rust,ignore
/// use ez_ffmpeg::{Output, StreamMap};
///
/// let output = Output::from("out.mkv")
///     .add_stream_map(StreamMap::new("[v0]").codec("libx264")
///         .codec_opt("b", "4M").codec_opt("preset", "slow"))
///     .add_stream_map(StreamMap::new("[v1]").codec("libx265")
///         .codec_opt("crf", "22"))
///     .add_stream_map(StreamMap::new("0:a:0").codec("aac").codec_opt("b", "128k"))
///     .add_stream_map(StreamMap::new("0:a:1").codec("libopus").codec_opt("b", "96k"));
/// ```
///
/// # Precedence
///
/// - Encoder: per-map [`codec`](Self::codec) > per-type
///   [`set_video_codec`](super::Output::set_video_codec) /
///   [`set_audio_codec`](super::Output::set_audio_codec) /
///   [`set_subtitle_codec`](super::Output::set_subtitle_codec) > the
///   container format's default encoder.
/// - Options: per-map [`codec_opt`](Self::codec_opt) entries are merged
///   **key by key** over the per-type
///   [`set_video_codec_opt`](super::Output::set_video_codec_opt) /
///   [`set_audio_codec_opt`](super::Output::set_audio_codec_opt) tables —
///   the FFmpeg semantics, where every option matches its stream specifier
///   independently (`-b:v 4M -preset:v:0 slow` leaves stream `v:0` with
///   *both* the bitrate and the preset). A per-map key overrides the
///   same-named per-type key; other per-type keys still apply.
///
/// This "more specific wins, per key" chain is the builder equivalent of
/// the CLI's "last matching option is applied" rule.
///
/// # Map granularity is binding granularity
///
/// A map that expands to several streams applies its codec to **all** of
/// them: `StreamMap::new("0:a").codec("aac")` re-encodes every audio
/// stream of input 0 with AAC (like `-c:a aac`). To give two streams
/// different encoders, write two single-stream maps (`"0:a:0"`,
/// `"0:a:1"`), exactly like splitting one coarse `-map 0` into indexed
/// maps on the CLI.
///
/// # Copy
///
/// `codec("copy")` selects stream copy for the map, equivalent to
/// [`Output::add_stream_map_with_copy`](super::Output::add_stream_map_with_copy)
/// (FFmpeg `-c:<spec> copy`). Combining copy with a *different* per-map
/// codec, or with [`codec_opt`](Self::codec_opt) entries, is rejected at
/// build time with
/// [`OpenOutputError::StreamMapCopyConflict`](crate::error::OpenOutputError::StreamMapCopyConflict):
/// copied packets never pass through an encoder, so those settings could
/// never take effect.
///
/// A negative (disabling) map such as `"-0:v"` only disables previously
/// matched streams; attaching a codec or codec options to one is rejected
/// at build time as well.
//
// NOTE (coherence): `StreamMap` must NEVER implement `Into<String>` /
// `From<StreamMap> for String`. The blanket `impl<T: Into<String>> From<T>
// for StreamMap` below only coexists with std's reflexive `impl<T> From<T>
// for T` because `StreamMap: Into<String>` does not hold; adding it would
// make the two impls overlap for `T = StreamMap` and stop compiling.
#[derive(Debug, Clone)]
pub struct StreamMap {
    /// Stream specifier string: `"0:v"`, `"1:a:0"`, `"0:v?"`, `"[label]"`, ...
    pub(crate) linklabel: String,
    /// Stream copy flag (FFmpeg `-c copy` for this map).
    pub(crate) copy: bool,
    /// Per-map encoder request (FFmpeg `-c:<spec>` for this map's streams).
    pub(crate) codec: Option<String>,
    /// Per-map encoder options in insertion order (FFmpeg `-b:<spec>`,
    /// `-preset:<spec>`, ...). Later entries win on duplicate keys.
    pub(crate) codec_opts: Vec<(String, String)>,
}

impl StreamMap {
    /// Creates a mapping for `spec`, an FFmpeg-style stream specifier
    /// (`"0:v"`, `"1:a:0"`, `"0:s?"`) or a filter output label
    /// (`"[v0]"`).
    ///
    /// Without further calls this is exactly
    /// `Output::add_stream_map(spec)`.
    pub fn new(spec: impl Into<String>) -> Self {
        Self {
            linklabel: spec.into(),
            copy: false,
            codec: None,
            codec_opts: Vec::new(),
        }
    }

    /// Selects the encoder for every stream this map matches — the
    /// builder form of FFmpeg's indexed `-c:v:0 libx264`.
    ///
    /// `"copy"` selects stream copy (see the type-level docs). Calling
    /// `codec` again replaces the previous value.
    pub fn codec(mut self, name: impl Into<String>) -> Self {
        self.codec = Some(name.into());
        self
    }

    /// Adds one encoder option for every stream this map matches — the
    /// builder form of FFmpeg's indexed `-b:v:0 4M` / `-preset:v:0 slow`.
    ///
    /// Per-map entries override same-named per-type options key by key
    /// (see the type-level docs). Repeating a key keeps the last value.
    pub fn codec_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.codec_opts.push((key.into(), value.into()));
        self
    }

    /// Resolves the user-facing fields into bind-time form: normalizes
    /// `codec("copy")` to the copy flag, rejects copy × re-encoding
    /// conflicts, and converts the option list into the `CString` table
    /// the encoder layer consumes (later duplicates win, FFmpeg
    /// last-match style). Called once per map at build time, before any
    /// expansion, so every error is a typed `build()` failure.
    pub(crate) fn resolve_for_bind(&self) -> crate::error::Result<ResolvedStreamMap> {
        let (copy, codec) = match self.codec.as_deref() {
            // FFmpeg parity: `-c:<spec> copy` means stream copy, not an
            // encoder named "copy".
            Some("copy") => (true, None),
            Some(name) => {
                if self.copy {
                    return Err(OpenOutputError::StreamMapCopyConflict {
                        spec: self.linklabel.clone(),
                        what: "a per-map codec",
                    }
                    .into());
                }
                (false, Some(name.to_string()))
            }
            None => (self.copy, None),
        };

        let codec_opts = if self.codec_opts.is_empty() {
            None
        } else if copy {
            // Copied packets never reach an encoder; silently dropping the
            // options would misrepresent the delivered configuration.
            return Err(OpenOutputError::StreamMapCopyConflict {
                spec: self.linklabel.clone(),
                what: "per-map codec options",
            }
            .into());
        } else {
            let mut map = HashMap::with_capacity(self.codec_opts.len());
            for (key, value) in &self.codec_opts {
                map.insert(CString::new(key.as_str())?, CString::new(value.as_str())?);
            }
            Some(map)
        };

        Ok(ResolvedStreamMap {
            copy,
            codec,
            codec_opts,
        })
    }
}

/// A plain specifier string is a map with no per-map overrides, so every
/// pre-existing `add_stream_map("0:v")` call keeps compiling unchanged.
impl<T: Into<String>> From<T> for StreamMap {
    fn from(linklabel: T) -> Self {
        Self::new(linklabel)
    }
}

/// Bind-time form of one [`StreamMap`]: conflicts rejected, `"copy"`
/// normalized into the flag, options converted for the encoder layer.
#[derive(Debug)]
pub(crate) struct ResolvedStreamMap {
    pub(crate) copy: bool,
    pub(crate) codec: Option<String>,
    pub(crate) codec_opts: Option<HashMap<CString, CString>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;

    fn cstr(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    #[test]
    fn new_stores_spec_without_overrides() {
        let map = StreamMap::new("0:v");
        assert_eq!(map.linklabel, "0:v");
        assert!(!map.copy);
        assert_eq!(map.codec, None);
        assert!(map.codec_opts.is_empty());
    }

    #[test]
    fn blanket_from_matches_new() {
        // The source-compat path: &str and String become bare maps.
        let from_str: StreamMap = "0:a?".into();
        let from_string: StreamMap = String::from("0:a?").into();
        for map in [from_str, from_string] {
            assert_eq!(map.linklabel, "0:a?");
            assert!(!map.copy && map.codec.is_none() && map.codec_opts.is_empty());
        }
    }

    #[test]
    fn codec_copy_normalizes_to_the_copy_flag() {
        let resolved = StreamMap::new("0:a").codec("copy").resolve_for_bind().unwrap();
        assert!(resolved.copy);
        assert_eq!(resolved.codec, None);
        assert!(resolved.codec_opts.is_none());
    }

    #[test]
    fn copy_flag_with_codec_is_a_typed_conflict() {
        let mut map = StreamMap::new("0:v").codec("libx264");
        map.copy = true; // what add_stream_map_with_copy forces
        match map.resolve_for_bind() {
            Err(Error::OpenOutput(OpenOutputError::StreamMapCopyConflict { spec, what })) => {
                assert_eq!(spec, "0:v");
                assert_eq!(what, "a per-map codec");
            }
            other => panic!("expected StreamMapCopyConflict, got {other:?}"),
        }
    }

    #[test]
    fn copy_with_codec_opts_is_a_typed_conflict() {
        let mut map = StreamMap::new("0:a").codec_opt("b", "320k");
        map.copy = true;
        match map.resolve_for_bind() {
            Err(Error::OpenOutput(OpenOutputError::StreamMapCopyConflict { spec, what })) => {
                assert_eq!(spec, "0:a");
                assert_eq!(what, "per-map codec options");
            }
            other => panic!("expected StreamMapCopyConflict, got {other:?}"),
        }
    }

    #[test]
    fn codec_copy_with_codec_opts_is_a_typed_conflict() {
        // `-c:a:0 copy -b:a:0 320k`: the options could never take effect.
        let map = StreamMap::new("0:a").codec("copy").codec_opt("b", "320k");
        assert!(matches!(
            map.resolve_for_bind(),
            Err(Error::OpenOutput(
                OpenOutputError::StreamMapCopyConflict { .. }
            ))
        ));
    }

    #[test]
    fn later_codec_call_replaces_the_earlier_one() {
        let resolved = StreamMap::new("0:v")
            .codec("copy")
            .codec("libx264")
            .resolve_for_bind()
            .unwrap();
        assert!(!resolved.copy);
        assert_eq!(resolved.codec.as_deref(), Some("libx264"));
    }

    #[test]
    fn duplicate_option_keys_keep_the_last_value() {
        let resolved = StreamMap::new("0:v")
            .codec("mpeg4")
            .codec_opt("b", "1M")
            .codec_opt("b", "4M")
            .resolve_for_bind()
            .unwrap();
        let opts = resolved.codec_opts.unwrap();
        assert_eq!(opts.len(), 1);
        assert_eq!(opts.get(&cstr("b")), Some(&cstr("4M")));
    }

    #[test]
    fn empty_option_list_resolves_to_none() {
        let resolved = StreamMap::new("0:v").codec("mpeg4").resolve_for_bind().unwrap();
        assert!(resolved.codec_opts.is_none());
    }

    #[test]
    fn interior_nul_in_option_is_a_typed_error() {
        let map = StreamMap::new("0:v").codec_opt("b\0ad", "1M");
        assert!(map.resolve_for_bind().is_err());
    }

    #[test]
    fn add_stream_map_with_copy_forces_the_copy_flag() {
        use crate::core::context::output::Output;
        let output = Output::from("out.mkv").add_stream_map_with_copy(StreamMap::new("0:a"));
        assert!(output.stream_map_specs[0].copy);

        // The string form keeps its historical behavior too.
        let output = Output::from("out.mkv").add_stream_map_with_copy("0:a?");
        assert!(output.stream_map_specs[0].copy);
        assert_eq!(output.stream_map_specs[0].linklabel, "0:a?");
    }

    #[test]
    fn add_stream_map_accepts_str_string_and_stream_map() {
        use crate::core::context::output::Output;
        // Source-compat pin for the generic signature: every historical
        // argument shape still compiles and lands in stream_map_specs.
        let output = Output::from("out.mkv")
            .add_stream_map("0:v")
            .add_stream_map(String::from("0:a:0"))
            .add_stream_map(StreamMap::new("0:a:1").codec("flac"));
        assert_eq!(output.stream_map_specs.len(), 3);
        assert_eq!(output.stream_map_specs[2].codec.as_deref(), Some("flac"));
    }
}
