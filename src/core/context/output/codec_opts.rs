use super::Output;
use std::collections::HashMap;

impl Output {
    /// Sets a **video codec-specific option**.
    ///
    /// These options control **video encoding parameters** such as compression, quality, and speed.
    ///
    /// **Supported Parameters:**
    /// | Parameter | Description |
    /// |-----------|-------------|
    /// | `crf=0-51` | Quality level for x264/x265, lower means higher quality (`0` is lossless) |
    /// | `preset=ultrafast, superfast, fast, medium, slow, veryslow` | Encoding speed, affects compression efficiency |
    /// | `tune=film, animation, grain, stillimage, fastdecode, zerolatency` | Optimizations for specific types of content |
    /// | `b=4M` | Bitrate (e.g., `4M` for 4 Mbps) |
    /// | `g=50` | GOP (Group of Pictures) size, affects keyframe frequency |
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_video_codec_opt("crf", "18")
    ///     .set_video_codec_opt("preset", "fast");
    /// ```
    pub fn set_video_codec_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let Some(ref mut opts) = self.video_codec_opts {
            opts.insert(key.into(), value.into());
        } else {
            let mut opts = HashMap::new();
            opts.insert(key.into(), value.into());
            self.video_codec_opts = Some(opts);
        }
        self
    }

    /// **Sets multiple video codec options at once.**
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_video_codec_opts(vec![
    ///         ("crf", "18"),
    ///         ("preset", "fast")
    ///     ]);
    /// ```
    pub fn set_video_codec_opts(
        mut self,
        opts: Vec<(impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let video_opts = self.video_codec_opts.get_or_insert_with(HashMap::new);
        for (key, value) in opts {
            video_opts.insert(key.into(), value.into());
        }
        self
    }

    /// Sets a **audio codec-specific option**.
    ///
    /// These options control **audio encoding parameters** such as bitrate, sample rate, and format.
    ///
    /// **Supported Parameters:**
    /// | Parameter | Description |
    /// |-----------|-------------|
    /// | `b=192k` | Bitrate (e.g., `128k` for 128 Kbps, `320k` for 320 Kbps) |
    /// | `compression_level=0-12` | Compression efficiency for formats like FLAC |
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_audio_codec_opt("b", "320k")
    ///     .set_audio_codec_opt("compression_level", "6");
    /// ```
    pub fn set_audio_codec_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let Some(ref mut opts) = self.audio_codec_opts {
            opts.insert(key.into(), value.into());
        } else {
            let mut opts = HashMap::new();
            opts.insert(key.into(), value.into());
            self.audio_codec_opts = Some(opts);
        }
        self
    }

    /// **Sets multiple audio codec options at once.**
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_audio_codec_opts(vec![
    ///         ("b", "320k"),
    ///         ("compression_level", "6")
    ///     ]);
    /// ```
    pub fn set_audio_codec_opts(
        mut self,
        opts: Vec<(impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let audio_opts = self.audio_codec_opts.get_or_insert_with(HashMap::new);
        for (key, value) in opts {
            audio_opts.insert(key.into(), value.into());
        }
        self
    }

    /// Sets a **subtitle codec-specific option**.
    ///
    /// These options control **subtitle encoding parameters** such as format and character encoding.
    ///
    /// **Supported Parameters:**
    /// | Parameter | Description |
    /// |-----------|-------------|
    /// | `mov_text` | Subtitle format for MP4 files |
    /// | `srt` | Subtitle format for `.srt` files |
    /// | `ass` | Advanced SubStation Alpha (ASS) subtitle format |
    /// | `forced_subs=1` | Forces the subtitles to always be displayed |
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_subtitle_codec_opt("mov_text", "");
    /// ```
    pub fn set_subtitle_codec_opt(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        if let Some(ref mut opts) = self.subtitle_codec_opts {
            opts.insert(key.into(), value.into());
        } else {
            let mut opts = HashMap::new();
            opts.insert(key.into(), value.into());
            self.subtitle_codec_opts = Some(opts);
        }
        self
    }

    /// **Sets multiple subtitle codec options at once.**
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_subtitle_codec_opts(vec![
    ///         ("mov_text", ""),
    ///         ("forced_subs", "1")
    ///     ]);
    /// ```
    pub fn set_subtitle_codec_opts(
        mut self,
        opts: Vec<(impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let subtitle_opts = self.subtitle_codec_opts.get_or_insert_with(HashMap::new);
        for (key, value) in opts {
            subtitle_opts.insert(key.into(), value.into());
        }
        self
    }

    /// Sets a format-specific option for the output container.
    ///
    /// FFmpeg supports various format-specific options that can be passed to the muxer.
    /// These options allow fine-tuning of the output container’s behavior.
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_format_opt("movflags", "faststart")
    ///     .set_format_opt("flvflags", "no_duration_filesize");
    /// ```
    ///
    /// ### Common Format Options:
    /// | Format | Option | Description |
    /// |--------|--------|-------------|
    /// | `mp4`  | `movflags=faststart` | Moves moov atom to the beginning of the file for faster playback start |
    /// | `flv`  | `flvflags=no_duration_filesize` | Removes duration/size metadata for live streaming |
    ///
    /// **Parameters:**
    /// - `key`: The format option name (e.g., `"movflags"`, `"flvflags"`).
    /// - `value`: The value to set (e.g., `"faststart"`, `"no_duration_filesize"`).
    ///
    /// Returns the modified `Output` struct for chaining.
    pub fn set_format_opt(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let Some(ref mut opts) = self.format_opts {
            opts.insert(key.into(), value.into());
        } else {
            let mut opts = HashMap::new();
            opts.insert(key.into(), value.into());
            self.format_opts = Some(opts);
        }
        self
    }

    /// Sets multiple format-specific options at once.
    ///
    /// This method allows setting multiple format options in a single call.
    ///
    /// **Example Usage:**
    /// ```rust,ignore
    /// let output = Output::from("some_url")
    ///     .set_format_opts(vec![
    ///         ("movflags", "faststart"),
    ///         ("flvflags", "no_duration_filesize")
    ///     ]);
    /// ```
    ///
    /// **Parameters:**
    /// - `opts`: A vector of key-value pairs representing format options.
    ///
    /// Returns the modified `Output` struct for chaining.
    pub fn set_format_opts(mut self, opts: Vec<(impl Into<String>, impl Into<String>)>) -> Self {
        if let Some(ref mut format_opts) = self.format_opts {
            for (key, value) in opts {
                format_opts.insert(key.into(), value.into());
            }
        } else {
            let mut format_opts = HashMap::new();
            for (key, value) in opts {
                format_opts.insert(key.into(), value.into());
            }
            self.format_opts = Some(format_opts);
        }
        self
    }
}
