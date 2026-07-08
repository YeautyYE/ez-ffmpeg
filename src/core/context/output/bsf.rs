use super::Output;

impl Output {
    /// Sets the **bitstream-filter chain** for the **video** output stream(s),
    /// equivalent to FFmpeg `-bsf:v`.
    ///
    /// Bitstream filters transform encoded packets without decoding them (e.g.
    /// rewriting NAL headers). The chain runs in the mux stage — after stream
    /// copy or encoding, before the packet is written — matching the FFmpeg CLI.
    ///
    /// # Arguments
    /// * `bsf_chain` - A single BSF name (e.g. `"h264_mp4toannexb"`) or a
    ///   comma-separated chain with per-filter options
    ///   (e.g. `"hevc_metadata=aud=insert,extract_extradata"`), parsed by
    ///   FFmpeg's `av_bsf_list_parse_str`. An **empty string clears** any
    ///   previously set video BSF.
    ///
    /// # Returns
    /// * `Self` - Returns the modified `Output`, allowing for method chaining.
    ///
    /// # Examples
    /// ```rust,ignore
    /// // Convert H.264 from MP4 (length-prefixed) to Annex B for MPEG-TS.
    /// let output = Output::from("output.ts")
    ///     .set_format("mpegts")
    ///     .add_stream_map_with_copy("0:v")
    ///     .set_video_bsf("h264_mp4toannexb");
    /// ```
    pub fn set_video_bsf(mut self, bsf_chain: impl Into<String>) -> Self {
        let chain = bsf_chain.into();
        self.video_bsf = if chain.is_empty() { None } else { Some(chain) };
        self
    }

    /// Sets the **bitstream-filter chain** for the **audio** output stream(s),
    /// equivalent to FFmpeg `-bsf:a`.
    ///
    /// See [`set_video_bsf`](Self::set_video_bsf) for the chain syntax; an
    /// **empty string clears** any previously set audio BSF.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.aac")
    ///     .add_stream_map_with_copy("0:a")
    ///     .set_audio_bsf("aac_adtstoasc");
    /// ```
    pub fn set_audio_bsf(mut self, bsf_chain: impl Into<String>) -> Self {
        let chain = bsf_chain.into();
        self.audio_bsf = if chain.is_empty() { None } else { Some(chain) };
        self
    }

    /// Sets the **bitstream-filter chain** for the **subtitle** output
    /// stream(s), equivalent to FFmpeg `-bsf:s`.
    ///
    /// See [`set_video_bsf`](Self::set_video_bsf) for the chain syntax; an
    /// **empty string clears** any previously set subtitle BSF.
    pub fn set_subtitle_bsf(mut self, bsf_chain: impl Into<String>) -> Self {
        let chain = bsf_chain.into();
        self.subtitle_bsf = if chain.is_empty() { None } else { Some(chain) };
        self
    }
}
