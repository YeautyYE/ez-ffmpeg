pub struct FilterComplex {
    pub(crate) filter_descs: String,
    pub(crate) hw_device: Option<String>,

    /// sws (libswscale) option string applied to the **auto-inserted** `scale`
    /// filters of this graph (FFmpeg `AVFilterGraph.scale_sws_opts`).
    /// Default `None`, i.e. libavfilter's built-in defaults.
    pub(crate) sws_opts: Option<String>,

    /// swr (libswresample) option string applied to the **auto-inserted**
    /// `aresample` filters of this graph (FFmpeg `AVFilterGraph.aresample_swr_opts`).
    /// Default `None`.
    pub(crate) swr_opts: Option<String>,
}

impl FilterComplex {
    /// Sets sws (libswscale) options for the scale filters libavfilter
    /// **auto-inserts** into this graph to reconcile pixel format / size / color
    /// mismatches.
    ///
    /// This maps to FFmpeg's graph-level `AVFilterGraph.scale_sws_opts`. It only
    /// affects the *auto-inserted* `scale` filters; a `scale=...` you write
    /// explicitly in the filtergraph description keeps its own arguments.
    ///
    /// The string uses FFmpeg option syntax, e.g.
    /// `"flags=lanczos+accurate_rnd"`.
    ///
    /// # Graph-level, not per-output
    /// FFmpeg's API is graph-level: one value applies to the whole graph. When a
    /// `FilterComplex` also has [`Output::set_sws_opts`](crate::core::context::output::Output::set_sws_opts)
    /// on its bound outputs, the `FilterComplex` value wins. Two bound outputs
    /// requesting *different* non-empty values (with no `FilterComplex` value to
    /// arbitrate) is rejected when the graph is configured.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ez_ffmpeg::core::context::filter_complex::FilterComplex;
    /// let fc = FilterComplex::from("scale=1280:720,format=yuv420p")
    ///     .set_sws_opts("flags=lanczos+accurate_rnd");
    /// ```
    pub fn set_sws_opts(mut self, opts: impl Into<String>) -> Self {
        self.sws_opts = Some(opts.into());
        self
    }

    /// Sets swr (libswresample) options for the `aresample` filters libavfilter
    /// **auto-inserts** into this graph to reconcile sample format / rate /
    /// channel-layout mismatches.
    ///
    /// This maps to FFmpeg's graph-level `AVFilterGraph.aresample_swr_opts`. It
    /// only affects the *auto-inserted* `aresample` filters; an `aresample=...`
    /// you write explicitly keeps its own arguments.
    ///
    /// The string uses FFmpeg option syntax, e.g.
    /// `"resampler=soxr:precision=28"`.
    ///
    /// # Graph-level, not per-output
    /// See [`set_sws_opts`](Self::set_sws_opts): the value is graph-level and the
    /// same precedence / conflict rules apply.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ez_ffmpeg::core::context::filter_complex::FilterComplex;
    /// let fc = FilterComplex::from("aresample=48000")
    ///     .set_swr_opts("resampler=soxr:precision=28");
    /// ```
    pub fn set_swr_opts(mut self, opts: impl Into<String>) -> Self {
        self.swr_opts = Some(opts.into());
        self
    }

    /// Assigns a hardware device for this filter complex, enabling GPU-accelerated
    /// or device-specific filtering.
    ///
    /// # Parameters
    /// * `hw_device` - A `String` specifying the hardware device name or identifier
    ///   recognized by FFmpeg (e.g., `"cuda"`, `"vaapi"`, `"dxva2"`, etc.).
    ///
    /// # Returns
    /// A modified `FilterComplex` that includes the specified hardware device.
    ///
    /// # Example
    /// ```rust,ignore
    /// use ez_ffmpeg::core::context::filter_complex::FilterComplex;
    ///
    /// // Create a FilterComplex for scaling, then set a CUDA device
    /// let mut fc = FilterComplex::from("scale=1280:720")
    ///     .set_hw_device("cuda");
    /// ```
    pub fn set_hw_device(mut self, hw_device: impl Into<String>) -> Self {
        self.hw_device = Some(hw_device.into());
        self
    }
}

impl From<String> for FilterComplex {
    fn from(filter_descs: String) -> Self {
        Self {
            filter_descs,
            hw_device: None,
            sws_opts: None,
            swr_opts: None,
        }
    }
}

impl From<&str> for FilterComplex {
    fn from(filter_descs: &str) -> Self {
        Self {
            filter_descs: filter_descs.to_string(),
            hw_device: None,
            sws_opts: None,
            swr_opts: None,
        }
    }
}
