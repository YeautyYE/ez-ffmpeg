//! Builder and options for [`SubtitleFilter`], plus the module's error type.

use super::ass;
use super::filter::SubtitleFilter;
use super::loader;
use super::render::fonts::FontStore;
use super::render::layout::RenderOptions;
use super::render::PureRenderer;
use super::source::SubtitleSource;
use std::ffi::CString;
use std::path::{Path, PathBuf};

/// Which system font lookup libass may use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum FontProvider {
    /// First available system provider (fontconfig / CoreText / DirectWrite).
    #[default]
    Autodetect,
    /// No system font lookup: only fonts reachable through
    /// [`SubtitleFilterBuilder::fonts_dir`] /
    /// [`SubtitleFilterBuilder::default_font_file`] are used. This makes
    /// rendering deterministic across machines and avoids any fontconfig
    /// dependency at runtime.
    None,
}

/// Text shaping engine selection (FFmpeg `ass` filter's `shaping` option).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum TextShaping {
    /// Leave libass at its default (complex shaping).
    #[default]
    Auto,
    /// Fast left-to-right shaping only; no ligatures, no bidi. Wrong for
    /// Arabic/Hebrew/Indic text, occasionally useful for speed or VSFilter
    /// compatibility.
    Simple,
    /// Full HarfBuzz shaping (ligatures, complex scripts, bidi).
    Complex,
}

/// Errors raised while configuring and loading subtitles — i.e. before any
/// frame is processed.
///
/// [`SubtitleFilterBuilder::build`] validates everything up front so callers
/// can treat subtitle problems as *soft* failures (log and simply not attach
/// the filter) instead of having a running job abort mid-transcode.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SubtitleError {
    #[error("libass initialization failed: {0}")]
    Init(String),
    #[error("subtitle parsing failed: {0}")]
    Parse(String),
    #[error("invalid subtitle option: {0}")]
    InvalidOption(String),
    #[error("font path not usable: {0}")]
    FontPath(String),
    #[error("cannot open subtitle source: {0}")]
    Open(String),
    #[error("no subtitle stream: {0}")]
    NoSubtitleStream(String),
    #[error("bitmap subtitles ({0}) cannot be burned; only text-based subtitles are supported")]
    BitmapSubtitles(String),
    #[error("subtitle decoding failed: {0}")]
    Decode(String),
}

/// Builder for [`SubtitleFilter`]; obtain one via [`SubtitleFilter::builder`].
#[derive(Default)]
#[must_use = "builders do nothing unless build() is called"]
pub struct SubtitleFilterBuilder {
    source: Option<SubtitleSource>,
    fonts_dir: Option<PathBuf>,
    default_font_file: Option<PathBuf>,
    default_family: Option<String>,
    font_provider: FontProvider,
    force_style: Option<String>,
    original_size: Option<(u32, u32)>,
    charenc: Option<String>,
    stream_index: Option<usize>,
    wrap_unicode: Option<bool>,
    shaping: TextShaping,
    font_scale: Option<f64>,
    line_position: Option<f64>,
    margins: Option<(i32, i32, i32, i32)>,
    use_margins: Option<bool>,
}

impl std::fmt::Debug for SubtitleFilterBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately hand-written: never dump subtitle content (it can be
        // an entire script).
        let source_kind = match &self.source {
            None => "none",
            Some(SubtitleSource::AssContent(_)) => "ass_content",
            Some(SubtitleSource::File(_)) => "file",
            Some(SubtitleSource::SrtContent(_)) => "srt_content",
        };
        f.debug_struct("SubtitleFilterBuilder")
            .field("source", &source_kind)
            .field("fonts_dir", &self.fonts_dir)
            .field("default_font_file", &self.default_font_file)
            .field("default_family", &self.default_family)
            .field("font_provider", &self.font_provider)
            .field("force_style", &self.force_style)
            .field("original_size", &self.original_size)
            .field("charenc", &self.charenc)
            .field("stream_index", &self.stream_index)
            .field("wrap_unicode", &self.wrap_unicode)
            .field("shaping", &self.shaping)
            .field("font_scale", &self.font_scale)
            .field("line_position", &self.line_position)
            .field("margins", &self.margins)
            .field("use_margins", &self.use_margins)
            .finish()
    }
}

impl SubtitleFilterBuilder {
    /// Uses a complete ASS/SSA script held in memory (UTF-8) — no temp file.
    pub fn ass_content(mut self, content: impl Into<String>) -> Self {
        self.source = Some(SubtitleSource::AssContent(content.into()));
        self
    }

    /// Extra directory scanned for font files (libass `fontsdir`), typically
    /// where application-managed fonts are downloaded to.
    pub fn fonts_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.fonts_dir = Some(dir.into());
        self
    }

    /// Path to a concrete font file used as the default font. Combined with
    /// [`FontProvider::None`] this pins rendering to exactly this file and
    /// eliminates lookup races between a fonts dir and a family name.
    pub fn default_font_file(mut self, font: impl Into<PathBuf>) -> Self {
        self.default_font_file = Some(font.into());
        self
    }

    /// Family name used when the script's style does not resolve to a font.
    pub fn default_family(mut self, family: impl Into<String>) -> Self {
        self.default_family = Some(family.into());
        self
    }

    /// See [`FontProvider`]; defaults to [`FontProvider::Autodetect`].
    pub fn font_provider(mut self, provider: FontProvider) -> Self {
        self.font_provider = provider;
        self
    }

    /// Comma-separated `Style.Field=Value` overrides applied to the script's
    /// style definitions (same semantics as the FFmpeg `subtitles` filter's
    /// `force_style`). Inline override tags in dialogue lines still win.
    pub fn force_style(mut self, style: impl Into<String>) -> Self {
        self.force_style = Some(style.into());
        self
    }

    /// Resolution the subtitles were authored against, used to compensate the
    /// aspect ratio when the video was resized (FFmpeg `original_size`).
    pub fn original_size(mut self, width: u32, height: u32) -> Self {
        self.original_size = Some((width, height));
        self
    }

    /// Character encoding of file/SRT sources (FFmpeg `charenc`; requires the
    /// linked FFmpeg to support iconv). Not applicable to
    /// [`Self::ass_content`], which must already be UTF-8.
    pub fn charenc(mut self, charenc: impl Into<String>) -> Self {
        self.charenc = Some(charenc.into());
        self
    }

    /// Burns subtitles from a file: .srt/.ass/.vtt, or any container holding
    /// a text subtitle stream (same lavformat/lavcodec loader FFmpeg's
    /// `subtitles` filter uses, including container font attachments).
    pub fn file(mut self, path: impl Into<PathBuf>) -> Self {
        self.source = Some(SubtitleSource::File(path.into()));
        self
    }

    /// Uses SRT content held in memory (no temp file). Markup supported by
    /// FFmpeg's SRT decoder (`<i>`, `<b>`, `<font color>`, ...) is honored.
    pub fn srt_content(mut self, content: impl Into<String>) -> Self {
        self.source = Some(SubtitleSource::SrtContent(content.into()));
        self
    }

    /// Selects which subtitle stream to burn when the source has several
    /// (0 = first subtitle stream), mirroring FFmpeg's `si` option. Only
    /// applies to [`Self::file`] sources.
    pub fn stream_index(mut self, index: usize) -> Self {
        self.stream_index = Some(index);
        self
    }

    /// Overrides the automatic Unicode line-wrapping rule (FFmpeg
    /// `wrap_unicode`). Unset = FFmpeg's auto behavior: enabled for decoded
    /// non-ASS sources (SRT/VTT/...), disabled for native ASS scripts.
    /// Requires a libass built with libunibreak; otherwise the request is
    /// logged and ignored, like FFmpeg.
    pub fn wrap_unicode(mut self, enable: bool) -> Self {
        self.wrap_unicode = Some(enable);
        self
    }

    /// Text shaping engine; defaults to [`TextShaping::Auto`] (libass's
    /// default, complex shaping), mirroring the FFmpeg `ass` filter's
    /// `shaping` option.
    pub fn shaping(mut self, shaping: TextShaping) -> Self {
        self.shaping = shaping;
        self
    }

    /// Global multiplier applied to all font sizes (accessibility-style
    /// "bigger subtitles" without editing the script). 1.0 = script sizes.
    pub fn font_scale(mut self, scale: f64) -> Self {
        self.font_scale = Some(scale);
        self
    }

    /// Vertical position of regular (non-explicitly-positioned) events, in
    /// percent: 0.0 = author intent (default), 100.0 = top of the frame.
    /// Typical "raise subtitles" values are 5–20.
    pub fn line_position(mut self, percent: f64) -> Self {
        self.line_position = Some(percent);
        self
    }

    /// Distances (pixels) from the video rectangle to the frame edges, for
    /// content with baked-in letterbox/pillarbox bars: subtitles are then
    /// laid out relative to the picture area instead of the full frame.
    /// Negative values express pan-and-scan. See also
    /// [`Self::use_margins`].
    pub fn margins(mut self, top: i32, bottom: i32, left: i32, right: i32) -> Self {
        self.margins = Some((top, bottom, left, right));
        self
    }

    /// Allows regular events to be placed on the whole frame (e.g. onto
    /// letterbox bars declared via [`Self::margins`]) instead of only
    /// inside the video rectangle.
    pub fn use_margins(mut self, enable: bool) -> Self {
        self.use_margins = Some(enable);
        self
    }

    /// Validates the configuration, initializes libass, and loads the
    /// subtitle source. All failures surface here, never mid-pipeline.
    pub fn build(self) -> Result<SubtitleFilter, SubtitleError> {
        let source = self.source.ok_or_else(|| {
            SubtitleError::InvalidOption(
                "no subtitle source set; call ass_content(...) first".to_string(),
            )
        })?;

        if let Some((width, height)) = self.original_size {
            if width == 0 || height == 0 || width > i32::MAX as u32 || height > i32::MAX as u32 {
                return Err(SubtitleError::InvalidOption(format!(
                    "original_size must be within 1..=i32::MAX, got {width}x{height}"
                )));
            }
        }

        if let Some(scale) = self.font_scale {
            if !(scale.is_finite() && scale > 0.0) {
                return Err(SubtitleError::InvalidOption(format!(
                    "font_scale must be finite and > 0, got {scale}"
                )));
            }
        }
        if let Some(percent) = self.line_position {
            if !(percent.is_finite() && (0.0..=100.0).contains(&percent)) {
                return Err(SubtitleError::InvalidOption(format!(
                    "line_position must be within 0.0..=100.0 percent, got {percent}"
                )));
            }
        }

        if let Some(dir) = &self.fonts_dir {
            if !dir.is_dir() {
                return Err(SubtitleError::FontPath(format!(
                    "fonts_dir is not an existing directory: {}",
                    dir.display()
                )));
            }
        }
        if let Some(font) = &self.default_font_file {
            if !font.is_file() {
                return Err(SubtitleError::FontPath(format!(
                    "default_font_file is not an existing file: {}",
                    font.display()
                )));
            }
        }

        let force_style_entries: Vec<String> = match &self.force_style {
            Some(style) => {
                let entries: Vec<String> = style
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect();
                if entries.iter().any(|s| s.as_bytes().contains(&0)) {
                    return Err(SubtitleError::InvalidOption(
                        "force_style contains a NUL byte".to_string(),
                    ));
                }
                entries
            }
            None => Vec::new(),
        };

        // Load the script (and any container attachments) first.
        let (mut script, attachments, native_ass) = match &source {
            SubtitleSource::AssContent(content) => {
                if self.charenc.is_some() {
                    return Err(SubtitleError::InvalidOption(
                        "charenc only applies to file/SRT sources; ass_content must be UTF-8"
                            .to_string(),
                    ));
                }
                if self.stream_index.is_some() {
                    return Err(SubtitleError::InvalidOption(
                        "stream_index only applies to file sources".to_string(),
                    ));
                }
                if content.as_bytes().contains(&0) {
                    return Err(SubtitleError::InvalidOption(
                        "ass_content contains a NUL byte".to_string(),
                    ));
                }
                let script =
                    ass::parse(content).map_err(|e| SubtitleError::Parse(e.to_string()))?;
                if !content.contains("Dialogue:") {
                    log::warn!(
                        "subtitle script parsed but contains no Dialogue events; \
                         nothing will be rendered"
                    );
                }
                (script, Vec::new(), true)
            }
            SubtitleSource::File(path) => {
                let loaded = loader::load_subtitles(
                    loader::LoaderInput::Path(path),
                    &loader::LoaderOptions {
                        charenc: self.charenc.as_deref(),
                        stream_index: self.stream_index,
                    },
                )?;
                (loaded.script, loaded.attachments, loaded.native_ass)
            }
            SubtitleSource::SrtContent(content) => {
                let loaded = loader::load_subtitles(
                    loader::LoaderInput::Memory(content.as_bytes()),
                    &loader::LoaderOptions {
                        charenc: self.charenc.as_deref(),
                        stream_index: self.stream_index,
                    },
                )?;
                (loaded.script, loaded.attachments, loaded.native_ass)
            }
        };

        if !force_style_entries.is_empty() {
            let refs: Vec<&str> = force_style_entries.iter().map(String::as_str).collect();
            ass::apply_force_style(&mut script, &refs);
        }

        // Font sources: system lookup per provider, then the explicit
        // sources (dir, pinned file, script-embedded, container-attached).
        let mut fonts = FontStore::new(self.font_provider == FontProvider::Autodetect);
        if let Some(dir) = &self.fonts_dir {
            fonts.load_fonts_dir(dir);
        }
        if let Some(font) = &self.default_font_file {
            if !fonts.load_default_font_file(font) {
                return Err(SubtitleError::FontPath(format!(
                    "default_font_file is not a parsable font file: {}",
                    font.display()
                )));
            }
        }
        for embedded in &script.fonts {
            fonts.add_memory_font(&embedded.filename.clone(), embedded.data.clone());
        }
        for (name, data) in attachments {
            fonts.add_memory_font(&name, data);
        }
        fonts.set_default_family(self.default_family.clone());
        if self.font_provider == FontProvider::None && fonts.loaded_faces() == 0 {
            log::warn!(
                "subtitle filter: FontProvider::None with no fonts_dir/default_font_file/embedded \
                 fonts; text events cannot be rendered"
            );
        }

        // FFmpeg wrap_unicode auto rule: enabled for decoded non-ASS
        // sources, disabled for native ASS scripts.
        let wrap_unicode = self.wrap_unicode.unwrap_or(!native_ass);

        let opts = RenderOptions {
            font_scale: self.font_scale.unwrap_or(1.0),
            line_position: self.line_position.unwrap_or(0.0),
            margins: self.margins,
            use_margins: self.use_margins.unwrap_or(false),
            complex_shaping: self.shaping != TextShaping::Simple,
            wrap_unicode,
        };

        let renderer = PureRenderer::new(script, fonts, opts);
        Ok(SubtitleFilter::new(Box::new(renderer), self.original_size))
    }
}

/// Converts a filesystem path into the `char *` form libass/lavformat expect.
/// Returns a neutral message; callers wrap it into the fitting error variant
/// (font paths vs subtitle sources).
pub(crate) fn path_cstring(path: &Path) -> Result<CString, String> {
    #[cfg(unix)]
    let bytes = std::os::unix::ffi::OsStrExt::as_bytes(path.as_os_str()).to_vec();
    #[cfg(not(unix))]
    let bytes = path
        .to_str()
        .ok_or_else(|| format!("path is not valid UTF-8: {}", path.display()))?
        .as_bytes()
        .to_vec();
    CString::new(bytes).map_err(|_| format!("path contains a NUL byte: {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subtitle::test_util;

    #[test]
    fn builds_from_valid_ass_content() {
        let filter = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(test_util::HELLO_EVENT))
            .build();
        assert!(filter.is_ok(), "{:?}", filter.err());
    }

    #[test]
    fn builds_with_zero_events() {
        // Legal (empty subtitles); only logs a warning.
        let filter = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(""))
            .build();
        assert!(filter.is_ok(), "{:?}", filter.err());
    }

    #[test]
    fn rejects_missing_source() {
        let err = SubtitleFilter::builder().build().unwrap_err();
        assert!(matches!(err, SubtitleError::InvalidOption(_)), "{err:?}");
    }

    #[test]
    fn rejects_garbage_content() {
        let err = SubtitleFilter::builder()
            .ass_content("definitely not an ass script")
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::Parse(_)), "{err:?}");
    }

    #[test]
    fn rejects_nul_byte_in_content() {
        let err = SubtitleFilter::builder()
            .ass_content(format!("{}\0", test_util::minimal_ass("")))
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::InvalidOption(_)), "{err:?}");
    }

    #[test]
    fn rejects_charenc_with_ass_content() {
        let err = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(""))
            .charenc("GBK")
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::InvalidOption(_)), "{err:?}");
    }

    #[test]
    fn rejects_missing_font_file() {
        let err = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(""))
            .default_font_file("/nonexistent/definitely/missing.ttf")
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::FontPath(_)), "{err:?}");
    }

    #[test]
    fn rejects_missing_fonts_dir() {
        let err = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(""))
            .fonts_dir("/nonexistent/definitely/missing-dir")
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::FontPath(_)), "{err:?}");
    }

    /// force_style must observably change rendering (FontSize doubling widens
    /// the produced glyph bitmaps).
    #[test]
    fn force_style_changes_rendering() {
        let Some(font) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };

        let max_bitmap_width = |force_style: Option<&str>| -> i32 {
            let mut builder = SubtitleFilter::builder()
                .ass_content(test_util::minimal_ass(test_util::HELLO_EVENT))
                .default_font_file(font)
                .font_provider(FontProvider::None);
            if let Some(style) = force_style {
                builder = builder.force_style(style);
            }
            let mut filter = builder.build().expect("build");
            let renderer = filter.renderer_mut();
            renderer.set_frame_size(640, 360);
            renderer.set_storage_size(640, 360);
            renderer
                .render_frame(1000)
                .iter()
                .map(|image| image.w as i32)
                .max()
                .unwrap_or(0)
        };

        let base = max_bitmap_width(None);
        let styled = max_bitmap_width(Some("FontSize=96"));
        assert!(base > 0, "baseline render produced no bitmaps");
        assert!(
            styled > base,
            "FontSize=96 should widen bitmaps (base {base}, styled {styled})"
        );
    }

    /// Renders `HELLO_EVENT` at 640x360 through a caller-tweaked builder and
    /// returns (max bitmap width, bottommost painted edge). `None` when no
    /// test font is present on this machine.
    fn hello_metrics(
        configure: impl FnOnce(SubtitleFilterBuilder) -> SubtitleFilterBuilder,
    ) -> Option<(usize, i32)> {
        let font = test_util::test_font()?;
        let builder = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(test_util::HELLO_EVENT))
            .default_font_file(font)
            .font_provider(FontProvider::None);
        let mut filter = configure(builder).build().expect("build");
        let renderer = filter.renderer_mut();
        renderer.set_frame_size(640, 360);
        renderer.set_storage_size(640, 360);
        let overlays = renderer.render_frame(1000);
        assert!(!overlays.is_empty(), "expected visible overlays");
        let max_w = overlays.iter().map(|image| image.w).max().unwrap_or(0);
        let bottom = overlays
            .iter()
            .map(|image| image.dst_y + image.h as i32)
            .max()
            .unwrap_or(0);
        Some((max_w, bottom))
    }

    #[test]
    fn shaping_option_renders_with_both_engines() {
        for shaping in [TextShaping::Simple, TextShaping::Complex] {
            let Some((width, _)) = hello_metrics(|builder| builder.shaping(shaping)) else {
                eprintln!("skipping: no known test font present on this machine");
                return;
            };
            assert!(width > 0, "{shaping:?} shaping produced empty overlays");
        }
    }

    #[test]
    fn font_scale_scales_rendering() {
        let Some((base_width, _)) = hello_metrics(|builder| builder) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let (scaled_width, _) =
            hello_metrics(|builder| builder.font_scale(2.0)).expect("font probed above");
        assert!(
            scaled_width > base_width,
            "font_scale(2.0) should widen bitmaps (base {base_width}, scaled {scaled_width})"
        );
    }

    #[test]
    fn line_position_raises_regular_events() {
        let Some((_, base_bottom)) = hello_metrics(|builder| builder) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let (_, raised_bottom) =
            hello_metrics(|builder| builder.line_position(50.0)).expect("font probed above");
        // 50% of a 360px frame is ~180px; assert with slack for style margins.
        assert!(
            raised_bottom + 100 < base_bottom,
            "line_position(50) should raise the line (base {base_bottom}, raised {raised_bottom})"
        );
    }

    #[test]
    fn margins_letterbox_and_whole_frame_placement() {
        let Some((_, base_bottom)) = hello_metrics(|builder| builder) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let (_, boxed_bottom) =
            hello_metrics(|builder| builder.margins(0, 100, 0, 0)).expect("font probed above");
        let (_, whole_bottom) =
            hello_metrics(|builder| builder.margins(0, 100, 0, 0).use_margins(true))
                .expect("font probed above");
        assert!(
            boxed_bottom <= base_bottom - 80,
            "a 100px bottom margin should lift regular events above the bar \
             (base {base_bottom}, letterboxed {boxed_bottom})"
        );
        assert!(
            whole_bottom > boxed_bottom,
            "use_margins(true) should allow placement on the bar again \
             (letterboxed {boxed_bottom}, whole-frame {whole_bottom})"
        );
    }

    #[test]
    fn rejects_invalid_font_scale_and_line_position() {
        for scale in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let err = SubtitleFilter::builder()
                .ass_content(test_util::minimal_ass(""))
                .font_scale(scale)
                .build()
                .unwrap_err();
            assert!(
                matches!(err, SubtitleError::InvalidOption(_)),
                "{scale}: {err:?}"
            );
        }
        for percent in [-0.1, 100.1, f64::NAN] {
            let err = SubtitleFilter::builder()
                .ass_content(test_util::minimal_ass(""))
                .line_position(percent)
                .build()
                .unwrap_err();
            assert!(
                matches!(err, SubtitleError::InvalidOption(_)),
                "{percent}: {err:?}"
            );
        }
    }

    const SRT_SAMPLE: &str = "1\n00:00:00,000 --> 00:00:05,000\nHello from SRT\n\n2\n00:00:06,000 --> 00:00:08,000\n<i>Second line</i>\n";

    fn write_temp(name: &str, content: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ez_ffmpeg_subtitle_test_{}_{name}",
            std::process::id()
        ));
        std::fs::write(&path, content).expect("write temp subtitle file");
        path
    }

    /// End-to-end proof of the lavformat/lavcodec loader on in-memory SRT:
    /// events render inside their window and not between windows.
    #[test]
    fn builds_from_srt_content_and_times_events() {
        let Some(font) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let mut filter = SubtitleFilter::builder()
            .srt_content(SRT_SAMPLE)
            .default_font_file(font)
            .font_provider(FontProvider::None)
            .build()
            .expect("build from srt_content");
        let renderer = filter.renderer_mut();
        renderer.set_frame_size(640, 360);
        renderer.set_storage_size(640, 360);
        assert!(
            !renderer.render_frame(1_000).is_empty(),
            "event 1 should render at t=1s"
        );
        assert!(
            renderer.render_frame(5_500).is_empty(),
            "no event spans t=5.5s"
        );
        assert!(
            !renderer.render_frame(7_000).is_empty(),
            "event 2 should render at t=7s"
        );
    }

    #[test]
    fn wrap_unicode_override_is_accepted_on_all_sources() {
        // Plumbing test: the override must reach both the loader path and
        // the native-ASS path without error (rendering differences require
        // a libass built with libunibreak, so they are not asserted here).
        for enable in [true, false] {
            assert!(SubtitleFilter::builder()
                .srt_content(SRT_SAMPLE)
                .wrap_unicode(enable)
                .build()
                .is_ok());
            assert!(SubtitleFilter::builder()
                .ass_content(test_util::minimal_ass(test_util::HELLO_EVENT))
                .wrap_unicode(enable)
                .build()
                .is_ok());
        }
    }

    #[test]
    fn builds_from_srt_file() {
        let path = write_temp("sample.srt", SRT_SAMPLE);
        let result = SubtitleFilter::builder().file(&path).build();
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn builds_from_ass_file() {
        let path = write_temp(
            "sample.ass",
            &test_util::minimal_ass(test_util::HELLO_EVENT),
        );
        let result = SubtitleFilter::builder().file(&path).build();
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn rejects_missing_file() {
        let err = SubtitleFilter::builder()
            .file("/nonexistent/definitely/missing.srt")
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::Open(_)), "{err:?}");
    }

    #[test]
    fn rejects_source_without_subtitle_stream() {
        // The repo fixture test.mp4 has no subtitle stream.
        let err = SubtitleFilter::builder()
            .file("test.mp4")
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::NoSubtitleStream(_)), "{err:?}");
    }

    #[test]
    fn rejects_out_of_range_stream_index() {
        let path = write_temp("index.srt", SRT_SAMPLE);
        let err = SubtitleFilter::builder()
            .file(&path)
            .stream_index(3)
            .build()
            .unwrap_err();
        let _ = std::fs::remove_file(&path);
        assert!(matches!(err, SubtitleError::NoSubtitleStream(_)), "{err:?}");
    }

    #[test]
    fn rejects_invalid_original_size() {
        for (width, height) in [(0u32, 1080u32), (1920, 0), (u32::MAX, 1080)] {
            let err = SubtitleFilter::builder()
                .ass_content(test_util::minimal_ass(""))
                .original_size(width, height)
                .build()
                .unwrap_err();
            assert!(
                matches!(err, SubtitleError::InvalidOption(_)),
                "{width}x{height}: {err:?}"
            );
        }
    }

    #[test]
    fn rejects_stream_index_with_ass_content() {
        let err = SubtitleFilter::builder()
            .ass_content(test_util::minimal_ass(""))
            .stream_index(0)
            .build()
            .unwrap_err();
        assert!(matches!(err, SubtitleError::InvalidOption(_)), "{err:?}");
    }
}
