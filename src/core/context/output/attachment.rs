use super::Output;

impl Output {
    /// Embeds a file as a container **attachment** stream (FFmpeg `-attach`),
    /// e.g. a `.ttf`/`.otf` font for Matroska subtitle rendering, or cover art.
    ///
    /// The MIME type is guessed from the file extension
    /// (`.ttf` → `application/x-truetype-font`,
    /// `.otf` → `application/vnd.ms-opentype`, otherwise
    /// `application/octet-stream`). Use
    /// [`add_attachment_with_mimetype`](Self::add_attachment_with_mimetype) to
    /// set it explicitly.
    ///
    /// # Behavior & limitations
    /// - **Local files only.** The path is read with `std::fs` at build time,
    ///   so protocol URLs (`http:`, `pipe:`, …) are **not** supported here
    ///   (unlike some FFmpeg inputs). A missing, unreadable, empty, or oversized
    ///   file surfaces as an `Err` from
    ///   [`FfmpegContext`](crate::FfmpegContext) build — never a panic. The
    ///   setter itself does no I/O and never fails.
    /// - **Muxer support.** Attachments are supported only by **Matroska/WebM**
    ///   (`.mkv`/`.webm`). MP4, MOV, MPEG-TS and other muxers do not accept a
    ///   generic attachment stream and will reject the job at header-write time
    ///   (surfacing as an `Err` from the running job, not a panic).
    /// - Must be used **alongside at least one mapped/encoded stream**: an
    ///   output whose only stream is an attachment is not supported.
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mkv")
    ///     .add_stream_map("0:v")
    ///     .add_attachment("assets/DejaVuSans.ttf");
    /// ```
    pub fn add_attachment(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.attachments.push(AttachmentSpec {
            path: path.into(),
            mimetype: None,
        });
        self
    }

    /// Same as [`add_attachment`](Self::add_attachment) but with an explicit
    /// MIME type (e.g. `"application/x-truetype-font"`, `"image/png"`) instead
    /// of guessing from the extension.
    ///
    /// An empty `mimetype` is rejected at build time (Matroska requires a
    /// non-empty mimetype tag for every attachment).
    ///
    /// # Example
    /// ```rust,ignore
    /// let output = Output::from("output.mkv")
    ///     .add_stream_map("0:v")
    ///     .add_attachment_with_mimetype("cover.png", "image/png");
    /// ```
    pub fn add_attachment_with_mimetype(
        mut self,
        path: impl Into<std::path::PathBuf>,
        mimetype: impl Into<String>,
    ) -> Self {
        self.attachments.push(AttachmentSpec {
            path: path.into(),
            mimetype: Some(mimetype.into()),
        });
        self
    }
}

/// One `-attach` request. The file is **not** read here; I/O is deferred to
/// output build time so the [`Output`] setters stay infallible.
///
/// Created via [`Output::add_attachment`] /
/// [`Output::add_attachment_with_mimetype`].
#[derive(Debug, Clone)]
pub(crate) struct AttachmentSpec {
    /// Path to the file whose bytes become the attachment payload (local file).
    pub(crate) path: std::path::PathBuf,
    /// Explicit MIME type override. `None` ⇒ guess from the file extension.
    pub(crate) mimetype: Option<String>,
}
