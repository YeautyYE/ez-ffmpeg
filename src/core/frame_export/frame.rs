//! The owned, packed [`VideoFrame`] output type.

use super::options::PixelLayout;

/// One exported video frame: tightly packed 8-bit pixels plus metadata.
///
/// The byte buffer is owned and has no row padding — `as_bytes().len()` equals
/// `width * height * layout.bytes_per_pixel()` and the row stride equals
/// `width * layout.bytes_per_pixel()`. This is the shape ML/CV consumers expect
/// (feed it straight into an ndarray view, a tensor, or an image encoder).
pub struct VideoFrame {
    width: u32,
    height: u32,
    layout: PixelLayout,
    pts_us: Option<i64>,
    index: u64,
    data: Vec<u8>,
    /// Drop-time return path to the producing run's buffer pool. `None` once
    /// [`into_vec`](VideoFrame::into_vec) detaches the buffer (or when the
    /// frame was built without a pool). Purely an allocation-recycling
    /// channel: it never blocks and never affects the data contract.
    recycle: Option<crossbeam_channel::Sender<Vec<u8>>>,
}

impl VideoFrame {
    /// Builds a frame from an already-packed, tight buffer. Crate-internal: the
    /// sink guarantees `data.len() == width * height * layout.bytes_per_pixel()`.
    /// `recycle` is the sink pool's return path for the buffer (dropped frames
    /// hand their allocation back for the next frame to reuse).
    pub(crate) fn new(
        width: u32,
        height: u32,
        layout: PixelLayout,
        pts_us: Option<i64>,
        index: u64,
        data: Vec<u8>,
        recycle: Option<crossbeam_channel::Sender<Vec<u8>>>,
    ) -> Self {
        debug_assert_eq!(
            data.len(),
            width as usize * height as usize * layout.bytes_per_pixel(),
            "VideoFrame buffer must be tightly packed"
        );
        Self {
            width,
            height,
            layout,
            pts_us,
            index,
            data,
            recycle,
        }
    }

    /// Frame width in pixels.
    pub fn width(&self) -> u32 {
        self.width
    }

    /// Frame height in pixels.
    pub fn height(&self) -> u32 {
        self.height
    }

    /// The packed pixel layout of [`as_bytes`](VideoFrame::as_bytes).
    pub fn layout(&self) -> PixelLayout {
        self.layout
    }

    /// Post-filter presentation time in microseconds, normalized to the start
    /// of the extraction window (the stream start when no `start_time_us` was
    /// set). Vsync passthrough preserves the source timing one-to-one; `None`
    /// when the frame carried no usable timestamp.
    pub fn pts_us(&self) -> Option<i64> {
        self.pts_us
    }

    /// 0-based export index (counts delivered frames in order).
    pub fn index(&self) -> u64 {
        self.index
    }

    /// The packed pixel bytes. Length is `width * height * bytes_per_pixel`;
    /// rows are tight (`row_bytes()` each) and top-down.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Consumes the frame and returns the owned packed buffer (no copy).
    pub fn into_vec(mut self) -> Vec<u8> {
        // Detach from the buffer pool first: `Drop` still runs for `self`, and
        // with `recycle` cleared it has nothing to send — the caller owns the
        // allocation outright and the pool simply allocates fresh next time.
        self.recycle = None;
        std::mem::take(&mut self.data)
    }

    /// Bytes per row: `width * layout.bytes_per_pixel()`.
    pub fn row_bytes(&self) -> usize {
        self.width as usize * self.layout.bytes_per_pixel()
    }
}

impl Drop for VideoFrame {
    fn drop(&mut self) {
        if let Some(recycle) = self.recycle.take() {
            // Non-blocking by design: a full pool or a finished run (receiver
            // gone) just means the buffer is freed here instead of reused.
            let _ = recycle.try_send(std::mem::take(&mut self.data));
        }
    }
}

impl std::fmt::Debug for VideoFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VideoFrame")
            .field("width", &self.width)
            .field("height", &self.height)
            .field("layout", &self.layout)
            .field("pts_us", &self.pts_us)
            .field("index", &self.index)
            .field("bytes", &self.data.len())
            .finish()
    }
}
