//! Path rasterization and bitmap effects for the pure-Rust renderer:
//! zeno-backed fill/stroke of glyph and drawing outlines, plus the ASS
//! `\be` box blur and `\blur` gaussian approximation.

use zeno::{Cap, Command, Fill, Join, Mask, Placement, Stroke, Style, Vector};

/// An 8-bit coverage bitmap positioned on the frame — the pure-Rust
/// equivalent of one `ASS_Image` node's geometry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CoverageBitmap {
    pub w: usize,
    pub h: usize,
    /// Top-left placement in frame coordinates.
    pub x: i32,
    pub y: i32,
    /// Row-major, stride == w.
    pub data: Vec<u8>,
}

impl CoverageBitmap {
    pub(crate) fn is_empty(&self) -> bool {
        self.w == 0 || self.h == 0
    }

    fn from_mask(data: Vec<u8>, placement: Placement) -> Self {
        Self {
            w: placement.width as usize,
            h: placement.height as usize,
            x: placement.left,
            y: placement.top,
            data,
        }
    }

    /// Union of two coverage bitmaps (per-pixel max) — used to merge the
    /// fill into the stroke for ASS borders.
    pub(crate) fn max_with(&self, other: &CoverageBitmap) -> CoverageBitmap {
        if self.is_empty() {
            return other.clone();
        }
        if other.is_empty() {
            return self.clone();
        }
        let x0 = self.x.min(other.x);
        let y0 = self.y.min(other.y);
        let x1 = (self.x + self.w as i32).max(other.x + other.w as i32);
        let y1 = (self.y + self.h as i32).max(other.y + other.h as i32);
        let w = (x1 - x0) as usize;
        let h = (y1 - y0) as usize;
        let mut data = vec![0u8; w * h];
        for src in [self, other] {
            let ox = (src.x - x0) as usize;
            let oy = (src.y - y0) as usize;
            for row in 0..src.h {
                let dst_row = &mut data[(oy + row) * w + ox..(oy + row) * w + ox + src.w];
                let src_row = &src.data[row * src.w..(row + 1) * src.w];
                for (d, s) in dst_row.iter_mut().zip(src_row) {
                    *d = (*d).max(*s);
                }
            }
        }
        CoverageBitmap {
            w,
            h,
            x: x0,
            y: y0,
            data,
        }
    }

    /// Restricts coverage to `rect` (frame coordinates, exclusive right and
    /// bottom edges). `inverse` zeroes the inside instead.
    pub(crate) fn clip_rect(&mut self, x0: i32, y0: i32, x1: i32, y1: i32, inverse: bool) {
        if self.is_empty() {
            return;
        }
        for row in 0..self.h {
            let frame_y = self.y + row as i32;
            let inside_row = frame_y >= y0 && frame_y < y1;
            for col in 0..self.w {
                let frame_x = self.x + col as i32;
                let inside = inside_row && frame_x >= x0 && frame_x < x1;
                if inside == inverse {
                    self.data[row * self.w + col] = 0;
                }
            }
        }
    }
}

/// A rendered node's pixel payload: geometry is per-instance (collision
/// stacking shifts placements), the coverage data is refcounted so
/// template stores and replays clone nodes without copying pixels.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SharedBitmap {
    pub w: usize,
    pub h: usize,
    /// Top-left placement in frame coordinates.
    pub x: i32,
    pub y: i32,
    /// Row-major, stride == w.
    pub data: std::sync::Arc<Vec<u8>>,
}

impl SharedBitmap {
    pub(crate) fn is_empty(&self) -> bool {
        self.w == 0 || self.h == 0
    }

    /// See [`CoverageBitmap::clip_rect`]. Payloads are still uniquely owned
    /// when clipping runs (fresh render, before any template share), so
    /// `make_mut` mutates in place; a shared payload would copy — correct
    /// either way.
    pub(crate) fn clip_rect(&mut self, x0: i32, y0: i32, x1: i32, y1: i32, inverse: bool) {
        let mut work = CoverageBitmap {
            w: self.w,
            h: self.h,
            x: self.x,
            y: self.y,
            data: std::mem::take(std::sync::Arc::make_mut(&mut self.data)),
        };
        work.clip_rect(x0, y0, x1, y1, inverse);
        *self = work.into();
    }
}

impl From<CoverageBitmap> for SharedBitmap {
    fn from(bitmap: CoverageBitmap) -> Self {
        Self {
            w: bitmap.w,
            h: bitmap.h,
            x: bitmap.x,
            y: bitmap.y,
            data: std::sync::Arc::new(bitmap.data),
        }
    }
}

/// Bounding-box size `(w, h)` of a command list in its own coordinate
/// space, `None` for an empty list. Control points count toward the box
/// (conservative: never smaller than the true curve extent). Any non-finite
/// coordinate poisons the result to infinity so callers reject the path —
/// hostile scripts can inject NaN/inf through extreme scale factors, and
/// NaN would otherwise slip past a min/max fold.
pub(crate) fn path_extent(commands: &[Command]) -> Option<(f64, f64)> {
    let mut min = (f64::INFINITY, f64::INFINITY);
    let mut max = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    let mut any = false;
    let mut poisoned = false;
    let mut visit = |p: &Vector| {
        any = true;
        let (x, y) = (f64::from(p.x), f64::from(p.y));
        if !(x.is_finite() && y.is_finite()) {
            poisoned = true;
            return;
        }
        min.0 = min.0.min(x);
        min.1 = min.1.min(y);
        max.0 = max.0.max(x);
        max.1 = max.1.max(y);
    };
    for command in commands {
        match command {
            Command::MoveTo(p) | Command::LineTo(p) => visit(p),
            Command::QuadTo(c, p) => {
                visit(c);
                visit(p);
            }
            Command::CurveTo(c1, c2, p) => {
                visit(c1);
                visit(c2);
                visit(p);
            }
            Command::Close => {}
        }
    }
    if !any {
        return None;
    }
    if poisoned {
        return Some((f64::INFINITY, f64::INFINITY));
    }
    Some((max.0 - min.0, max.1 - min.1))
}

/// Upper bound on a single coverage mask, in pixels (= bytes: masks are
/// 8-bit alpha). 2^28 = 256 MiB — 8x the area of a full 8K frame, far
/// beyond any displayable glyph, drawing or opaque box, and two orders of
/// magnitude inside zeno's u32 `width * height` sizing limit.
const MAX_MASK_AREA_PX: f64 = (1u64 << 28) as f64;

/// Whether rasterizing `commands` (padded by `pad` on every side for a
/// stroke radius) stays within [`MAX_MASK_AREA_PX`]. Two guards cooperate:
/// the layout-level extent guard is frame-RELATIVE (8x the frame per side,
/// i.e. 64x the frame area), which alone scales badly — on an 8K frame it
/// admits ~3.8 GiB masks and, once a border stroke widens the box, even
/// overflows zeno's u32 `width * height` sizing (zeno mask.rs). This
/// absolute wing keeps any admitted mask allocation bounded regardless of
/// frame size; together they act as min(64x frame area, 2^28). The +4.0
/// covers zeno's rounding and anti-aliasing slack per axis.
fn raster_area_safe(commands: &[Command], pad: f64) -> bool {
    match path_extent(commands) {
        None => true,
        Some((w, h)) => {
            let side_w = w + 2.0 * pad + 4.0;
            let side_h = h + 2.0 * pad + 4.0;
            side_w.is_finite()
                && side_h.is_finite()
                && side_w * side_h <= MAX_MASK_AREA_PX
        }
    }
}

/// Rasterizes a filled path (non-zero rule, like FreeType/VSFilter).
pub(crate) fn fill_path(commands: &[Command]) -> CoverageBitmap {
    if commands.is_empty() || !raster_area_safe(commands, 0.0) {
        return CoverageBitmap::default();
    }
    let (data, placement) = Mask::new(commands)
        .style(Style::Fill(Fill::NonZero))
        .render();
    CoverageBitmap::from_mask(data, placement)
}

/// Rasterizes the ASS border of a path: a stroke of width `2 * border`
/// merged with the fill (the stroke is centered on the contour, so it
/// extends `border` pixels outward; the union removes the inner half).
/// Like libass `ass_outline_stroke(xbord, ybord)`, the pen is an ellipse:
/// asymmetric radii stroke a circular pen in a Y-squeezed space and
/// resample the coverage back.
pub(crate) fn border_path(
    commands: &[Command],
    fill: &CoverageBitmap,
    border_x: f32,
    border_y: f32,
) -> CoverageBitmap {
    if commands.is_empty() || (border_x <= 0.0 && border_y <= 0.0) {
        return CoverageBitmap::default();
    }
    let rx = border_x.max(1.0 / 64.0);
    let ry = border_y.max(1.0 / 64.0);
    if (rx - ry).abs() <= rx.max(ry) * 1e-3 {
        if !raster_area_safe(commands, f64::from(rx.max(ry))) {
            return fill.clone();
        }
        let mut stroke = Stroke::new(2.0 * rx.max(ry));
        stroke.cap(Cap::Round).join(Join::Round);
        let (data, placement) = Mask::new(commands).style(Style::Stroke(stroke)).render();
        return CoverageBitmap::from_mask(data, placement).max_with(fill);
    }
    // Anisotropic pen: squeeze Y so the (rx, ry) ellipse becomes a circle
    // of radius rx, stroke there, then resample rows back. The squeeze is
    // capped — beyond 32:1 the ellipse is visually a horizontal slit.
    let squeeze = (rx / ry).clamp(1.0 / 32.0, 32.0);
    let squeezed: Vec<Command> = commands
        .iter()
        .map(|c| transform_y(*c, f64::from(squeeze)))
        .collect();
    if !raster_area_safe(&squeezed, f64::from(rx)) {
        return fill.clone();
    }
    let mut stroke = Stroke::new(2.0 * rx);
    stroke.cap(Cap::Round).join(Join::Round);
    let (data, placement) = Mask::new(&squeezed).style(Style::Stroke(stroke)).render();
    let squeezed_bitmap = CoverageBitmap::from_mask(data, placement);
    resample_rows_union(&squeezed_bitmap, f64::from(squeeze), fill)
}

fn transform_y(command: Command, sy: f64) -> Command {
    let map = |p: Vector| Vector::new(p.x, (f64::from(p.y) * sy) as f32);
    match command {
        Command::MoveTo(p) => Command::MoveTo(map(p)),
        Command::LineTo(p) => Command::LineTo(map(p)),
        Command::QuadTo(c, p) => Command::QuadTo(map(c), map(p)),
        Command::CurveTo(c1, c2, p) => Command::CurveTo(map(c1), map(c2), map(p)),
        Command::Close => Command::Close,
    }
}

/// The frame-row extent a Y-scaled bitmap (y' = y * sy) resamples to: the top
/// frame row and the row count. Pure function of `src.y`, `src.h`, `sy`, so it
/// may be recomputed cheaply anywhere the same value is needed.
fn resample_extent(src: &CoverageBitmap, sy: f64) -> (i32, usize) {
    let y0 = (f64::from(src.y) / sy).floor() as i32;
    let y1 = ((f64::from(src.y) + src.h as f64) / sy).ceil() as i32;
    (y0, (y1 - y0).max(1) as usize)
}

/// Box-resamples `src` (Y-scaled by `sy`) into `dst` (row stride `dst_w`),
/// placing the resampled block at (`off_x`, `off_y`). Rows whose coverage
/// window is empty are left untouched (callers zero-fill `dst`). Splitting the
/// core out lets both `resample_rows` and `resample_rows_union` share the exact
/// same per-pixel arithmetic.
fn resample_into(
    src: &CoverageBitmap,
    sy: f64,
    dst: &mut [u8],
    dst_w: usize,
    off_x: usize,
    off_y: usize,
) {
    let (y0, rh) = resample_extent(src, sy);
    for row in 0..rh {
        // Frame row `row` covers source rows [a, b) in the squeezed space.
        let a = (f64::from(y0 + row as i32)) * sy - f64::from(src.y);
        let b = a + sy;
        let (a0, b0) = (a.max(0.0), b.min(src.h as f64));
        if b0 <= a0 {
            continue;
        }
        let base = (off_y + row) * dst_w + off_x;
        for col in 0..src.w {
            let mut acc = 0.0f64;
            let mut sr = a0.floor() as usize;
            while (sr as f64) < b0 {
                let cover = (b0.min(sr as f64 + 1.0) - a0.max(sr as f64)).max(0.0);
                acc += cover * f64::from(src.data[sr * src.w + col]);
                sr += 1;
            }
            dst[base + col] = ((acc / (b0 - a0)) + 0.5) as u8;
        }
    }
}

/// Box-resamples a bitmap rendered in a Y-scaled space (y' = y * sy) back
/// to frame rows, preserving coverage.
fn resample_rows(src: &CoverageBitmap, sy: f64) -> CoverageBitmap {
    if src.is_empty() {
        return CoverageBitmap::default();
    }
    let (y0, h) = resample_extent(src, sy);
    let mut data = vec![0u8; src.w * h];
    resample_into(src, sy, &mut data, src.w, 0, 0);
    CoverageBitmap {
        w: src.w,
        h,
        x: src.x,
        y: y0,
        data,
    }
}

/// Fused resample-and-union for the anisotropic border: resamples `squeezed`
/// back to frame rows AND unions it with `fill` (per-pixel max) into a single
/// allocation. Byte-identical to `resample_rows(squeezed, sy).max_with(fill)`,
/// but writes the resampled rows straight into the union buffer instead of
/// materializing an intermediate bitmap and rescanning it.
fn resample_rows_union(
    squeezed: &CoverageBitmap,
    sy: f64,
    fill: &CoverageBitmap,
) -> CoverageBitmap {
    // Empty edges mirror `resample_rows(..).max_with(fill)` exactly.
    if squeezed.is_empty() {
        return fill.clone();
    }
    if fill.is_empty() {
        return resample_rows(squeezed, sy);
    }
    let (ry0, rh) = resample_extent(squeezed, sy);
    let rx = squeezed.x;
    let rw = squeezed.w as i32;
    // Union bounding box — identical to CoverageBitmap::max_with.
    let x0 = rx.min(fill.x);
    let y0 = ry0.min(fill.y);
    let x1 = (rx + rw).max(fill.x + fill.w as i32);
    let y1 = (ry0 + rh as i32).max(fill.y + fill.h as i32);
    let w = (x1 - x0) as usize;
    let h = (y1 - y0) as usize;
    let mut data = vec![0u8; w * h];
    // Resampled coverage straight into the union buffer at its offset (the zero
    // fill above stands in for max_with's first source pass over empty rows).
    resample_into(
        squeezed,
        sy,
        &mut data,
        w,
        (rx - x0) as usize,
        (ry0 - y0) as usize,
    );
    // Max the fill on top — mirrors max_with's second source pass.
    let fox = (fill.x - x0) as usize;
    let foy = (fill.y - y0) as usize;
    for row in 0..fill.h {
        let base = (foy + row) * w + fox;
        let dst_row = &mut data[base..base + fill.w];
        let src_row = &fill.data[row * fill.w..(row + 1) * fill.w];
        for (d, &s) in dst_row.iter_mut().zip(src_row) {
            *d = (*d).max(s);
        }
    }
    CoverageBitmap {
        w,
        h,
        x: x0,
        y: y0,
        data,
    }
}

/// libass `ass_fix_outline`: subtracts (half of) the glyph coverage from
/// the border coverage so translucent fills do not sit on border color.
pub(crate) fn fix_outline(glyph: &CoverageBitmap, outline: &mut CoverageBitmap) {
    if glyph.is_empty() || outline.is_empty() {
        return;
    }
    let l = outline.x.max(glyph.x);
    let t = outline.y.max(glyph.y);
    let r = (outline.x + outline.w as i32).min(glyph.x + glyph.w as i32);
    let b = (outline.y + outline.h as i32).min(glyph.y + glyph.h as i32);
    for y in t..b {
        for x in l..r {
            let g = glyph.data[(y - glyph.y) as usize * glyph.w + (x - glyph.x) as usize];
            let o =
                &mut outline.data[(y - outline.y) as usize * outline.w + (x - outline.x) as usize];
            *o = if *o > g { *o - g / 2 } else { 0 };
        }
    }
}

/// libass `ass_be_padding`: the fixed margin budget the \be blur may
/// spread into (the kernel itself clamps at edges and never grows).
fn be_padding(be: i32) -> usize {
    match be {
        b if b <= 3 => b.max(0) as usize,
        b if b <= 7 => 4,
        _ => 5,
    }
}

/// `\be`: the VSFilter "blur edges" effect, ported from libass
/// `ass_synth_blur`/`ass_be_blur_c`: the bitmap is padded ONCE by
/// `ass_be_padding(be)`, then blurred in place with a [1 2 1]²/16 kernel
/// whose borders repeat the edge samples; multi-pass runs are rescaled to
/// a 0..64 working range first (`be_blur_pre`/`be_blur_post`).
pub(crate) fn be_blur(bitmap: &mut CoverageBitmap, passes: i32) {
    let mut be = passes.clamp(0, 127);
    if be <= 0 || bitmap.is_empty() {
        return;
    }
    pad(bitmap, be_padding(be));
    let w = bitmap.w;
    let h = bitmap.h;
    if w < 2 || h < 2 {
        return;
    }
    // Scratch reused across every pass — allocated once here instead of three
    // Vecs per pass inside be_blur_pass. Each pass fully rewrites all three
    // before reading, so no stale state carries over.
    let mut col_pix = vec![0u16; w];
    let mut col_sum = vec![0u16; w];
    let mut horiz = vec![0u16; w];
    if be > 1 {
        // be_blur_pre: (v*64 + 127) / 255, kept in 8 bits.
        for v in bitmap.data.iter_mut() {
            *v = ((*v >> 1) + 1) >> 1;
        }
        while be > 1 {
            be_blur_pass(
                &mut bitmap.data,
                w,
                h,
                &mut col_pix,
                &mut col_sum,
                &mut horiz,
            );
            be -= 1;
        }
        // be_blur_post: (v*255 + 32) / 64 == (v << 2) - (v > 32).
        for v in bitmap.data.iter_mut() {
            *v = ((i32::from(*v) << 2) - i32::from(*v > 32)) as u8;
        }
    }
    be_blur_pass(
        &mut bitmap.data,
        w,
        h,
        &mut col_pix,
        &mut col_sum,
        &mut horiz,
    );
}

/// Horizontal [1 2 1] sums of one row with repeated edges — the inner
/// recurrence of libass `ass_be_blur_c`.
fn be_row_sums(src: &[u8], out: &mut [u16]) {
    let w = src.len();
    let mut old_pix = u16::from(src[0]);
    let mut old_sum = old_pix;
    for x in 1..w {
        let cur = u16::from(src[x]);
        let pair = old_pix + cur;
        old_pix = cur;
        out[x - 1] = old_sum + pair;
        old_sum = pair;
    }
    out[w - 1] = old_sum + old_pix;
}

/// One in-place VSFilter box-blur pass (libass `ass_be_blur_c`): separable
/// [1 2 1] in both axes, /16, edges repeated, output shifted to (x-1, y-1)
/// exactly like the reference implementation. `col_pix`/`col_sum`/`horiz` are
/// caller-owned scratch of length `w`, reused across passes.
fn be_blur_pass(
    buf: &mut [u8],
    w: usize,
    h: usize,
    col_pix: &mut [u16],
    col_sum: &mut [u16],
    horiz: &mut [u16],
) {
    // First row primes the column accumulators without output.
    be_row_sums(&buf[..w], horiz);
    col_pix.copy_from_slice(horiz);
    col_sum.copy_from_slice(horiz);

    for y in 1..h {
        // Source row y and destination row y-1 never overlap, and be_row_sums
        // lands row y fully into `horiz` before the destination is written, so
        // reading straight from `buf` (no per-row copy) is safe.
        be_row_sums(&buf[y * w..(y + 1) * w], horiz);
        let dst = &mut buf[(y - 1) * w..y * w];
        for x in 0..w {
            let vert = col_pix[x] + horiz[x];
            col_pix[x] = horiz[x];
            dst[x] = ((col_sum[x] + vert) >> 4) as u8;
            col_sum[x] = vert;
        }
    }
    // Final row: vertical tail repeats the last horizontal sums.
    let dst = &mut buf[(h - 1) * w..h * w];
    for x in 0..w {
        dst[x] = ((col_sum[x] + col_pix[x]) >> 4) as u8;
    }
}

/// `\blur`: gaussian with per-axis standard deviations, approximated by
/// three box-blur passes per axis (Kovesi construction). libass computes
/// sigma as `blur * blur_scale_axis * 2/sqrt(log(256))` and hands
/// `ass_gaussian_blur` the squared values; callers pass the sigmas here.
pub(crate) fn gaussian_blur(bitmap: &mut CoverageBitmap, sigma_x: f64, sigma_y: f64) {
    if bitmap.is_empty() || (sigma_x <= 0.0 && sigma_y <= 0.0) {
        return;
    }
    let boxes_x = boxes_for_gauss(sigma_x.max(0.0));
    let boxes_y = boxes_for_gauss(sigma_y.max(0.0));
    pad_xy(
        bitmap,
        (sigma_x.max(0.0).ceil() as usize + 1) * 2,
        (sigma_y.max(0.0).ceil() as usize + 1) * 2,
    );
    let mut tmp = bitmap.data.clone();
    let w = bitmap.w;
    let h = bitmap.h;
    // Per-column accumulators for box_blur_v, reused across the three passes.
    let mut col_acc = vec![0u32; w];
    for (rx, ry) in boxes_x.into_iter().zip(boxes_y) {
        if rx > 0 {
            box_blur_h(&bitmap.data, &mut tmp, w, h, rx);
        } else {
            tmp.copy_from_slice(&bitmap.data);
        }
        if ry > 0 {
            box_blur_v(&tmp, &mut bitmap.data, w, h, ry, &mut col_acc);
        } else {
            bitmap.data.copy_from_slice(&tmp);
        }
    }
}

/// Kovesi's three-box approximation of a gaussian: the per-pass box radii.
/// `n` is fixed at 3 (the pass count), so the result is a fixed-size array.
fn boxes_for_gauss(sigma: f64) -> [usize; 3] {
    const N: f64 = 3.0;
    let w_ideal = (12.0 * sigma * sigma / N + 1.0).sqrt();
    let mut wl = w_ideal.floor() as i64;
    if wl % 2 == 0 {
        wl -= 1;
    }
    let wu = wl + 2;
    let m_ideal =
        (12.0 * sigma * sigma - N * (wl as f64) * (wl as f64) - 4.0 * N * wl as f64 - 3.0 * N)
            / (-4.0 * wl as f64 - 4.0);
    let m = m_ideal.round() as i64;
    let mut boxes = [0usize; 3];
    for (i, b) in boxes.iter_mut().enumerate() {
        let width = if (i as i64) < m { wl } else { wu };
        *b = (width.max(1) as usize - 1) / 2;
    }
    boxes
}

fn box_blur_h(src: &[u8], dst: &mut [u8], w: usize, h: usize, r: usize) {
    let norm = (2 * r + 1) as u32;
    for y in 0..h {
        let row = &src[y * w..(y + 1) * w];
        let mut acc: u32 = 0;
        for &v in &row[..=r.min(w.saturating_sub(1))] {
            acc += u32::from(v);
        }
        for x in 0..w {
            dst[y * w + x] = (acc / norm) as u8;
            if x + r + 1 < w {
                acc += u32::from(row[x + r + 1]);
            }
            if x >= r {
                acc -= u32::from(row[x - r]);
            }
        }
    }
}

/// Vertical box blur, walked row-major so every memory access is contiguous.
/// `acc` is caller-owned per-column scratch of length `w`, reset here. The
/// per-column arithmetic (u32 sliding window, /norm truncating, u8 cast) is
/// identical to a per-column walk — only the traversal order changes.
fn box_blur_v(src: &[u8], dst: &mut [u8], w: usize, h: usize, r: usize, acc: &mut [u32]) {
    let norm = (2 * r + 1) as u32;
    acc.fill(0);
    // Prime each column with its initial window [0, min(r, h-1)].
    let last = r.min(h.saturating_sub(1));
    for yy in 0..=last {
        for (a, &s) in acc.iter_mut().zip(&src[yy * w..(yy + 1) * w]) {
            *a += u32::from(s);
        }
    }
    for y in 0..h {
        for (d, &a) in dst[y * w..(y + 1) * w].iter_mut().zip(acc.iter()) {
            *d = (a / norm) as u8;
        }
        if y + r + 1 < h {
            for (a, &s) in acc.iter_mut().zip(&src[(y + r + 1) * w..(y + r + 2) * w]) {
                *a += u32::from(s);
            }
        }
        if y >= r {
            for (a, &s) in acc.iter_mut().zip(&src[(y - r) * w..(y - r + 1) * w]) {
                *a -= u32::from(s);
            }
        }
    }
}

/// Grows the bitmap by `margin` pixels on every side (blur needs room).
fn pad(bitmap: &mut CoverageBitmap, margin: usize) {
    pad_xy(bitmap, margin, margin);
}

/// Grows the bitmap by independent horizontal/vertical margins.
fn pad_xy(bitmap: &mut CoverageBitmap, margin_x: usize, margin_y: usize) {
    if margin_x == 0 && margin_y == 0 {
        return;
    }
    let w = bitmap.w + margin_x * 2;
    let h = bitmap.h + margin_y * 2;
    let mut data = vec![0u8; w * h];
    for row in 0..bitmap.h {
        let dst = (row + margin_y) * w + margin_x;
        data[dst..dst + bitmap.w]
            .copy_from_slice(&bitmap.data[row * bitmap.w..(row + 1) * bitmap.w]);
    }
    bitmap.w = w;
    bitmap.h = h;
    bitmap.x -= margin_x as i32;
    bitmap.y -= margin_y as i32;
    bitmap.data = data;
}

/// Builds zeno commands from a ttf-parser glyph outline. The transform
/// maps font units to frame pixels: scale, then flip Y (fonts are Y-up),
/// then offset; an optional shear implements `\fax`/`\fay` later.
pub(crate) struct OutlinePath {
    pub commands: Vec<Command>,
    scale_x: f32,
    scale_y: f32,
    dx: f32,
    dy: f32,
}

impl OutlinePath {
    pub(crate) fn new(scale_x: f32, scale_y: f32, dx: f32, dy: f32) -> Self {
        Self {
            commands: Vec::new(),
            scale_x,
            scale_y,
            dx,
            dy,
        }
    }

    fn point(&self, x: f32, y: f32) -> Vector {
        Vector::new(self.dx + x * self.scale_x, self.dy - y * self.scale_y)
    }
}

impl ttf_parser::OutlineBuilder for OutlinePath {
    fn move_to(&mut self, x: f32, y: f32) {
        let p = self.point(x, y);
        self.commands.push(Command::MoveTo(p));
    }

    fn line_to(&mut self, x: f32, y: f32) {
        let p = self.point(x, y);
        self.commands.push(Command::LineTo(p));
    }

    fn quad_to(&mut self, x1: f32, y1: f32, x: f32, y: f32) {
        let c = self.point(x1, y1);
        let p = self.point(x, y);
        self.commands.push(Command::QuadTo(c, p));
    }

    fn curve_to(&mut self, x1: f32, y1: f32, x2: f32, y2: f32, x: f32, y: f32) {
        let c1 = self.point(x1, y1);
        let c2 = self.point(x2, y2);
        let p = self.point(x, y);
        self.commands.push(Command::CurveTo(c1, c2, p));
    }

    fn close(&mut self) {
        self.commands.push(Command::Close);
    }
}

/// Converts a parsed ASS drawing (26.6 fixed point, Y-down already) into
/// zeno commands. `scale` divides by `2^(p-1)` per the `\p` scale level,
/// times the screen scale.
pub(crate) fn drawing_commands(
    drawing: &crate::subtitle::ass::Drawing,
    scale_x: f32,
    scale_y: f32,
    dx: f32,
    dy: f32,
) -> Vec<Command> {
    use crate::subtitle::ass::{DrawCmd, Point6};
    let map = |p: Point6| -> Vector {
        Vector::new(
            dx + (p.x as f32 / 64.0) * scale_x,
            dy + (p.y as f32 / 64.0) * scale_y,
        )
    };
    drawing
        .cmds
        .iter()
        .map(|cmd| match *cmd {
            DrawCmd::Move(p) => Command::MoveTo(map(p)),
            DrawCmd::Line(p) => Command::LineTo(map(p)),
            DrawCmd::Cubic(c1, c2, p) => Command::CurveTo(map(c1), map(c2), map(p)),
            DrawCmd::Close => Command::Close,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square(side: f32) -> Vec<Command> {
        vec![
            Command::MoveTo(Vector::new(0.0, 0.0)),
            Command::LineTo(Vector::new(side, 0.0)),
            Command::LineTo(Vector::new(side, side)),
            Command::LineTo(Vector::new(0.0, side)),
            Command::Close,
        ]
    }

    #[test]
    fn fill_produces_opaque_interior() {
        let bitmap = fill_path(&square(20.0));
        assert!(!bitmap.is_empty());
        // Sample the center: fully covered.
        let cx = bitmap.w / 2;
        let cy = bitmap.h / 2;
        assert_eq!(bitmap.data[cy * bitmap.w + cx], 255);
    }

    #[test]
    fn border_extends_beyond_fill() {
        let fill = fill_path(&square(20.0));
        let border = border_path(&square(20.0), &fill, 3.0, 3.0);
        assert!(border.w >= fill.w + 4, "stroke must widen the bitmap");
        assert!(border.x <= fill.x - 2);
        // Border contains the fill (union semantics).
        let cx = border.w / 2;
        let cy = border.h / 2;
        assert_eq!(border.data[cy * border.w + cx], 255);
    }

    /// A path within the layout guard's frame-relative bound can still
    /// dwarf [`MAX_MASK_AREA_PX`] on 8K-class frames (61440^2 ≈ 3.8 GiB of
    /// alpha, and the stroked (61440 + 2*7680)^2 even passes zeno's `2^32`
    /// u32 sizing) — the rasterizer must degrade (empty mask / fill-only
    /// border), never allocate gigabytes or overflow.
    #[test]
    fn oversized_paths_degrade_instead_of_overflowing_mask_sizing() {
        assert!(fill_path(&square(70_000.0)).is_empty());
        // Passes the old u32-overflow wing (3.8e9 < 2^32) but not the
        // absolute area cap; before the cap this was a ~3.8 GiB allocation.
        assert!(fill_path(&square(61_440.0)).is_empty());
        // Positive control: a full-8K-frame-sized square (59M px) renders.
        assert!(!fill_path(&square(7_680.0)).is_empty());

        let fill = fill_path(&square(20.0));
        let bordered = border_path(&square(61_440.0), &fill, 7_680.0, 7_680.0);
        assert_eq!(bordered, fill, "unsafe stroke must fall back to the fill");
        // The anisotropic pen route has its own mask sizing on the squeezed
        // path; it must degrade the same way.
        let aniso = border_path(&square(61_440.0), &fill, 7_680.0, 240.0);
        assert_eq!(aniso, fill, "unsafe aniso stroke must fall back to the fill");
    }

    #[test]
    fn asymmetric_border_extends_per_axis() {
        // \xbord8\ybord1: the ring must be ~8px wider horizontally but only
        // ~1px taller vertically (libass strokes a true ellipse).
        let fill = fill_path(&square(20.0));
        let border = border_path(&square(20.0), &fill, 8.0, 1.0);
        let grow_x = (fill.x - border.x).min(border.x + border.w as i32 - fill.x - fill.w as i32);
        let grow_y = (fill.y - border.y).min(border.y + border.h as i32 - fill.y - fill.h as i32);
        assert!(grow_x >= 7, "horizontal growth {grow_x} must be ~8px");
        assert!(grow_y <= 3, "vertical growth {grow_y} must be ~1px");
        // Still contains the fill.
        let cx = (fill.x - border.x) as usize + fill.w / 2;
        let cy = (fill.y - border.y) as usize + fill.h / 2;
        assert_eq!(border.data[cy * border.w + cx], 255);
    }

    #[test]
    fn be_blur_pads_once_by_libass_budget() {
        // ass_be_padding: be for be<=3, 4 for be<=7, capped at 5 beyond.
        let mut bitmap = fill_path(&square(8.0));
        let before_w = bitmap.w;
        be_blur(&mut bitmap, 2);
        assert_eq!(bitmap.w, before_w + 4, "be=2 pads by 2 per side");
        assert!(bitmap.data.iter().any(|&v| v > 0 && v < 255));

        let mut big = fill_path(&square(8.0));
        let before_w = big.w;
        be_blur(&mut big, 20);
        assert_eq!(big.w, before_w + 10, "be=20 pads by the 5px cap");
    }

    #[test]
    fn fix_outline_hollows_border_under_fill() {
        let fill = fill_path(&square(20.0));
        let mut border = border_path(&square(20.0), &fill, 3.0, 3.0);
        fix_outline(&fill, &mut border);
        // Center (opaque fill): o > g never holds, so coverage drops to 0.
        let cx = (fill.x - border.x) as usize + fill.w / 2;
        let cy = (fill.y - border.y) as usize + fill.h / 2;
        assert_eq!(border.data[cy * border.w + cx], 0);
        // The outer ring is untouched.
        assert_eq!(
            border.data[border.w / 2],
            border_path(&square(20.0), &fill, 3.0, 3.0).data[border.w / 2]
        );
    }

    #[test]
    fn gaussian_blur_preserves_total_energy_roughly() {
        let mut bitmap = fill_path(&square(10.0));
        let sum_before: u64 = bitmap.data.iter().map(|&v| u64::from(v)).sum();
        gaussian_blur(&mut bitmap, 2.0, 2.0);
        let sum_after: u64 = bitmap.data.iter().map(|&v| u64::from(v)).sum();
        let ratio = sum_after as f64 / sum_before as f64;
        assert!((0.85..=1.15).contains(&ratio), "energy ratio {ratio}");
    }

    #[test]
    fn clip_rect_zeroes_outside() {
        let mut bitmap = fill_path(&square(10.0));
        let (bx, by) = (bitmap.x, bitmap.y);
        bitmap.clip_rect(bx, by, bx + 3, by + 3, false);
        assert_eq!(bitmap.data[0], 255, "inside the clip");
        assert_eq!(bitmap.data[5 * bitmap.w + 5], 0, "outside the clip");

        let mut inv = fill_path(&square(10.0));
        inv.clip_rect(bx, by, bx + 3, by + 3, true);
        assert_eq!(inv.data[0], 0, "inverse clip zeroes the inside");
        assert_eq!(inv.data[5 * inv.w + 5], 255);
    }

    #[test]
    fn max_with_unions_disjoint_bitmaps() {
        let a = CoverageBitmap {
            w: 2,
            h: 1,
            x: 0,
            y: 0,
            data: vec![10, 20],
        };
        let b = CoverageBitmap {
            w: 1,
            h: 1,
            x: 3,
            y: 0,
            data: vec![30],
        };
        let union = a.max_with(&b);
        assert_eq!((union.w, union.h, union.x, union.y), (4, 1, 0, 0));
        assert_eq!(union.data, vec![10, 20, 0, 30]);
    }

    // ---- Parity harness for the optimized blur/border kernels ----------------
    //
    // The optimized kernels must be byte-identical to the pre-optimization code
    // (libass parity is contractual). Two independent oracles guard this:
    //   1. `golden_hashes_match`: FNV-1a hashes captured from the ORIGINAL
    //      production kernels (regenerate with the ignored `dump_goldens`).
    //   2. `*_matches_reference`: direct comparison against verbatim copies of
    //      the original kernels (`reference_*`) over a deterministic mask set.

    fn fnv1a(bytes: &[u8]) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in bytes {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h
    }

    /// Folds geometry (w/h/x/y) and coverage into one hash so a placement drift
    /// is caught, not just data drift.
    fn bitmap_hash(b: &CoverageBitmap) -> u64 {
        let mut bytes = Vec::with_capacity(32 + b.data.len());
        bytes.extend_from_slice(&(b.w as u64).to_le_bytes());
        bytes.extend_from_slice(&(b.h as u64).to_le_bytes());
        bytes.extend_from_slice(&b.x.to_le_bytes());
        bytes.extend_from_slice(&b.y.to_le_bytes());
        bytes.extend_from_slice(&b.data);
        fnv1a(&bytes)
    }

    fn det_mask(w: usize, h: usize, f: impl Fn(usize, usize) -> u8) -> CoverageBitmap {
        let mut data = vec![0u8; w * h];
        for (i, px) in data.iter_mut().enumerate() {
            *px = f(i % w, i / w);
        }
        // Non-zero placement so x/y bookkeeping is exercised through pad/resample.
        CoverageBitmap {
            w,
            h,
            x: 5,
            y: 7,
            data,
        }
    }

    /// The representative mask set: three sizes on a non-trivial coverage
    /// pattern, plus an all-opaque and a sparse mask.
    fn masks() -> Vec<(&'static str, CoverageBitmap)> {
        let pat = |x: usize, y: usize| ((x * 31 + y * 17) % 256) as u8;
        vec![
            ("16x32", det_mask(16, 32, pat)),
            ("128x24", det_mask(128, 24, pat)),
            ("24x128", det_mask(24, 128, pat)),
            ("all255_32x32", det_mask(32, 32, |_, _| 255)),
            (
                "sparse_40x40",
                det_mask(
                    40,
                    40,
                    |x, y| if (x * 7 + y * 3) % 29 == 0 { 180 } else { 0 },
                ),
            ),
        ]
    }

    /// The representative set plus two large masks that exceed cache — the
    /// regime where box_blur_v's strided-vs-row-major traversal actually shows
    /// up (small masks fit in cache after padding and are traversal-neutral).
    fn bench_masks() -> Vec<(&'static str, CoverageBitmap)> {
        let pat = |x: usize, y: usize| ((x * 31 + y * 17) % 256) as u8;
        let mut v = masks();
        v.push(("512x512", det_mask(512, 512, pat)));
        v.push(("1024x256", det_mask(1024, 256, pat)));
        v
    }

    /// A closed glyph-like contour (line + quad + cubic) for the border path.
    fn glyph_like() -> Vec<Command> {
        vec![
            Command::MoveTo(Vector::new(2.0, 2.0)),
            Command::LineTo(Vector::new(18.0, 4.0)),
            Command::QuadTo(Vector::new(24.0, 10.0), Vector::new(20.0, 22.0)),
            Command::LineTo(Vector::new(6.0, 18.0)),
            Command::CurveTo(
                Vector::new(3.0, 14.0),
                Vector::new(1.0, 9.0),
                Vector::new(2.0, 2.0),
            ),
            Command::Close,
        ]
    }

    const BE_PASSES: [i32; 4] = [1, 2, 5, 20];
    const GAUSS_SIGMAS: [(f64, f64); 4] = [(0.5, 0.5), (2.0, 2.0), (5.0, 1.0), (0.0, 3.0)];
    const BORDER_ANISO: [(f32, f32); 2] = [(3.0, 1.5), (1.0, 4.0)];

    /// Regenerate the `golden_hashes_match` constants: run with
    /// `cargo test --release --features subtitle dump_goldens -- --ignored --nocapture`.
    #[test]
    #[ignore = "prints golden hashes for the parity constants"]
    fn dump_goldens() {
        for (name, mask) in masks() {
            for passes in BE_PASSES {
                let mut b = mask.clone();
                be_blur(&mut b, passes);
                println!(
                    "be_blur   {name:<14} {passes:<3} => {:#018x}",
                    bitmap_hash(&b)
                );
            }
        }
        for (name, mask) in masks() {
            for (sx, sy) in GAUSS_SIGMAS {
                let mut b = mask.clone();
                gaussian_blur(&mut b, sx, sy);
                println!(
                    "gaussian  {name:<14} {sx},{sy} => {:#018x}",
                    bitmap_hash(&b)
                );
            }
        }
        let fill = fill_path(&glyph_like());
        for (bx, by) in BORDER_ANISO {
            let b = border_path(&glyph_like(), &fill, bx, by);
            println!("border    {bx},{by} => {:#018x}", bitmap_hash(&b));
        }
    }

    // Hashes captured from the ORIGINAL kernels (pre-optimization). Any change
    // here means the optimized output diverged — regenerate only with a
    // deliberate, reviewed semantics change. Order matches the loops below.
    #[rustfmt::skip]
    const BE_GOLDENS: [u64; 20] = [
        0xd803_8531_e1bf_58f1, 0x6b9b_baf9_cba0_cd9c, 0xa4c8_97f5_4ae6_d86b, 0xe2a2_b89d_364f_5ee5,
        0xbf02_2691_b2a6_8151, 0x4b5a_e950_a4dd_10c9, 0x4e22_f94f_7eb4_573a, 0x3b77_5636_6cd3_ebde,
        0xc192_95e4_b0d9_b6a5, 0x8bfa_1295_4771_4068, 0x4aae_83be_92e2_3f4f, 0xf177_4dc9_d811_4556,
        0xc08f_1b96_b3fa_04b3, 0x61fd_6cd3_7e0f_3a9f, 0x4575_4522_d745_3a67, 0x54b2_a781_61fe_55e3,
        0x1198_c1c5_06b9_3135, 0x84de_fae0_6796_87b9, 0x9b0e_fed0_2429_7baf, 0xa7ed_8ca1_ef98_e917,
    ];
    #[rustfmt::skip]
    const GAUSS_GOLDENS: [u64; 20] = [
        0x83b1_cce7_4655_6c87, 0xabf2_d2c0_f06b_721a, 0xd9e4_7912_af7d_6728, 0x6ade_2182_5cee_d5ee,
        0x726d_3a16_e645_a43f, 0x8676_eb28_98d4_3dd2, 0x3bb4_da75_45dd_754f, 0xde06_8d27_3355_6dc2,
        0x4b75_aff0_f679_8cff, 0xaf8d_4273_9e70_be94, 0xe4ee_eb46_9c16_3bbb, 0xf7c9_85cb_c5d4_3ce5,
        0xb46c_035f_4007_9657, 0x771a_5b64_408b_2214, 0x5507_dd2e_c82d_6050, 0x38fe_e6e7_6247_3c8e,
        0xeaf2_a6d6_566b_745f, 0x136f_8e4d_1fdc_67dc, 0x8388_a4b7_367c_44ac, 0x6ec0_d23c_5ee6_091c,
    ];
    const BORDER_GOLDENS: [u64; 2] = [0xff22_7254_d918_c628, 0x1dce_d946_2715_012a];

    #[test]
    fn golden_hashes_match() {
        let mut i = 0;
        for (name, mask) in masks() {
            for passes in BE_PASSES {
                let mut b = mask.clone();
                be_blur(&mut b, passes);
                assert_eq!(
                    bitmap_hash(&b),
                    BE_GOLDENS[i],
                    "be_blur {name} passes={passes} diverged from golden"
                );
                i += 1;
            }
        }
        let mut j = 0;
        for (name, mask) in masks() {
            for (sx, sy) in GAUSS_SIGMAS {
                let mut b = mask.clone();
                gaussian_blur(&mut b, sx, sy);
                assert_eq!(
                    bitmap_hash(&b),
                    GAUSS_GOLDENS[j],
                    "gaussian_blur {name} sigma=({sx},{sy}) diverged from golden"
                );
                j += 1;
            }
        }
        let fill = fill_path(&glyph_like());
        for (k, (bx, by)) in BORDER_ANISO.into_iter().enumerate() {
            let b = border_path(&glyph_like(), &fill, bx, by);
            assert_eq!(
                bitmap_hash(&b),
                BORDER_GOLDENS[k],
                "border_path aniso ({bx},{by}) diverged from golden"
            );
        }
    }

    fn assert_bitmap_eq(a: &CoverageBitmap, b: &CoverageBitmap, ctx: &str) {
        assert_eq!(
            (a.w, a.h, a.x, a.y),
            (b.w, b.h, b.x, b.y),
            "{ctx}: geometry"
        );
        assert_eq!(a.data, b.data, "{ctx}: coverage data");
    }

    // ---- Verbatim copies of the pre-optimization kernels (parity oracles) ----
    // Do NOT "tidy" these: they must reproduce the original arithmetic exactly.
    // They reuse the unchanged helpers (pad, be_row_sums, box_blur_h, transform_y,
    // max_with) so only the optimized code paths differ from production.

    fn reference_be_blur_pass(buf: &mut [u8], w: usize, h: usize) {
        let mut col_pix = vec![0u16; w];
        let mut col_sum = vec![0u16; w];
        let mut horiz = vec![0u16; w];

        be_row_sums(&buf[..w], &mut horiz);
        col_pix.copy_from_slice(&horiz);
        col_sum.copy_from_slice(&horiz);

        for y in 1..h {
            let src = buf[y * w..(y + 1) * w].to_vec();
            be_row_sums(&src, &mut horiz);
            let dst = &mut buf[(y - 1) * w..y * w];
            for x in 0..w {
                let vert = col_pix[x] + horiz[x];
                col_pix[x] = horiz[x];
                dst[x] = ((col_sum[x] + vert) >> 4) as u8;
                col_sum[x] = vert;
            }
        }
        let dst = &mut buf[(h - 1) * w..h * w];
        for x in 0..w {
            dst[x] = ((col_sum[x] + col_pix[x]) >> 4) as u8;
        }
    }

    fn reference_be_blur(bitmap: &mut CoverageBitmap, passes: i32) {
        let mut be = passes.clamp(0, 127);
        if be <= 0 || bitmap.is_empty() {
            return;
        }
        pad(bitmap, be_padding(be));
        let w = bitmap.w;
        let h = bitmap.h;
        if w < 2 || h < 2 {
            return;
        }
        if be > 1 {
            for v in bitmap.data.iter_mut() {
                *v = ((*v >> 1) + 1) >> 1;
            }
            while be > 1 {
                reference_be_blur_pass(&mut bitmap.data, w, h);
                be -= 1;
            }
            for v in bitmap.data.iter_mut() {
                *v = ((i32::from(*v) << 2) - i32::from(*v > 32)) as u8;
            }
        }
        reference_be_blur_pass(&mut bitmap.data, w, h);
    }

    fn reference_boxes_for_gauss(sigma: f64, n: usize) -> Vec<usize> {
        let w_ideal = (12.0 * sigma * sigma / n as f64 + 1.0).sqrt();
        let mut wl = w_ideal.floor() as i64;
        if wl % 2 == 0 {
            wl -= 1;
        }
        let wu = wl + 2;
        let m_ideal = (12.0 * sigma * sigma
            - (n as f64) * (wl as f64) * (wl as f64)
            - 4.0 * n as f64 * wl as f64
            - 3.0 * n as f64)
            / (-4.0 * wl as f64 - 4.0);
        let m = m_ideal.round() as i64;
        (0..n as i64)
            .map(|i| {
                let width = if i < m { wl } else { wu };
                (width.max(1) as usize - 1) / 2
            })
            .collect()
    }

    fn reference_box_blur_v(src: &[u8], dst: &mut [u8], w: usize, h: usize, r: usize) {
        let norm = (2 * r + 1) as u32;
        for x in 0..w {
            let mut acc: u32 = 0;
            for y in 0..=r.min(h.saturating_sub(1)) {
                acc += u32::from(src[y * w + x]);
            }
            for y in 0..h {
                dst[y * w + x] = (acc / norm) as u8;
                if y + r + 1 < h {
                    acc += u32::from(src[(y + r + 1) * w + x]);
                }
                if y >= r {
                    acc -= u32::from(src[(y - r) * w + x]);
                }
            }
        }
    }

    fn reference_gaussian_blur(bitmap: &mut CoverageBitmap, sigma_x: f64, sigma_y: f64) {
        if bitmap.is_empty() || (sigma_x <= 0.0 && sigma_y <= 0.0) {
            return;
        }
        let boxes_x = reference_boxes_for_gauss(sigma_x.max(0.0), 3);
        let boxes_y = reference_boxes_for_gauss(sigma_y.max(0.0), 3);
        pad_xy(
            bitmap,
            (sigma_x.max(0.0).ceil() as usize + 1) * 2,
            (sigma_y.max(0.0).ceil() as usize + 1) * 2,
        );
        let mut tmp = bitmap.data.clone();
        let w = bitmap.w;
        let h = bitmap.h;
        for (rx, ry) in boxes_x.into_iter().zip(boxes_y) {
            if rx > 0 {
                box_blur_h(&bitmap.data, &mut tmp, w, h, rx);
            } else {
                tmp.copy_from_slice(&bitmap.data);
            }
            if ry > 0 {
                reference_box_blur_v(&tmp, &mut bitmap.data, w, h, ry);
            } else {
                bitmap.data.copy_from_slice(&tmp);
            }
        }
    }

    fn reference_resample_rows(src: &CoverageBitmap, sy: f64) -> CoverageBitmap {
        if src.is_empty() {
            return CoverageBitmap::default();
        }
        let y0 = (f64::from(src.y) / sy).floor() as i32;
        let y1 = ((f64::from(src.y) + src.h as f64) / sy).ceil() as i32;
        let h = (y1 - y0).max(1) as usize;
        let mut data = vec![0u8; src.w * h];
        for row in 0..h {
            let a = (f64::from(y0 + row as i32)) * sy - f64::from(src.y);
            let b = a + sy;
            let (a0, b0) = (a.max(0.0), b.min(src.h as f64));
            if b0 <= a0 {
                continue;
            }
            for col in 0..src.w {
                let mut acc = 0.0f64;
                let mut sr = a0.floor() as usize;
                while (sr as f64) < b0 {
                    let cover = (b0.min(sr as f64 + 1.0) - a0.max(sr as f64)).max(0.0);
                    acc += cover * f64::from(src.data[sr * src.w + col]);
                    sr += 1;
                }
                data[row * src.w + col] = ((acc / (b0 - a0)) + 0.5) as u8;
            }
        }
        CoverageBitmap {
            w: src.w,
            h,
            x: src.x,
            y: y0,
            data,
        }
    }

    /// Reproduces the original anisotropic border path (resample then max_with).
    fn reference_border_path_aniso(
        commands: &[Command],
        fill: &CoverageBitmap,
        border_x: f32,
        border_y: f32,
    ) -> CoverageBitmap {
        let rx = border_x.max(1.0 / 64.0);
        let ry = border_y.max(1.0 / 64.0);
        let squeeze = (rx / ry).clamp(1.0 / 32.0, 32.0);
        let squeezed: Vec<Command> = commands
            .iter()
            .map(|c| transform_y(*c, f64::from(squeeze)))
            .collect();
        let mut stroke = Stroke::new(2.0 * rx);
        stroke.cap(Cap::Round).join(Join::Round);
        let (data, placement) = Mask::new(&squeezed).style(Style::Stroke(stroke)).render();
        let squeezed_bitmap = CoverageBitmap::from_mask(data, placement);
        reference_resample_rows(&squeezed_bitmap, f64::from(squeeze)).max_with(fill)
    }

    #[test]
    fn be_blur_matches_reference() {
        for (name, mask) in masks() {
            for passes in BE_PASSES {
                let mut new = mask.clone();
                let mut old = mask.clone();
                be_blur(&mut new, passes);
                reference_be_blur(&mut old, passes);
                assert_bitmap_eq(&new, &old, &format!("be_blur {name} passes={passes}"));
            }
        }
    }

    #[test]
    fn gaussian_blur_matches_reference() {
        for (name, mask) in masks() {
            for (sx, sy) in GAUSS_SIGMAS {
                let mut new = mask.clone();
                let mut old = mask.clone();
                gaussian_blur(&mut new, sx, sy);
                reference_gaussian_blur(&mut old, sx, sy);
                assert_bitmap_eq(&new, &old, &format!("gaussian {name} ({sx},{sy})"));
            }
        }
    }

    #[test]
    fn box_blur_v_matches_reference() {
        for (name, mask) in masks() {
            let (w, h) = (mask.w, mask.h);
            // r=0 is never routed to box_blur_v (gaussian copies instead); r>=h
            // exercises the edge-clamp path.
            for r in [1usize, 2, 3, 5, 9, 50] {
                let mut new_dst = vec![0u8; w * h];
                let mut old_dst = vec![0u8; w * h];
                let mut acc = vec![0u32; w];
                box_blur_v(&mask.data, &mut new_dst, w, h, r, &mut acc);
                reference_box_blur_v(&mask.data, &mut old_dst, w, h, r);
                assert_eq!(new_dst, old_dst, "box_blur_v {name} r={r}");
            }
        }
    }

    #[test]
    fn boxes_for_gauss_matches_reference() {
        for sigma in [0.0, 0.3, 0.5, 1.0, 2.0, 3.7, 5.0, 12.5, 40.0] {
            let new = boxes_for_gauss(sigma);
            let old = reference_boxes_for_gauss(sigma, 3);
            assert_eq!(new.to_vec(), old, "boxes_for_gauss sigma={sigma}");
        }
    }

    #[test]
    fn resample_rows_matches_reference() {
        // Squeezed bitmaps from real stroke renders, both squeeze directions.
        for (bx, by) in BORDER_ANISO {
            let rx = bx.max(1.0 / 64.0);
            let ry = by.max(1.0 / 64.0);
            let squeeze = (rx / ry).clamp(1.0 / 32.0, 32.0);
            let squeezed: Vec<Command> = glyph_like()
                .iter()
                .map(|c| transform_y(*c, f64::from(squeeze)))
                .collect();
            let mut stroke = Stroke::new(2.0 * rx);
            stroke.cap(Cap::Round).join(Join::Round);
            let (data, placement) = Mask::new(&squeezed).style(Style::Stroke(stroke)).render();
            let sb = CoverageBitmap::from_mask(data, placement);
            let new = resample_rows(&sb, f64::from(squeeze));
            let old = reference_resample_rows(&sb, f64::from(squeeze));
            assert_bitmap_eq(&new, &old, &format!("resample_rows squeeze={squeeze}"));
        }
    }

    #[test]
    fn border_path_aniso_matches_reference() {
        let fill = fill_path(&glyph_like());
        for (bx, by) in BORDER_ANISO {
            let new = border_path(&glyph_like(), &fill, bx, by);
            let old = reference_border_path_aniso(&glyph_like(), &fill, bx, by);
            assert_bitmap_eq(&new, &old, &format!("border_path aniso ({bx},{by})"));
        }
        // Empty-fill edge: fused path must fall back to the bare resample
        // (mirrors R.max_with(empty) == R).
        let empty = CoverageBitmap::default();
        for (bx, by) in BORDER_ANISO {
            let new = border_path(&glyph_like(), &empty, bx, by);
            let old = reference_border_path_aniso(&glyph_like(), &empty, bx, by);
            assert_bitmap_eq(
                &new,
                &old,
                &format!("border_path aniso empty-fill ({bx},{by})"),
            );
        }
        // Empty-squeezed edge: union with an empty resample == fill.clone().
        let sb_empty = CoverageBitmap::default();
        assert_bitmap_eq(
            &resample_rows_union(&sb_empty, 2.0, &fill),
            &fill,
            "resample_rows_union empty-squeezed",
        );
    }

    /// Best per-call nanoseconds over three batches (mirrors bench_kernels::measure).
    fn bench_measure(mut run: impl FnMut()) -> f64 {
        run(); // warmup / page-in
        let start = std::time::Instant::now();
        run();
        let once = start.elapsed().as_nanos().max(1);
        let iters = (50_000_000 / once).clamp(20, 10_000) as usize;
        let mut best = f64::INFINITY;
        for _ in 0..3 {
            let start = std::time::Instant::now();
            for _ in 0..iters {
                run();
            }
            best = best.min(start.elapsed().as_nanos() as f64 / iters as f64);
        }
        best
    }

    /// `cargo test --release --features subtitle bench_be_blur -- --ignored --nocapture`.
    /// Numbers are clone-subtracted (each call clones the input mask first, since
    /// be_blur mutates and pads in place); the reported figure is kernel-only.
    #[test]
    #[ignore = "manual micro-benchmark; run in release with --ignored --nocapture"]
    fn bench_be_blur() {
        println!("be_blur kernel ns/call (best-of-3, clone-subtracted):");
        for (name, mask) in bench_masks() {
            let clone_ns = bench_measure(|| {
                let b = mask.clone();
                std::hint::black_box(b.data.first().copied());
            });
            for passes in [2, 5, 20] {
                let old = bench_measure(|| {
                    let mut b = mask.clone();
                    reference_be_blur(&mut b, passes);
                    std::hint::black_box(b.data.first().copied());
                });
                let new = bench_measure(|| {
                    let mut b = mask.clone();
                    be_blur(&mut b, passes);
                    std::hint::black_box(b.data.first().copied());
                });
                let (o, n) = ((old - clone_ns).max(0.0), (new - clone_ns).max(0.0));
                println!(
                    "  {name:<14} passes={passes:<2}  old {o:>9.0}  new {n:>9.0}  {:>4.2}x",
                    o / n.max(1.0)
                );
            }
        }
    }

    /// `cargo test --release --features subtitle bench_gaussian_blur -- --ignored --nocapture`.
    #[test]
    #[ignore = "manual micro-benchmark; run in release with --ignored --nocapture"]
    fn bench_gaussian_blur() {
        println!("gaussian_blur kernel ns/call (best-of-3, clone-subtracted):");
        for (name, mask) in bench_masks() {
            let clone_ns = bench_measure(|| {
                let b = mask.clone();
                std::hint::black_box(b.data.first().copied());
            });
            for (sx, sy) in [(2.0, 2.0), (5.0, 1.0), (5.0, 5.0)] {
                let old = bench_measure(|| {
                    let mut b = mask.clone();
                    reference_gaussian_blur(&mut b, sx, sy);
                    std::hint::black_box(b.data.first().copied());
                });
                let new = bench_measure(|| {
                    let mut b = mask.clone();
                    gaussian_blur(&mut b, sx, sy);
                    std::hint::black_box(b.data.first().copied());
                });
                let (o, n) = ((old - clone_ns).max(0.0), (new - clone_ns).max(0.0));
                println!(
                    "  {name:<14} sigma=({sx},{sy})  old {o:>9.0}  new {n:>9.0}  {:>4.2}x",
                    o / n.max(1.0)
                );
            }
        }
    }
}
