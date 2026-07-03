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

/// Rasterizes a filled path (non-zero rule, like FreeType/VSFilter).
pub(crate) fn fill_path(commands: &[Command]) -> CoverageBitmap {
    if commands.is_empty() {
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
    let mut stroke = Stroke::new(2.0 * rx);
    stroke.cap(Cap::Round).join(Join::Round);
    let (data, placement) = Mask::new(&squeezed).style(Style::Stroke(stroke)).render();
    let squeezed_bitmap = CoverageBitmap::from_mask(data, placement);
    resample_rows(&squeezed_bitmap, f64::from(squeeze)).max_with(fill)
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

/// Box-resamples a bitmap rendered in a Y-scaled space (y' = y * sy) back
/// to frame rows, preserving coverage.
fn resample_rows(src: &CoverageBitmap, sy: f64) -> CoverageBitmap {
    if src.is_empty() {
        return CoverageBitmap::default();
    }
    let y0 = (f64::from(src.y) / sy).floor() as i32;
    let y1 = ((f64::from(src.y) + src.h as f64) / sy).ceil() as i32;
    let h = (y1 - y0).max(1) as usize;
    let mut data = vec![0u8; src.w * h];
    for row in 0..h {
        // Frame row `row` covers source rows [a, b) in the squeezed space.
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
    if be > 1 {
        // be_blur_pre: (v*64 + 127) / 255, kept in 8 bits.
        for v in bitmap.data.iter_mut() {
            *v = ((*v >> 1) + 1) >> 1;
        }
        while be > 1 {
            be_blur_pass(&mut bitmap.data, w, h);
            be -= 1;
        }
        // be_blur_post: (v*255 + 32) / 64 == (v << 2) - (v > 32).
        for v in bitmap.data.iter_mut() {
            *v = ((i32::from(*v) << 2) - i32::from(*v > 32)) as u8;
        }
    }
    be_blur_pass(&mut bitmap.data, w, h);
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
/// exactly like the reference implementation.
fn be_blur_pass(buf: &mut [u8], w: usize, h: usize) {
    let mut col_pix = vec![0u16; w];
    let mut col_sum = vec![0u16; w];
    let mut horiz = vec![0u16; w];

    // First row primes the column accumulators without output.
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
    let boxes_x = boxes_for_gauss(sigma_x.max(0.0), 3);
    let boxes_y = boxes_for_gauss(sigma_y.max(0.0), 3);
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
            box_blur_v(&tmp, &mut bitmap.data, w, h, ry);
        } else {
            bitmap.data.copy_from_slice(&tmp);
        }
    }
}

fn boxes_for_gauss(sigma: f64, n: usize) -> Vec<usize> {
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

fn box_blur_v(src: &[u8], dst: &mut [u8], w: usize, h: usize, r: usize) {
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
}
