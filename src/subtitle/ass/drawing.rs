//! `\p` vector drawing parser — a port of libass 0.17.1 `ass_drawing.c`
//! (`drawing_tokenize` + `ass_drawing_parse`).
//!
//! Coordinates are kept in the same 26.6 fixed-point representation
//! (`double_to_d6`) so the b-spline integer arithmetic matches libass
//! bit for bit; the rasterizer divides by 64 when building paths.

use super::value::parse_f64_prefix;

/// One point in 26.6 fixed-point drawing units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Point6 {
    pub x: i32,
    pub y: i32,
}

/// A path command with resolved control points (cubic-only: b-splines are
/// converted, quadratic `q` segments are ignored like libass 0.17).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrawCmd {
    Move(Point6),
    Line(Point6),
    Cubic(Point6, Point6, Point6),
    Close,
}

/// A parsed drawing: commands plus the control box over all coordinates
/// (libass `cbox`, used for the drawing's layout metrics).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Drawing {
    pub cmds: Vec<DrawCmd>,
    /// (min, max) in 26.6 units; `None` when the drawing has no points.
    pub cbox: Option<(Point6, Point6)>,
}

/// `double_to_d6`: lrint(x * 64).
fn double_to_d6(x: f64) -> i32 {
    let scaled = (x * 64.0).round_ties_even();
    scaled.clamp(f64::from(i32::MIN), f64::from(i32::MAX)) as i32
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TokKind {
    Move,
    MoveNc,
    Line,
    CubicBezier,
    ConicBezier,
    BSpline,
}

#[derive(Debug, Clone, Copy)]
struct Token {
    kind: TokKind,
    point: Point6,
}

/// `drawing_tokenize`: numbers pair up into points; a command letter plus a
/// complete pair emits a token; the odd dangling coordinate is dropped; `c`
/// closes a b-spline by re-appending its first three points.
fn tokenize(text: &str) -> Vec<Token> {
    let bytes = text.as_bytes();
    let mut tokens: Vec<Token> = Vec::new();
    let mut kind: Option<TokKind> = None;
    let mut is_set = 0u8;
    let mut point = Point6::default();
    let mut spline_start: Option<usize> = None;

    let mut p = 0usize;
    while p < bytes.len() {
        let mut advance = 1usize;
        let mut got_coord = false;

        if bytes[p] == b'c' && spline_start.is_some() {
            let anchor = spline_start.expect("checked above");
            let all_spline = tokens.len() > anchor + 2
                && tokens[anchor + 1..=anchor + 2]
                    .iter()
                    .all(|t| t.kind == TokKind::BSpline);
            if all_spline {
                for i in 0..3 {
                    let point = tokens[anchor + i].point;
                    tokens.push(Token {
                        kind: TokKind::BSpline,
                        point,
                    });
                }
                spline_start = None;
            }
        } else if is_set < 2 && bytes[p] < 0x80 {
            // A number never starts with a non-ASCII byte. Guarding on
            // `bytes[p] < 0x80` also keeps `p` on a char boundary for the
            // slice below: an ASCII byte is a single-byte char, never a
            // UTF-8 continuation byte, so `&text[p..]` cannot split a
            // multibyte character (which would panic).
            if let Some((value, len)) = parse_f64_prefix(&text[p..]) {
                if is_set == 0 {
                    point.x = double_to_d6(value);
                    is_set = 1;
                } else {
                    point.y = double_to_d6(value);
                    is_set = 2;
                }
                got_coord = true;
                advance = len;
            }
        }
        if !got_coord {
            match bytes[p] {
                b'm' => kind = Some(TokKind::Move),
                b'n' => kind = Some(TokKind::MoveNc),
                b'l' => kind = Some(TokKind::Line),
                b'b' => kind = Some(TokKind::CubicBezier),
                b'q' => kind = Some(TokKind::ConicBezier),
                b's' => kind = Some(TokKind::BSpline),
                // 'p' (extend b-spline) is deliberately ignored, like libass.
                _ => {}
            }
            // "Ignore the odd extra value, it makes no sense."
            is_set = 0;
        }

        if is_set == 2 {
            if let Some(kind) = kind {
                tokens.push(Token { kind, point });
                is_set = 0;
                if kind == TokKind::BSpline && spline_start.is_none() && tokens.len() >= 2 {
                    spline_start = Some(tokens.len() - 2);
                }
            }
        }
        p += advance;
    }

    tokens
}

/// `drawing_add_curve`: reads a 4-point window (anchor + 3 controls); a
/// b-spline window is converted to a cubic with the exact libass integer
/// math (truncating `/3`, arithmetic `>>1` on 26.6 values).
fn add_curve(drawing: &mut Drawing, window: &[Token], spline: bool, started: bool) {
    let mut p: [Point6; 4] = [Point6::default(); 4];
    for (slot, token) in p.iter_mut().zip(window) {
        *slot = token.point;
        update_cbox(drawing, token.point);
    }

    if spline {
        let x01 = (p[1].x - p[0].x) / 3;
        let y01 = (p[1].y - p[0].y) / 3;
        let x12 = (p[2].x - p[1].x) / 3;
        let y12 = (p[2].y - p[1].y) / 3;
        let x23 = (p[3].x - p[2].x) / 3;
        let y23 = (p[3].y - p[2].y) / 3;

        p[0].x = p[1].x + ((x12 - x01) >> 1);
        p[0].y = p[1].y + ((y12 - y01) >> 1);
        p[3].x = p[2].x + ((x23 - x12) >> 1);
        p[3].y = p[2].y + ((y23 - y12) >> 1);
        p[1].x += x12;
        p[1].y += y12;
        p[2].x -= x12;
        p[2].y -= y12;
    }

    if !started {
        drawing.cmds.push(DrawCmd::Move(p[0]));
    }
    drawing.cmds.push(DrawCmd::Cubic(p[1], p[2], p[3]));
}

fn update_cbox(drawing: &mut Drawing, p: Point6) {
    drawing.cbox = Some(match drawing.cbox {
        None => (p, p),
        Some((min, max)) => (
            Point6 {
                x: min.x.min(p.x),
                y: min.y.min(p.y),
            },
            Point6 {
                x: max.x.max(p.x),
                y: max.y.max(p.y),
            },
        ),
    });
}

/// `ass_drawing_parse`: evaluates the token list with libass's pen model.
pub(crate) fn parse_drawing(text: &str) -> Drawing {
    let tokens = tokenize(text);
    let mut drawing = Drawing::default();
    let mut pen = Point6::default();
    let mut started = false;

    let mut i = 0usize;
    while i < tokens.len() {
        match tokens[i].kind {
            TokKind::MoveNc => {
                pen = tokens[i].point;
                update_cbox(&mut drawing, pen);
                i += 1;
            }
            TokKind::Move => {
                pen = tokens[i].point;
                update_cbox(&mut drawing, pen);
                if started {
                    drawing.cmds.push(DrawCmd::Close);
                    started = false;
                }
                i += 1;
            }
            TokKind::Line => {
                let to = tokens[i].point;
                update_cbox(&mut drawing, to);
                if !started {
                    drawing.cmds.push(DrawCmd::Move(pen));
                }
                drawing.cmds.push(DrawCmd::Line(to));
                started = true;
                i += 1;
            }
            TokKind::CubicBezier => {
                let have_window = i > 0
                    && tokens.len() > i + 2
                    && tokens[i..=i + 2]
                        .iter()
                        .all(|t| t.kind == TokKind::CubicBezier);
                if have_window {
                    add_curve(&mut drawing, &tokens[i - 1..=i + 2], false, started);
                    started = true;
                    i += 3;
                } else {
                    i += 1;
                }
            }
            TokKind::BSpline => {
                let have_window = i > 0
                    && tokens.len() > i + 2
                    && tokens[i..=i + 2].iter().all(|t| t.kind == TokKind::BSpline);
                if have_window {
                    add_curve(&mut drawing, &tokens[i - 1..=i + 2], true, started);
                    started = true;
                    i += 1;
                } else {
                    i += 1;
                }
            }
            TokKind::ConicBezier => i += 1, // tokenized but never drawn (libass 0.17)
        }
    }

    if started {
        drawing.cmds.push(DrawCmd::Close);
    }

    drawing
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d6(v: i32) -> i32 {
        v * 64
    }

    #[test]
    fn non_ascii_bytes_are_skipped_without_panic() {
        // A non-ASCII char inside a drawing must be skipped like any other
        // unknown byte, not panic on a non-char-boundary str slice. The
        // result equals the same drawing with the char removed.
        let with = parse_drawing("m 0 0 你 l 10 10");
        let without = parse_drawing("m 0 0 l 10 10");
        assert_eq!(with, without);
        // Emoji (4-byte) and a bare continuation-heavy string must also be safe.
        let _ = parse_drawing("m 0 0 🎬 l 5 5");
        let _ = parse_drawing("日本語のテスト");
    }

    #[test]
    fn rectangle_parses_to_closed_contour() {
        let drawing = parse_drawing("m 0 0 l 100 0 100 50 0 50");
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 0, y: 0 }),
                DrawCmd::Line(Point6 { x: d6(100), y: 0 }),
                DrawCmd::Line(Point6 {
                    x: d6(100),
                    y: d6(50)
                }),
                DrawCmd::Line(Point6 { x: 0, y: d6(50) }),
                DrawCmd::Close,
            ]
        );
        let (min, max) = drawing.cbox.expect("cbox");
        assert_eq!((min.x, min.y, max.x, max.y), (0, 0, d6(100), d6(50)));
    }

    #[test]
    fn implicit_line_repetition_and_fractions() {
        // Repeated coordinate pairs reuse the current command; fractional
        // coordinates hit the 26.6 quantization.
        let drawing = parse_drawing("m 0 0 l 10.5 0 l 10.5 10.25");
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 0, y: 0 }),
                DrawCmd::Line(Point6 { x: 672, y: 0 }),
                DrawCmd::Line(Point6 { x: 672, y: 656 }),
                DrawCmd::Close,
            ]
        );
    }

    #[test]
    fn second_move_closes_the_previous_contour() {
        let drawing = parse_drawing("m 0 0 l 10 0 10 10 m 20 20 l 30 20");
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 0, y: 0 }),
                DrawCmd::Line(Point6 { x: d6(10), y: 0 }),
                DrawCmd::Line(Point6 {
                    x: d6(10),
                    y: d6(10)
                }),
                DrawCmd::Close,
                DrawCmd::Move(Point6 {
                    x: d6(20),
                    y: d6(20)
                }),
                DrawCmd::Line(Point6 {
                    x: d6(30),
                    y: d6(20)
                }),
                DrawCmd::Close,
            ]
        );
    }

    #[test]
    fn move_nc_does_not_close() {
        let drawing = parse_drawing("m 0 0 l 10 0 n 20 20 l 30 20");
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 0, y: 0 }),
                DrawCmd::Line(Point6 { x: d6(10), y: 0 }),
                // n moves the pen without closing; the line continues from
                // the last outline point (libass keeps one open contour).
                DrawCmd::Line(Point6 {
                    x: d6(30),
                    y: d6(20)
                }),
                DrawCmd::Close,
            ]
        );
    }

    #[test]
    fn cubic_bezier_needs_three_control_points() {
        let drawing = parse_drawing("m 0 0 b 10 0 20 10 30 10");
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 0, y: 0 }),
                DrawCmd::Cubic(
                    Point6 { x: d6(10), y: 0 },
                    Point6 {
                        x: d6(20),
                        y: d6(10)
                    },
                    Point6 {
                        x: d6(30),
                        y: d6(10)
                    },
                ),
                DrawCmd::Close,
            ]
        );
        // Incomplete window: tokens skipped, nothing drawn at all (the
        // pen move alone emits no command).
        let drawing = parse_drawing("m 0 0 b 10 0 20 10");
        assert_eq!(drawing.cmds, vec![]);
    }

    #[test]
    fn bspline_converts_with_integer_math() {
        let drawing = parse_drawing("m 0 0 s 0 1 1 1 1 0");
        // Manual libass math for the window [(0,0),(0,64),(64,64),(64,0)]:
        // x01=0 y01=21, x12=21 y12=0, x23=0 y23=-21
        // start p0 = (0+((21-0)>>1), 64+((0-21)>>1)) = (10, 53)
        //   (arithmetic shift: -21>>1 == -11)
        // end   p3 = (64+((0-21)>>1), 64+((-21-0)>>1)) = (53, 53)
        // ctrl  p1 = (0+21, 64+0) = (21, 64); p2 = (64-21, 64) = (43, 64)
        // The curve's Move comes from the converted p0, not the pen.
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 10, y: 53 }),
                DrawCmd::Cubic(
                    Point6 { x: 21, y: 64 },
                    Point6 { x: 43, y: 64 },
                    Point6 { x: 53, y: 53 },
                ),
                DrawCmd::Close,
            ]
        );
    }

    #[test]
    fn odd_dangling_coordinate_is_dropped() {
        let drawing = parse_drawing("m 0 0 l 10 0 5 m 1 1");
        // "5" never completes a pair before 'm' resets the accumulator.
        assert_eq!(
            drawing.cmds,
            vec![
                DrawCmd::Move(Point6 { x: 0, y: 0 }),
                DrawCmd::Line(Point6 { x: d6(10), y: 0 }),
                DrawCmd::Close,
            ]
        );
    }

    #[test]
    fn empty_and_garbage_input() {
        assert_eq!(parse_drawing("").cmds, vec![]);
        assert_eq!(parse_drawing("xyz !!").cmds, vec![]);
        assert_eq!(parse_drawing("m 0 0").cmds, vec![]); // move alone draws nothing
    }
}
