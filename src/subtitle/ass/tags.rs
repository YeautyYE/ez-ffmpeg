//! Override tag lexer — a port of libass 0.17.1 `ass_parse_tags`
//! (ass_parse.c) up to but not including application: tags are parsed into
//! [`Tag`] values carrying raw arguments; validation, style fallbacks and
//! first-wins rules are applied by the layout state machine, mirroring
//! where libass performs them.
//!
//! Grammar quirks preserved: spaces allowed after the backslash, tag names
//! matched by prefix in the libass chain order, parenthesized arguments
//! split before the inline argument is appended, a backslash inside
//! parentheses swallows the rest as one argument, the closing parenthesis
//! is optional, and empty arguments are dropped.

use super::value::{
    parse_alpha_tag, parse_color_tag, parse_f64, parse_i32_saturating, Color, ASS_SPACES,
};

/// libass `MAX_VALID_NARGS`: up to 8 arguments are stored (one extra so an
/// over-long `\fade` is detectably invalid).
const MAX_ARGS: usize = 8;

/// Which of the four color/alpha slots a tag addresses
/// (primary, secondary, outline, back).
pub(crate) type ColorIndex = usize;

/// `\fs` accepts absolute sizes and the VSFilter relative form `\fs+N` /
/// `\fs-N` (multiplies the current size by `1 + N/10`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum FontSizeArg {
    Absolute(f64),
    RelativeFactor(f64),
}

/// One parsed override tag. `None` arguments mean "no argument given":
/// applying them resets the field to its style value (or to 0 for the
/// shear/blur family), exactly like libass.
/// Which karaoke effect a syllable uses (libass `Effect` enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum KaraokeKind {
    /// `\k`: primary color from the syllable start (EF_KARAOKE).
    Plain,
    /// `\kf`/`\K`: sweeping fill (EF_KARAOKE_KF), approximated stepwise.
    Fill,
    /// `\ko`: like `\k` plus no outline before the start (EF_KARAOKE_KO).
    Outline,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Tag<'a> {
    XBord(Option<f64>),
    YBord(Option<f64>),
    XShad(Option<f64>),
    YShad(Option<f64>),
    ShearX(Option<f64>),
    ShearY(Option<f64>),
    Blur(Option<f64>),
    Be(Option<f64>),
    /// Value already divided by 100 (a scale ratio), like libass.
    FontScaleX(Option<f64>),
    FontScaleY(Option<f64>),
    /// `\fsc`: resets both scales to the style values.
    ResetFontScale,
    Spacing(Option<f64>),
    FontSize(Option<FontSizeArg>),
    Bord(Option<f64>),
    Shad(Option<f64>),
    /// `\move`; `t1 <= t2` already ensured. Zero times mean "whole event".
    Move {
        x1: f64,
        y1: f64,
        x2: f64,
        y2: f64,
        t1: i32,
        t2: i32,
    },
    RotX(Option<f64>),
    RotY(Option<f64>),
    RotZ(Option<f64>),
    /// `None` = reset to the style font (no argument, or the literal `0`).
    FontName(Option<&'a str>),
    /// `\alpha`: all four slots at once.
    AlphaAll(Option<i32>),
    Alpha(ColorIndex, Option<i32>),
    Color(ColorIndex, Option<Color>),
    /// Raw `\an` argument; validation (1..=9) happens at application.
    AlignNumpad(i32),
    /// Raw legacy `\a` argument; validation (1..=11, 4/8 quirk) at application.
    AlignLegacy(i32),
    Pos {
        x: f64,
        y: f64,
    },
    /// Both `\fad` (two_arg) and `\fade`; the `t3 = t4 - t3` fixup for the
    /// two-argument form needs the event duration and happens at application.
    Fade {
        a1: i32,
        a2: i32,
        a3: i32,
        t1: i32,
        t2: i32,
        t3: i32,
        t4: i32,
        two_arg: bool,
    },
    Org {
        x: f64,
        y: f64,
    },
    /// `\t(...)`: parsed for grammar (so nested tags cannot corrupt the
    /// stream) but not animated yet — events render in their initial state.
    Transform,
    ClipRect {
        inverse: bool,
        x0: i32,
        y0: i32,
        x1: i32,
        y1: i32,
    },
    /// Vector `\clip`/`\iclip`: recognized, not yet applied.
    ClipVector {
        inverse: bool,
    },
    /// `\r` / `\rStyleName`.
    Reset(Option<&'a str>),
    Bold(Option<i32>),
    Italic(Option<i32>),
    Underline(Option<i32>),
    Strike(Option<i32>),
    /// `\k`/`\kf`/`\K`/`\ko`: karaoke syllable of `centisec` duration
    /// (default 100 like libass). `\kf`/`\K` sweep visuals are approximated
    /// stepwise by the renderer.
    Karaoke {
        kind: KaraokeKind,
        centisec: f64,
    },
    /// `\kt` (v4++): sets the absolute karaoke start offset and resets the
    /// accumulated timing.
    KaraokeSet(f64),
    /// `\p`; the argument is read unconditionally (a bare `\p` is scale 0,
    /// i.e. "exit drawing mode") and clamped to >= 0 like libass.
    DrawScale(i32),
    DrawBaselineOffset(f64),
    WrapStyle(Option<i32>),
    FontEncoding(Option<i32>),
}

fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t')
}

/// libass `push_arg`: trailing spaces stripped, empty arguments dropped,
/// capped at [`MAX_ARGS`].
fn push_arg<'a>(args: &mut Vec<&'a str>, arg: &'a str) {
    if args.len() < MAX_ARGS {
        let arg = arg.trim_end_matches(ASS_SPACES);
        if !arg.is_empty() {
            args.push(arg);
        }
    }
}

fn first(args: &[&str]) -> Option<f64> {
    args.first().map(|a| parse_f64(a))
}

fn first_i32(args: &[&str]) -> i32 {
    // libass reads args[0] unconditionally; the sentinel is an empty string
    // which parses to 0.
    args.first().map_or(0, |a| parse_i32_saturating(a, 10))
}

fn arg_i32(args: &[&str], index: usize) -> i32 {
    args.get(index).map_or(0, |a| parse_i32_saturating(a, 10))
}

fn arg_f64(args: &[&str], index: usize) -> f64 {
    args.get(index).map_or(0.0, |a| parse_f64(a))
}

/// Parses the inside of one `{...}` override block into tags.
pub(crate) fn parse_tag_block(block: &str) -> Vec<Tag<'_>> {
    let bytes = block.as_bytes();
    let mut tags = Vec::new();
    let mut p = 0usize;

    while p < bytes.len() {
        // Scan to the next backslash.
        while p < bytes.len() && bytes[p] != b'\\' {
            p += 1;
        }
        if p >= bytes.len() {
            break;
        }
        p += 1;
        // Spaces between the backslash and the tag name are allowed.
        while p < bytes.len() && is_space(bytes[p]) {
            p += 1;
        }

        // Tag name span: up to '(' or the next backslash.
        let name_start = p;
        let mut q = p;
        while q < bytes.len() && bytes[q] != b'(' && bytes[q] != b'\\' {
            q += 1;
        }
        if q == name_start {
            continue;
        }
        let name_end = q;

        // Parenthesized arguments are split for ALL tags, before any
        // inline argument — that's what VSFilter does.
        let mut args: Vec<&str> = Vec::new();
        let mut has_paren = false;
        if q < bytes.len() && bytes[q] == b'(' {
            has_paren = true;
            q += 1;
            loop {
                while q < bytes.len() && is_space(bytes[q]) {
                    q += 1;
                }
                let arg_start = q;
                let mut r = q;
                while r < bytes.len() && !matches!(bytes[r], b',' | b'\\' | b')') {
                    r += 1;
                }
                if r < bytes.len() && bytes[r] == b',' {
                    push_arg(&mut args, &block[arg_start..r]);
                    q = r + 1;
                } else {
                    // A backslash swallows everything to the ')' as one
                    // argument (nested tags in \t, drawings in \clip).
                    if r < bytes.len() && bytes[r] == b'\\' {
                        while r < bytes.len() && bytes[r] != b')' {
                            r += 1;
                        }
                    }
                    push_arg(&mut args, &block[arg_start..r]);
                    q = r;
                    // The closing parenthesis may be missing.
                    if q < bytes.len() {
                        q += 1;
                    }
                    break;
                }
            }
        }

        let name = &block[name_start..name_end];
        let _ = has_paren; // grammar bookkeeping only; kept for readability
        if let Some(tag) = match_tag(name, args) {
            tags.push(tag);
        }
        p = q;
    }

    tags
}

/// `mystrcmp` + inline-argument push: on a prefix match, the rest of the
/// name span becomes the last argument (trailing spaces trimmed, empty
/// dropped).
fn tag<'a>(name: &str, sample: &str, args: &mut Vec<&'a str>, full: &'a str) -> bool {
    match name.strip_prefix(sample) {
        Some(_) => {
            // `full` is the same span as `name` but with the original
            // lifetime; slice the inline-arg region out of it.
            push_arg(args, &full[sample.len()..]);
            true
        }
        None => false,
    }
}

/// `complex_tag`: prefix match without the inline-argument push.
fn complex(name: &str, sample: &str) -> bool {
    name.starts_with(sample)
}

/// The libass dispatch chain, in its exact order (prefix matching makes the
/// order load-bearing: `fscx` must be tried before `fsc` before `fs`, ...).
fn match_tag<'a>(name: &'a str, mut args: Vec<&'a str>) -> Option<Tag<'a>> {
    let full = name;
    let a = &mut args;
    if tag(name, "xbord", a, full) {
        Some(Tag::XBord(first(a)))
    } else if tag(name, "ybord", a, full) {
        Some(Tag::YBord(first(a)))
    } else if tag(name, "xshad", a, full) {
        Some(Tag::XShad(first(a)))
    } else if tag(name, "yshad", a, full) {
        Some(Tag::YShad(first(a)))
    } else if tag(name, "fax", a, full) {
        Some(Tag::ShearX(first(a)))
    } else if tag(name, "fay", a, full) {
        Some(Tag::ShearY(first(a)))
    } else if complex(name, "iclip") {
        clip_tag(a, true)
    } else if tag(name, "blur", a, full) {
        Some(Tag::Blur(first(a)))
    } else if tag(name, "fscx", a, full) {
        Some(Tag::FontScaleX(first(a).map(|v| v / 100.0)))
    } else if tag(name, "fscy", a, full) {
        Some(Tag::FontScaleY(first(a).map(|v| v / 100.0)))
    } else if tag(name, "fsc", a, full) {
        Some(Tag::ResetFontScale)
    } else if tag(name, "fsp", a, full) {
        Some(Tag::Spacing(first(a)))
    } else if tag(name, "fs", a, full) {
        let arg = a.first().map(|raw| {
            let value = parse_f64(raw);
            if raw.starts_with(['+', '-']) {
                FontSizeArg::RelativeFactor(value)
            } else {
                FontSizeArg::Absolute(value)
            }
        });
        Some(Tag::FontSize(arg))
    } else if tag(name, "bord", a, full) {
        Some(Tag::Bord(first(a)))
    } else if complex(name, "move") {
        if a.len() == 4 || a.len() == 6 {
            let (mut t1, mut t2) = (0, 0);
            if a.len() == 6 {
                t1 = arg_i32(a, 4);
                t2 = arg_i32(a, 5);
                if t1 > t2 {
                    std::mem::swap(&mut t1, &mut t2);
                }
            }
            Some(Tag::Move {
                x1: arg_f64(a, 0),
                y1: arg_f64(a, 1),
                x2: arg_f64(a, 2),
                y2: arg_f64(a, 3),
                t1,
                t2,
            })
        } else {
            None
        }
    } else if tag(name, "frx", a, full) {
        Some(Tag::RotX(first(a)))
    } else if tag(name, "fry", a, full) {
        Some(Tag::RotY(first(a)))
    } else if tag(name, "frz", a, full) || tag(name, "fr", a, full) {
        Some(Tag::RotZ(first(a)))
    } else if tag(name, "fn", a, full) {
        let family = match a.first() {
            Some(&raw) if raw != "0" => {
                // libass skips leading spaces of the family here.
                Some(raw.trim_start_matches(ASS_SPACES))
            }
            _ => None,
        };
        Some(Tag::FontName(family))
    } else if tag(name, "alpha", a, full) {
        Some(Tag::AlphaAll(a.first().map(|s| parse_alpha_tag(s))))
    } else if tag(name, "an", a, full) {
        Some(Tag::AlignNumpad(first_i32(a)))
    } else if tag(name, "a", a, full) {
        Some(Tag::AlignLegacy(first_i32(a)))
    } else if complex(name, "pos") {
        if a.len() == 2 {
            Some(Tag::Pos {
                x: arg_f64(a, 0),
                y: arg_f64(a, 1),
            })
        } else {
            None
        }
    } else if complex(name, "fade") || complex(name, "fad") {
        if a.len() == 2 {
            Some(Tag::Fade {
                a1: 0xFF,
                a2: 0,
                a3: 0xFF,
                t1: -1,
                t2: arg_i32(a, 0),
                t3: arg_i32(a, 1),
                t4: -1,
                two_arg: true,
            })
        } else if a.len() == 7 {
            Some(Tag::Fade {
                a1: arg_i32(a, 0),
                a2: arg_i32(a, 1),
                a3: arg_i32(a, 2),
                t1: arg_i32(a, 3),
                t2: arg_i32(a, 4),
                t3: arg_i32(a, 5),
                t4: arg_i32(a, 6),
                two_arg: false,
            })
        } else {
            None
        }
    } else if complex(name, "org") {
        if a.len() == 2 {
            Some(Tag::Org {
                x: arg_f64(a, 0),
                y: arg_f64(a, 1),
            })
        } else {
            None
        }
    } else if complex(name, "t") {
        Some(Tag::Transform)
    } else if complex(name, "clip") {
        clip_tag(a, false)
    } else if tag(name, "c", a, full) || tag(name, "1c", a, full) {
        Some(Tag::Color(0, a.first().map(|s| parse_color_tag(s))))
    } else if tag(name, "2c", a, full) {
        Some(Tag::Color(1, a.first().map(|s| parse_color_tag(s))))
    } else if tag(name, "3c", a, full) {
        Some(Tag::Color(2, a.first().map(|s| parse_color_tag(s))))
    } else if tag(name, "4c", a, full) {
        Some(Tag::Color(3, a.first().map(|s| parse_color_tag(s))))
    } else if tag(name, "1a", a, full) {
        Some(Tag::Alpha(0, a.first().map(|s| parse_alpha_tag(s))))
    } else if tag(name, "2a", a, full) {
        Some(Tag::Alpha(1, a.first().map(|s| parse_alpha_tag(s))))
    } else if tag(name, "3a", a, full) {
        Some(Tag::Alpha(2, a.first().map(|s| parse_alpha_tag(s))))
    } else if tag(name, "4a", a, full) {
        Some(Tag::Alpha(3, a.first().map(|s| parse_alpha_tag(s))))
    } else if tag(name, "r", a, full) {
        Some(Tag::Reset(a.first().copied()))
    } else if tag(name, "be", a, full) {
        Some(Tag::Be(first(a)))
    } else if tag(name, "b", a, full) {
        Some(Tag::Bold(a.first().map(|_| first_i32(a))))
    } else if tag(name, "i", a, full) {
        Some(Tag::Italic(a.first().map(|_| first_i32(a))))
    } else if tag(name, "kt", a, full) {
        // libass: val = argtod(*args) * 10 with default 0.
        Some(Tag::KaraokeSet(first(a).unwrap_or(0.0)))
    } else if tag(name, "kf", a, full) || tag(name, "K", a, full) {
        Some(Tag::Karaoke {
            kind: KaraokeKind::Fill,
            centisec: first(a).unwrap_or(100.0),
        })
    } else if tag(name, "ko", a, full) {
        Some(Tag::Karaoke {
            kind: KaraokeKind::Outline,
            centisec: first(a).unwrap_or(100.0),
        })
    } else if tag(name, "k", a, full) {
        Some(Tag::Karaoke {
            kind: KaraokeKind::Plain,
            centisec: first(a).unwrap_or(100.0),
        })
    } else if tag(name, "shad", a, full) {
        Some(Tag::Shad(first(a)))
    } else if tag(name, "s", a, full) {
        Some(Tag::Strike(a.first().map(|_| first_i32(a))))
    } else if tag(name, "u", a, full) {
        Some(Tag::Underline(a.first().map(|_| first_i32(a))))
    } else if tag(name, "pbo", a, full) {
        Some(Tag::DrawBaselineOffset(
            a.first().map_or(0.0, |s| parse_f64(s)),
        ))
    } else if tag(name, "p", a, full) {
        Some(Tag::DrawScale(first_i32(a).max(0)))
    } else if tag(name, "q", a, full) {
        Some(Tag::WrapStyle(a.first().map(|_| first_i32(a))))
    } else if tag(name, "fe", a, full) {
        Some(Tag::FontEncoding(a.first().map(|_| first_i32(a))))
    } else {
        None
    }
}

fn clip_tag(args: &[&str], inverse: bool) -> Option<Tag<'static>> {
    if args.len() == 4 {
        Some(Tag::ClipRect {
            inverse,
            x0: arg_i32(args, 0),
            y0: arg_i32(args, 1),
            x1: arg_i32(args, 2),
            y1: arg_i32(args, 3),
        })
    } else if args.len() == 1 || args.len() == 2 {
        Some(Tag::ClipVector { inverse })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one(block: &str) -> Tag<'_> {
        let tags = parse_tag_block(block);
        assert_eq!(
            tags.len(),
            1,
            "expected exactly one tag in {block:?}: {tags:?}"
        );
        tags.into_iter().next().unwrap()
    }

    #[test]
    fn inline_and_paren_arguments() {
        assert_eq!(one(r"\bord2"), Tag::Bord(Some(2.0)));
        assert_eq!(one(r"\bord 2 "), Tag::Bord(Some(2.0)));
        assert_eq!(one(r"\bord(2)"), Tag::Bord(Some(2.0)));
        // Paren args come first; the inline arg is appended after.
        assert_eq!(one(r"\bord2(3)"), Tag::Bord(Some(3.0)));
        // No argument = reset to style.
        assert_eq!(one(r"\bord"), Tag::Bord(None));
        // Spaces after the backslash are allowed.
        assert_eq!(one("\\ \tbord2"), Tag::Bord(Some(2.0)));
    }

    #[test]
    fn prefix_matching_uses_libass_chain_order() {
        assert_eq!(one(r"\fscx50"), Tag::FontScaleX(Some(0.5)));
        assert_eq!(one(r"\fsc"), Tag::ResetFontScale);
        assert_eq!(one(r"\fsp1.5"), Tag::Spacing(Some(1.5)));
        assert_eq!(
            one(r"\fs20"),
            Tag::FontSize(Some(FontSizeArg::Absolute(20.0)))
        );
        assert_eq!(
            one(r"\fs+10"),
            Tag::FontSize(Some(FontSizeArg::RelativeFactor(10.0)))
        );
        assert_eq!(one(r"\frz45"), Tag::RotZ(Some(45.0)));
        assert_eq!(one(r"\fr45"), Tag::RotZ(Some(45.0)));
        assert_eq!(
            one(r"\1c&HFF&"),
            Tag::Color(
                0,
                Some(Color {
                    r: 255,
                    g: 0,
                    b: 0,
                    t: 0
                })
            )
        );
        assert_eq!(
            one(r"\c&HFF0000&"),
            Tag::Color(
                0,
                Some(Color {
                    r: 0,
                    g: 0,
                    b: 255,
                    t: 0
                })
            )
        );
        assert_eq!(one(r"\4a&H80&"), Tag::Alpha(3, Some(0x80)));
        assert_eq!(
            one(r"\K50"),
            Tag::Karaoke {
                kind: KaraokeKind::Fill,
                centisec: 50.0
            }
        );
        assert_eq!(
            one(r"\kf50"),
            Tag::Karaoke {
                kind: KaraokeKind::Fill,
                centisec: 50.0
            }
        );
        assert_eq!(
            one(r"\k"),
            Tag::Karaoke {
                kind: KaraokeKind::Plain,
                centisec: 100.0
            }
        );
        assert_eq!(
            one(r"\ko25"),
            Tag::Karaoke {
                kind: KaraokeKind::Outline,
                centisec: 25.0
            }
        );
        assert_eq!(one(r"\kt150"), Tag::KaraokeSet(150.0));
        assert_eq!(one(r"\kt"), Tag::KaraokeSet(0.0));
    }

    #[test]
    fn positioning_tags_validate_arg_count() {
        assert_eq!(one(r"\pos(10,20)"), Tag::Pos { x: 10.0, y: 20.0 });
        assert_eq!(one(r"\pos( 10 , 20 )"), Tag::Pos { x: 10.0, y: 20.0 });
        // Missing ')' is tolerated.
        assert_eq!(one(r"\pos(10,20"), Tag::Pos { x: 10.0, y: 20.0 });
        // Wrong arity: tag dropped.
        assert!(parse_tag_block(r"\pos(10)").is_empty());
        assert!(parse_tag_block(r"\pos(10,20,30)").is_empty());
        // \move with swapped times normalizes t1 <= t2.
        assert_eq!(
            one(r"\move(0,0,100,50,900,300)"),
            Tag::Move {
                x1: 0.0,
                y1: 0.0,
                x2: 100.0,
                y2: 50.0,
                t1: 300,
                t2: 900
            }
        );
        assert_eq!(
            one(r"\fad(200,300)"),
            Tag::Fade {
                a1: 255,
                a2: 0,
                a3: 255,
                t1: -1,
                t2: 200,
                t3: 300,
                t4: -1,
                two_arg: true
            }
        );
        assert_eq!(
            one(r"\clip(0,0,100,50)"),
            Tag::ClipRect {
                inverse: false,
                x0: 0,
                y0: 0,
                x1: 100,
                y1: 50
            }
        );
        assert_eq!(
            one(r"\iclip(0,0,100,50)"),
            Tag::ClipRect {
                inverse: true,
                x0: 0,
                y0: 0,
                x1: 100,
                y1: 50
            }
        );
        assert_eq!(
            one(r"\clip(m 0 0 l 10 0 10 10)"),
            Tag::ClipVector { inverse: false }
        );
    }

    #[test]
    fn font_name_and_reset() {
        assert_eq!(one(r"\fnArial Black"), Tag::FontName(Some("Arial Black")));
        assert_eq!(one(r"\fn Arial"), Tag::FontName(Some("Arial")));
        assert_eq!(one(r"\fn0"), Tag::FontName(None));
        assert_eq!(one(r"\fn"), Tag::FontName(None));
        assert_eq!(one(r"\rAlt"), Tag::Reset(Some("Alt")));
        assert_eq!(one(r"\r"), Tag::Reset(None));
    }

    #[test]
    fn flag_tags_keep_raw_values_for_validation_at_apply() {
        assert_eq!(one(r"\b1"), Tag::Bold(Some(1)));
        assert_eq!(one(r"\b700"), Tag::Bold(Some(700)));
        assert_eq!(one(r"\b"), Tag::Bold(None));
        assert_eq!(one(r"\i1"), Tag::Italic(Some(1)));
        assert_eq!(one(r"\u0"), Tag::Underline(Some(0)));
        assert_eq!(one(r"\s1"), Tag::Strike(Some(1)));
        assert_eq!(one(r"\an7"), Tag::AlignNumpad(7));
        assert_eq!(one(r"\an"), Tag::AlignNumpad(0));
        assert_eq!(one(r"\a11"), Tag::AlignLegacy(11));
        assert_eq!(one(r"\q2"), Tag::WrapStyle(Some(2)));
        assert_eq!(one(r"\p1"), Tag::DrawScale(1));
        assert_eq!(one(r"\p-2"), Tag::DrawScale(0)); // clamped like libass
        assert_eq!(one(r"\p"), Tag::DrawScale(0)); // bare \p exits drawing mode
        assert_eq!(one(r"\pbo-4"), Tag::DrawBaselineOffset(-4.0));
    }

    #[test]
    fn transform_swallows_nested_tags() {
        // The backslash-argument rule keeps nested tags inside one argument;
        // the block must yield \t (ignored) and then \bord.
        let tags = parse_tag_block(r"\t(0,500,\fs30\1c&HFF&)\bord2");
        assert_eq!(tags, vec![Tag::Transform, Tag::Bord(Some(2.0))]);
    }

    #[test]
    fn unknown_tags_are_skipped() {
        assert!(parse_tag_block(r"\xyzzy42").is_empty());
        let tags = parse_tag_block(r"\xyzzy\bord2");
        assert_eq!(tags, vec![Tag::Bord(Some(2.0))]);
        // Empty name (dangling backslash) is skipped.
        assert!(parse_tag_block("\\").is_empty());
    }
}
