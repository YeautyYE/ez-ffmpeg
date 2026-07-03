//! Scalar value parsing with VSFilter-compatible semantics.
//!
//! Every function is a faithful port of its libass 0.17.1 counterpart
//! (`ass.c` / `ass_utils.h` / `ass_strtod.c`), including the quirks: numbers
//! wrap modulo 2^32, trailing junk is ignored, invalid input parses as 0.

/// `ass_utils.h skip_spaces`: only spaces and tabs.
pub(crate) const ASS_SPACES: &[char] = &[' ', '\t'];

/// C `isspace` set used by `ass_isspace` / `ass_strtod` / `sscanf`.
fn is_c_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\x0b' | b'\x0c' | b'\r')
}

/// Case-insensitive ASCII prefix strip.
fn strip_ci_prefix<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    let head = s.as_bytes().get(..prefix.len())?;
    head.eq_ignore_ascii_case(prefix.as_bytes())
        .then(|| &s[prefix.len()..]) // prefix is ASCII, so this is a char boundary
}

/// `mystrtou32_modulo`: strtoul-like, but reduces modulo 2^32 instead of
/// saturating (emulates the Windows `scanf` behavior VSFilter relies on).
/// Invalid input yields 0.
fn wrapping_u32(s: &str, base: u32) -> u32 {
    let s = s.trim_start_matches(ASS_SPACES);
    let (negative, s) = match s.as_bytes().first() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    // A second optional "0x" is consumed in base 16 ("&H0xFF" parses as 255).
    let s = match base {
        16 => strip_ci_prefix(s, "0x").unwrap_or(s),
        _ => s,
    };
    let mut value: u32 = 0;
    let mut any_digit = false;
    for &byte in s.as_bytes() {
        let digit = match byte {
            b'0'..=b'9' if u32::from(byte - b'0') < base.min(10) => u32::from(byte - b'0'),
            b'a'..=b'f' if base > 10 && u32::from(byte - b'a') < base - 10 => {
                u32::from(byte - b'a') + 10
            }
            b'A'..=b'F' if base > 10 && u32::from(byte - b'A') < base - 10 => {
                u32::from(byte - b'A') + 10
            }
            _ => break,
        };
        value = value.wrapping_mul(base).wrapping_add(digit);
        any_digit = true;
    }
    if !any_digit {
        return 0;
    }
    if negative {
        value.wrapping_neg()
    } else {
        value
    }
}

/// `parse_int_header`: optional case-insensitive `&H`/`0x` prefix selects
/// base 16 (checked on the raw string, before any space skipping).
pub(crate) fn parse_int(s: &str) -> i32 {
    let (rest, base) = match strip_ci_prefix(s, "&h").or_else(|| strip_ci_prefix(s, "0x")) {
        Some(rest) => (rest, 16),
        None => (s, 10),
    };
    wrapping_u32(rest, base) as i32
}

/// C `strtol(str, NULL, 10)` with saturation, as used by `parse_bool`.
fn strtol10(s: &str) -> i64 {
    let bytes = s.as_bytes();
    let mut i = 0;
    while bytes.get(i).copied().is_some_and(is_c_space) {
        i += 1;
    }
    let negative = match bytes.get(i) {
        Some(b'+') => {
            i += 1;
            false
        }
        Some(b'-') => {
            i += 1;
            true
        }
        _ => false,
    };
    let mut value: i64 = 0;
    while let Some(&byte @ b'0'..=b'9') = bytes.get(i) {
        let digit = i64::from(byte - b'0');
        value = value
            .saturating_mul(10)
            .saturating_add(if negative { -digit } else { digit });
        i += 1;
    }
    value
}

/// `parse_bool`: leading spaces skipped, then a case-insensitive `yes`
/// PREFIX ("yesterday" is true), otherwise `strtol(...) > 0`.
pub(crate) fn parse_bool(s: &str) -> bool {
    let s = s.trim_start_matches(ASS_SPACES);
    let yes = s
        .as_bytes()
        .get(..3)
        .is_some_and(|head| head.eq_ignore_ascii_case(b"yes"));
    yes || strtol10(s) > 0
}

/// `ass_strtod` (the classic Tcl strtod): C whitespace, optional sign,
/// decimal digits with one optional dot, optional exponent (backtracked when
/// it has no digits). No hex floats, no inf/nan. Returns the value and the
/// total consumed length (including leading whitespace), or `None` when
/// nothing was consumed — the `mystrtod` success condition.
pub(crate) fn parse_f64_prefix(s: &str) -> Option<(f64, usize)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while bytes.get(i).copied().is_some_and(is_c_space) {
        i += 1;
    }
    let start = i;
    if matches!(bytes.get(i), Some(b'+' | b'-')) {
        i += 1;
    }
    let mut mantissa_digits = 0usize;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        mantissa_digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
            mantissa_digits += 1;
        }
    }
    if mantissa_digits == 0 {
        return None;
    }
    let mut end = i;
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        let mut j = i + 1;
        if matches!(bytes.get(j), Some(b'+' | b'-')) {
            j += 1;
        }
        if bytes.get(j).is_some_and(u8::is_ascii_digit) {
            while bytes.get(j).is_some_and(u8::is_ascii_digit) {
                j += 1;
            }
            end = j;
        }
    }
    // Only ASCII bytes were stepped over, so start/end are char boundaries.
    Some((s[start..end].parse().unwrap_or(0.0), end))
}

/// `ass_atof`: [`parse_f64_prefix`] with the libass convention that invalid
/// input yields 0.0.
pub(crate) fn parse_f64(s: &str) -> f64 {
    parse_f64_prefix(s).map_or(0.0, |(value, _)| value)
}

/// One `sscanf("%d", ...)` conversion: C whitespace, optional sign, at least
/// one digit (saturating to i32 like glibc). Returns the value and the rest.
fn scan_c_int(s: &str) -> Option<(i32, &str)> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while bytes.get(i).copied().is_some_and(is_c_space) {
        i += 1;
    }
    let negative = match bytes.get(i) {
        Some(b'+') => {
            i += 1;
            false
        }
        Some(b'-') => {
            i += 1;
            true
        }
        _ => false,
    };
    let digits_start = i;
    let mut value: i64 = 0;
    while let Some(&byte @ b'0'..=b'9') = bytes.get(i) {
        let digit = i64::from(byte - b'0');
        value = value
            .saturating_mul(10)
            .saturating_add(if negative { -digit } else { digit });
        i += 1;
    }
    if i == digits_start {
        return None;
    }
    let value = value.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32;
    Some((value, &s[i..])) // ASCII scan, so i is a char boundary
}

/// `string2timecode`: `sscanf("%d:%d:%d.%d")`; all four fields are required.
/// The last field is centiseconds and is multiplied by 10 regardless of its
/// digit count (".5" is 50 ms, ".500" is 5000 ms — a preserved quirk).
/// `None` corresponds to libass's "Bad timestamp" warning (callers use 0).
pub(crate) fn parse_timecode_ms(s: &str) -> Option<i64> {
    let (hours, s) = scan_c_int(s)?;
    let s = s.strip_prefix(':')?;
    let (minutes, s) = scan_c_int(s)?;
    let s = s.strip_prefix(':')?;
    let (seconds, s) = scan_c_int(s)?;
    let s = s.strip_prefix('.')?;
    let (centis, _) = scan_c_int(s)?;
    Some(
        ((i64::from(hours) * 60 + i64::from(minutes)) * 60 + i64::from(seconds)) * 1000
            + i64::from(centis) * 10,
    )
}

/// A style/event color. ASS files store `&HAABBGGRR` where `AA` is the
/// transparency; this struct is the byte-order libass exposes after its
/// bswap (`0xRRGGBBTT`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Color {
    pub r: u8,
    pub g: u8,
    pub b: u8,
    /// ASS transparency: 0 = opaque, 255 = fully transparent.
    pub t: u8,
}

impl Color {
    pub(crate) fn from_packed(rgbt: u32) -> Self {
        Self {
            r: (rgbt >> 24) as u8,
            g: (rgbt >> 16) as u8,
            b: (rgbt >> 8) as u8,
            t: rgbt as u8,
        }
    }

    #[cfg(test)]
    pub(crate) fn packed(self) -> u32 {
        u32::from(self.r) << 24
            | u32::from(self.g) << 16
            | u32::from(self.b) << 8
            | u32::from(self.t)
    }
}

/// `parse_color_header`: VSFilter integer parse, then a byte swap turns the
/// file's `0xAABBGGRR` into `0xRRGGBBTT`.
pub(crate) fn parse_color(s: &str) -> Color {
    Color::from_packed((parse_int(s) as u32).swap_bytes())
}

/// `mystrtoi32`: C `strtoll` (whitespace, sign, optional `0x` in base 16,
/// saturating) clamped to the i32 range. Unlike the header parser this
/// SATURATES instead of wrapping — override tags use this one.
pub(crate) fn parse_i32_saturating(s: &str, base: u32) -> i32 {
    debug_assert!(base == 10 || base == 16);
    let bytes = s.as_bytes();
    let mut i = 0;
    while bytes.get(i).copied().is_some_and(is_c_space) {
        i += 1;
    }
    let negative = match bytes.get(i) {
        Some(b'+') => {
            i += 1;
            false
        }
        Some(b'-') => {
            i += 1;
            true
        }
        _ => false,
    };
    if base == 16
        && bytes.get(i) == Some(&b'0')
        && matches!(bytes.get(i + 1), Some(b'x' | b'X'))
        && bytes.get(i + 2).is_some_and(|b| b.is_ascii_hexdigit())
    {
        i += 2;
    }
    let mut value: i64 = 0;
    loop {
        let digit = match bytes.get(i) {
            Some(&b @ b'0'..=b'9') => i64::from(b - b'0'),
            Some(&b @ b'a'..=b'f') if base == 16 => i64::from(b - b'a') + 10,
            Some(&b @ b'A'..=b'F') if base == 16 => i64::from(b - b'A') + 10,
            _ => break,
        };
        value = value
            .saturating_mul(i64::from(base))
            .saturating_add(if negative { -digit } else { digit });
        i += 1;
    }
    value.clamp(i64::from(i32::MIN), i64::from(i32::MAX)) as i32
}

/// `&`/uppercase-`H` prefix run skipped by the tag color/alpha parsers
/// (lowercase `h` is NOT skipped — a preserved VSFilter quirk).
const AMP_H: &[char] = &['&', 'H'];

/// `parse_alpha_tag`: prefix junk skipped, then a saturating hex parse.
pub(crate) fn parse_alpha_tag(s: &str) -> i32 {
    parse_i32_saturating(s.trim_start_matches(AMP_H), 16)
}

/// `parse_color_tag`: like [`parse_alpha_tag`], byte-swapped into a color.
pub(crate) fn parse_color_tag(s: &str) -> Color {
    let raw = parse_i32_saturating(s.trim_start_matches(AMP_H), 16);
    Color::from_packed((raw as u32).swap_bytes())
}

/// `YCbCr Matrix` Script Info values (ass_types.h `ASS_YCbCrMatrix`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum YcbcrMatrix {
    /// Header absent or empty: guess based on resolution (VSFilter default).
    #[default]
    Default,
    /// Header present but unrecognized: use the default matrix, no guessing.
    Unknown,
    /// `None`: color mangling explicitly disabled.
    None,
    Bt601Tv,
    Bt601Pc,
    Bt709Tv,
    Bt709Pc,
    Smpte240mTv,
    Smpte240mPc,
    FccTv,
    FccPc,
}

/// `parse_ycbcr_matrix`: trimmed, case-insensitive match; empty means
/// "Default", anything unrecognized means "Unknown".
pub(crate) fn parse_ycbcr_matrix(s: &str) -> YcbcrMatrix {
    let s = s
        .trim_start_matches(ASS_SPACES)
        .trim_end_matches(ASS_SPACES);
    if s.is_empty() {
        return YcbcrMatrix::Default;
    }
    for (name, value) in [
        ("none", YcbcrMatrix::None),
        ("tv.601", YcbcrMatrix::Bt601Tv),
        ("pc.601", YcbcrMatrix::Bt601Pc),
        ("tv.709", YcbcrMatrix::Bt709Tv),
        ("pc.709", YcbcrMatrix::Bt709Pc),
        ("tv.240m", YcbcrMatrix::Smpte240mTv),
        ("pc.240m", YcbcrMatrix::Smpte240mPc),
        ("tv.fcc", YcbcrMatrix::FccTv),
        ("pc.fcc", YcbcrMatrix::FccPc),
    ] {
        if s.eq_ignore_ascii_case(name) {
            return value;
        }
    }
    YcbcrMatrix::Unknown
}

/// Alignment vertical bits (ass_types.h): the parsed `Alignment` is stored
/// in the legacy VSFilter encoding `halign(1..=3) | valign`.
pub(crate) const VALIGN_SUB: i32 = 0;
pub(crate) const VALIGN_TOP: i32 = 4;
pub(crate) const VALIGN_CENTER: i32 = 8;

/// `ass_utils.h numpad2align`: numpad (1..=9) to legacy encoding, with the
/// exact out-of-range behavior (0 maps to 0, negatives are mirrored,
/// `i32::MIN` picks 2).
pub(crate) fn numpad2align(val: i32) -> i32 {
    let val = if val == i32::MIN { 2 } else { val.abs() };
    let horizontal = ((val - 1) % 3) + 1; // val 0 yields 0, like the C code
    if val <= 3 {
        horizontal | VALIGN_SUB
    } else if val <= 6 {
        horizontal | VALIGN_CENTER
    } else {
        horizontal | VALIGN_TOP
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_int_matches_vsfilter_semantics() {
        assert_eq!(parse_int("123"), 123);
        assert_eq!(parse_int(" \t-5"), -5);
        assert_eq!(parse_int("+7"), 7);
        assert_eq!(parse_int("12abc"), 12); // trailing junk ignored
        assert_eq!(parse_int("abc"), 0);
        assert_eq!(parse_int(""), 0);
        assert_eq!(parse_int("&HFF"), 255);
        assert_eq!(parse_int("&hff"), 255);
        assert_eq!(parse_int("0xFF"), 255);
        assert_eq!(parse_int("&H0xFF"), 255); // double prefix is consumed
        assert_eq!(parse_int("&H"), 0);
        assert_eq!(parse_int("&HFF&"), 255); // trailing '&' is junk
        assert_eq!(parse_int("&H-10"), -16);
        assert_eq!(parse_int(" &HFF"), 0); // prefix check runs before space skip
        assert_eq!(parse_int("&H FF"), 255); // spaces after the prefix are skipped
                                             // Modulo-2^32 wrapping, not saturation:
        assert_eq!(parse_int("4294967295"), -1);
        assert_eq!(parse_int("4294967296"), 0);
        assert_eq!(parse_int("4294967298"), 2);
        assert_eq!(parse_int("-4294967295"), 1);
    }

    #[test]
    fn parse_bool_matches_libass() {
        for (input, expected) in [
            ("yes", true),
            ("YES", true),
            (" Yes", true),
            ("yesterday", true), // strncasecmp(str, "yes", 3) prefix quirk
            ("1", true),
            ("2", true),
            ("99999999999999999999", true), // strtol saturates positive
            ("no", false),
            ("0", false),
            ("-1", false),
            ("true", false), // not a VSFilter boolean
            ("", false),
        ] {
            assert_eq!(parse_bool(input), expected, "input {input:?}");
        }
    }

    #[test]
    fn parse_f64_matches_ass_strtod() {
        assert_eq!(parse_f64("18"), 18.0);
        assert_eq!(parse_f64("  \t18.5rest"), 18.5);
        assert_eq!(parse_f64("-3.25"), -3.25);
        assert_eq!(parse_f64(".5"), 0.5);
        assert_eq!(parse_f64("5."), 5.0);
        assert_eq!(parse_f64("1e3"), 1000.0);
        assert_eq!(parse_f64("1.5E+2"), 150.0);
        assert_eq!(parse_f64("1e"), 1.0); // exponent without digits backtracks
        assert_eq!(parse_f64("1e+"), 1.0);
        assert_eq!(parse_f64("."), 0.0);
        assert_eq!(parse_f64("+."), 0.0);
        assert_eq!(parse_f64("abc"), 0.0);
        assert_eq!(parse_f64("0x10"), 0.0); // no hex floats: parses the "0"
    }

    #[test]
    fn parse_timecode_matches_string2timecode() {
        assert_eq!(parse_timecode_ms("0:00:05.00"), Some(5_000));
        assert_eq!(parse_timecode_ms("1:02:03.45"), Some(3_723_450));
        assert_eq!(parse_timecode_ms("10:00:00.999"), Some(36_000_000 + 9_990));
        assert_eq!(parse_timecode_ms("0:00:00.5"), Some(50)); // centiseconds quirk
        assert_eq!(parse_timecode_ms(" 0: 00: 05.00"), Some(5_000)); // %d skips spaces
        assert_eq!(parse_timecode_ms("-1:00:00.00"), Some(-3_600_000));
        assert_eq!(parse_timecode_ms("0:00:05"), None); // all 4 fields required
        assert_eq!(parse_timecode_ms("0 :00:00.00"), None); // literal ':' mismatch
        assert_eq!(parse_timecode_ms("::"), None);
        assert_eq!(parse_timecode_ms("garbage"), None);
    }

    #[test]
    fn parse_color_swaps_file_byte_order() {
        // &HAABBGGRR: white, opaque.
        let white = parse_color("&H00FFFFFF");
        assert_eq!(
            white,
            Color {
                r: 255,
                g: 255,
                b: 255,
                t: 0
            }
        );
        // Half-transparent black.
        assert_eq!(parse_color("&H80000000").packed(), 0x0000_0080);
        // Decimal 16711680 = 0x00FF0000 = BB=FF: pure blue, opaque.
        assert_eq!(
            parse_color("16711680"),
            Color {
                r: 0,
                g: 0,
                b: 255,
                t: 0
            }
        );
        // Trailing '&' form used by most scripts.
        assert_eq!(
            parse_color("&HFF&"),
            Color {
                r: 255,
                g: 0,
                b: 0,
                t: 0
            }
        );
    }

    #[test]
    fn parse_ycbcr_matrix_recognizes_all_names() {
        assert_eq!(parse_ycbcr_matrix(""), YcbcrMatrix::Default);
        assert_eq!(parse_ycbcr_matrix("  \t "), YcbcrMatrix::Default);
        assert_eq!(parse_ycbcr_matrix(" TV.601 "), YcbcrMatrix::Bt601Tv);
        assert_eq!(parse_ycbcr_matrix("pc.709"), YcbcrMatrix::Bt709Pc);
        assert_eq!(parse_ycbcr_matrix("None"), YcbcrMatrix::None);
        assert_eq!(parse_ycbcr_matrix("tv.240M"), YcbcrMatrix::Smpte240mTv);
        assert_eq!(parse_ycbcr_matrix("pc.fcc"), YcbcrMatrix::FccPc);
        assert_eq!(parse_ycbcr_matrix("bt2020"), YcbcrMatrix::Unknown);
    }

    #[test]
    fn numpad2align_matches_ass_utils() {
        // Numpad 1..=9 (bottom/middle/top rows).
        for (numpad, legacy) in [
            (1, 1),
            (2, 2),
            (3, 3),
            (4, 1 | VALIGN_CENTER),
            (5, 2 | VALIGN_CENTER),
            (6, 3 | VALIGN_CENTER),
            (7, 1 | VALIGN_TOP),
            (8, 2 | VALIGN_TOP),
            (9, 3 | VALIGN_TOP),
        ] {
            assert_eq!(numpad2align(numpad), legacy, "numpad {numpad}");
        }
        // Out-of-range quirks preserved from the C code.
        assert_eq!(numpad2align(0), 0);
        assert_eq!(numpad2align(-5), 2 | VALIGN_CENTER);
        assert_eq!(numpad2align(10), 1 | VALIGN_TOP);
        assert_eq!(numpad2align(i32::MIN), 2);
    }
}
