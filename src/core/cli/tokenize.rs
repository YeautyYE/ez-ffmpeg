//! POSIX word splitting for the single-string convenience form.
//!
//! Contract (documented on [`crate::core::cli::from_cli`]): single/double
//! quotes, backslash escapes, backslash-newline line continuation, plus one
//! explicitly named cmd.exe compatibility extension — caret-newline
//! continuation outside quotes. Everything else shell-flavored is REJECTED,
//! not emulated: variables, command/process substitution, pipes, redirects,
//! background/list operators, comments, tilde expansion, bare newlines.
//! Glob characters (`*`, `?`, `[`) pass through literally (the no-match
//! fallback every one-file ffmpeg command hits in a real shell).
//!
//! The scan walks `char_indices()` — CLI strings contain unicode paths, and
//! byte-indexed slicing is banned in this codebase without a boundary proof.

use super::error::CliError;

/// Splits `command` into argv tokens under the documented contract.
/// A leading `ffmpeg` / `ffmpeg.exe` program token is stripped.
pub(crate) fn tokenize(command: &str) -> Result<Vec<String>, CliError> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    // A quoted empty string ('' / "") must yield an empty token, so track
    // "token started" separately from "token non-empty".
    let mut started = false;
    let mut chars = command.char_indices().peekable();

    let err = |offset: usize, message: &str| -> CliError {
        CliError::Tokenize {
            message: message.to_string(),
            offset,
        }
    };

    while let Some((offset, ch)) = chars.next() {
        match ch {
            '\'' => {
                started = true;
                loop {
                    match chars.next() {
                        Some((_, '\'')) => break,
                        Some((_, inner)) => current.push(inner),
                        None => return Err(err(offset, "unterminated single quote")),
                    }
                }
            }
            '"' => {
                started = true;
                loop {
                    match chars.next() {
                        Some((_, '"')) => break,
                        Some((esc_at, '\\')) => match chars.next() {
                            // POSIX: inside double quotes `\` escapes only
                            // `$` `` ` `` `"` `\` and newline (continuation).
                            Some((_, c @ ('$' | '`' | '"' | '\\'))) => current.push(c),
                            Some((_, '\n')) => {}
                            Some((_, other)) => {
                                current.push('\\');
                                current.push(other);
                            }
                            None => return Err(err(esc_at, "unterminated double quote")),
                        },
                        Some((at, '$')) => check_dollar(&mut chars, at, &mut current)?,
                        Some((at, '`')) => {
                            return Err(err(at, "command substitution (backtick) is a shell construct, not an ffmpeg option"));
                        }
                        Some((_, inner)) => current.push(inner),
                        None => return Err(err(offset, "unterminated double quote")),
                    }
                }
            }
            '\\' => match chars.next() {
                // Line continuation: `\` + newline (with CRLF tolerance for
                // pasted Windows text) disappears.
                Some((_, '\n')) => {}
                Some((_, '\r')) => {
                    if !matches!(chars.peek(), Some((_, '\n'))) {
                        return Err(err(offset, "stray carriage return after backslash"));
                    }
                    chars.next();
                }
                Some((_, escaped)) => {
                    started = true;
                    current.push(escaped);
                }
                None => {
                    return Err(err(
                        offset,
                        "trailing backslash (line continuation with nothing after it)",
                    ))
                }
            },
            // Explicitly named cmd.exe compatibility extension: caret-newline
            // continuation, only outside quotes. Any other caret is literal.
            '^' => match chars.peek() {
                Some((_, '\n')) => {
                    chars.next();
                }
                Some((_, '\r')) => {
                    chars.next();
                    if !matches!(chars.peek(), Some((_, '\n'))) {
                        return Err(err(offset, "stray carriage return after caret"));
                    }
                    chars.next();
                }
                _ => {
                    started = true;
                    current.push('^');
                }
            },
            '\n' => {
                return Err(err(
                    offset,
                    "bare newline; join lines with a trailing backslash (POSIX) or caret (cmd)",
                ))
            }
            c if c.is_whitespace() => {
                // Includes '\r' and '\t'. Token boundary.
                if started {
                    tokens.push(std::mem::take(&mut current));
                    started = false;
                }
            }
            '|' | ';' | '&' | '<' | '>' | '(' | ')' => {
                return Err(err(
                    offset,
                    "shell operator; pipes, redirects, command lists and subshells are not ffmpeg options",
                ));
            }
            '`' => {
                return Err(err(
                    offset,
                    "command substitution (backtick) is a shell construct, not an ffmpeg option",
                ));
            }
            '$' => check_dollar(&mut chars, offset, &mut current).map(|()| started = true)?,
            '#' if !started => {
                return Err(err(
                    offset,
                    "comment start; comments are shell syntax, not part of an ffmpeg command",
                ));
            }
            '~' if !started => {
                return Err(err(
                    offset,
                    "tilde expansion is shell syntax and is not performed; spell the home directory out",
                ));
            }
            other => {
                started = true;
                current.push(other);
            }
        }
    }

    if started {
        tokens.push(current);
    }

    Ok(strip_program_token(tokens))
}

/// `$` handling shared by the unquoted and double-quoted contexts: a `$`
/// introducing an expansion (`$VAR`, `${VAR}`, `$1`, `$@`…) is rejected —
/// the shell would substitute it and this layer must not silently pass the
/// literal through. A `$` no shell would expand (end of word, `$.` etc.)
/// stays literal.
fn check_dollar(
    chars: &mut std::iter::Peekable<std::str::CharIndices<'_>>,
    offset: usize,
    current: &mut String,
) -> Result<(), CliError> {
    let expands = matches!(
        chars.peek(),
        Some((_, c)) if c.is_ascii_alphanumeric()
            || matches!(c, '_' | '{' | '@' | '*' | '#' | '?' | '$' | '!' | '-')
    );
    if expands {
        return Err(CliError::Tokenize {
            message: "shell variable/parameter expansion is not performed; expand the value \
                      yourself or pass an argv slice"
                .to_string(),
            offset,
        });
    }
    current.push('$');
    Ok(())
}

/// Drops a leading `ffmpeg` / `ffmpeg.exe` program token so a pasted command
/// line and a bare option list both work. Also applied to the argv form.
pub(crate) fn strip_program_token(mut tokens: Vec<String>) -> Vec<String> {
    if let Some(first) = tokens.first() {
        if first == "ffmpeg" || first.eq_ignore_ascii_case("ffmpeg.exe") {
            tokens.remove(0);
        }
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::super::error::CliError;
    use super::tokenize;

    fn ok(cmd: &str) -> Vec<String> {
        tokenize(cmd).unwrap_or_else(|e| panic!("expected tokens for {cmd:?}, got: {e}"))
    }

    fn rejected(cmd: &str) -> CliError {
        match tokenize(cmd) {
            Ok(tokens) => panic!("expected a tokenize rejection for {cmd:?}, got {tokens:?}"),
            Err(err) => err,
        }
    }

    fn reject_msg(cmd: &str, needle: &str) {
        let err = rejected(cmd);
        let CliError::Tokenize { message, .. } = &err else {
            panic!("expected CliError::Tokenize for {cmd:?}, got {err:?}");
        };
        assert!(
            message.contains(needle),
            "message for {cmd:?} should mention {needle:?}: {message}"
        );
    }

    #[test]
    fn splits_on_runs_of_whitespace() {
        assert_eq!(
            ok("-i  in.mp4\t-y   out.mp4"),
            ["-i", "in.mp4", "-y", "out.mp4"]
        );
    }

    #[test]
    fn strips_leading_ffmpeg_token() {
        assert_eq!(
            ok("ffmpeg -i in.mp4 -y out.mp4"),
            ["-i", "in.mp4", "-y", "out.mp4"]
        );
        assert_eq!(ok("FFmpeg.EXE -y"), ["-y"]);
    }

    #[test]
    fn keeps_ffmpeg_when_not_leading() {
        assert_eq!(
            ok("-i ffmpeg -y out.mp4"),
            ["-i", "ffmpeg", "-y", "out.mp4"]
        );
    }

    #[test]
    fn single_quotes_group_and_keep_specials_literal() {
        assert_eq!(ok(r#"-i 'my movie.mp4'"#), ["-i", "my movie.mp4"]);
        assert_eq!(ok(r#"'a$b' 'seg_%03d.ts'"#), ["a$b", "seg_%03d.ts"]);
    }

    #[test]
    fn double_quotes_group_words() {
        assert_eq!(ok(r#"-i "my movie.mp4""#), ["-i", "my movie.mp4"]);
    }

    #[test]
    fn double_quote_backslash_escapes_posix_set_only() {
        // \" and \\ collapse; \s is NOT an escape inside double quotes.
        assert_eq!(ok(r#""a\"b" "c\\d" "e\sf""#), [r#"a"b"#, r"c\d", r"e\sf"]);
    }

    #[test]
    fn unquoted_backslash_escapes_next_char() {
        assert_eq!(ok(r"my\ movie.mp4"), ["my movie.mp4"]);
        assert_eq!(ok(r"\$HOME"), ["$HOME"]);
    }

    #[test]
    fn quoted_empty_string_is_a_token() {
        assert_eq!(ok("-metadata ''"), ["-metadata", ""]);
        assert_eq!(ok(r#"-metadata """#), ["-metadata", ""]);
    }

    #[test]
    fn adjacent_quoted_segments_join_one_token() {
        assert_eq!(ok(r#"seg'_'"%03d".ts"#), ["seg_%03d.ts"]);
    }

    #[test]
    fn unicode_paths_survive() {
        assert_eq!(
            ok("-i 视频文件.mp4 -y 输出【终】.mp4"),
            ["-i", "视频文件.mp4", "-y", "输出【终】.mp4"]
        );
    }

    #[test]
    fn unicode_inside_quotes_survives() {
        assert_eq!(
            ok("-i '目录 一/クリップ.mov'"),
            ["-i", "目录 一/クリップ.mov"]
        );
    }

    #[test]
    fn backslash_newline_continues_the_line() {
        assert_eq!(
            ok("-i in.mp4 \\\n-y out.mp4"),
            ["-i", "in.mp4", "-y", "out.mp4"]
        );
    }

    #[test]
    fn backslash_crlf_continues_the_line() {
        assert_eq!(
            ok("-i in.mp4 \\\r\n-y out.mp4"),
            ["-i", "in.mp4", "-y", "out.mp4"]
        );
    }

    #[test]
    fn caret_newline_continues_the_line() {
        assert_eq!(
            ok("-i in.mp4 ^\n-y out.mp4"),
            ["-i", "in.mp4", "-y", "out.mp4"]
        );
        assert_eq!(
            ok("-i in.mp4 ^\r\n-y out.mp4"),
            ["-i", "in.mp4", "-y", "out.mp4"]
        );
    }

    #[test]
    fn caret_inside_quotes_is_literal() {
        assert_eq!(ok("-i 'a^b.mp4'"), ["-i", "a^b.mp4"]);
    }

    #[test]
    fn caret_mid_token_is_literal() {
        assert_eq!(ok("-i a^b.mp4"), ["-i", "a^b.mp4"]);
    }

    #[test]
    fn continuation_splices_tokens_without_a_space() {
        // POSIX: `in\<newline>put` is one word "input".
        assert_eq!(ok("-i in\\\nput.mp4"), ["-i", "input.mp4"]);
    }

    #[test]
    fn bare_newline_rejected() {
        reject_msg("-i in.mp4\n-y out.mp4", "bare newline");
    }

    #[test]
    fn newline_inside_single_quotes_is_data() {
        assert_eq!(ok("-i 'a\nb.mp4'"), ["-i", "a\nb.mp4"]);
    }

    #[test]
    fn unterminated_quotes_rejected() {
        reject_msg("-i 'in.mp4", "unterminated single quote");
        reject_msg(r#"-i "in.mp4"#, "unterminated double quote");
    }

    #[test]
    fn trailing_backslash_rejected() {
        reject_msg("-i in.mp4 \\", "trailing backslash");
    }

    #[test]
    fn pipes_and_redirects_rejected() {
        reject_msg("-i in.mp4 out.mp4 | cat", "shell operator");
        reject_msg("-i in.mp4 out.mp4 2>&1", "shell operator");
        reject_msg("-i in.mp4 > log.txt", "shell operator");
        reject_msg("-i in.mp4 < x", "shell operator");
    }

    #[test]
    fn command_lists_and_subshells_rejected() {
        reject_msg("-i a.mp4 out.mp4 ; rm x", "shell operator");
        reject_msg("-i a.mp4 out.mp4 && echo done", "shell operator");
        reject_msg("(cd /tmp)", "shell operator");
    }

    #[test]
    fn variable_expansion_rejected_unquoted_and_double_quoted() {
        reject_msg("-i $SRC -y out.mp4", "expansion");
        reject_msg("-i ${SRC} -y out.mp4", "expansion");
        reject_msg(r#"-i "$SRC" -y out.mp4"#, "expansion");
        reject_msg("-i $1", "expansion");
    }

    #[test]
    fn dollar_in_single_quotes_is_literal() {
        assert_eq!(ok("-i '$SRC'"), ["-i", "$SRC"]);
    }

    #[test]
    fn lone_dollar_is_literal() {
        assert_eq!(ok("-i a$.mp4"), ["-i", "a$.mp4"]);
    }

    #[test]
    fn backticks_rejected() {
        reject_msg("-i `ls`", "backtick");
        reject_msg(r#"-i "`ls`""#, "backtick");
    }

    #[test]
    fn comment_start_rejected_but_mid_token_hash_kept() {
        reject_msg("-i in.mp4 # comment", "comment");
        assert_eq!(ok("-i seg#1.mp4"), ["-i", "seg#1.mp4"]);
    }

    #[test]
    fn tilde_expansion_rejected_but_mid_token_tilde_kept() {
        reject_msg("-i ~/in.mp4", "tilde");
        assert_eq!(ok("-i a~b.mp4"), ["-i", "a~b.mp4"]);
    }

    #[test]
    fn glob_characters_pass_through_literally() {
        // The documented no-match fallback: ffmpeg option grammar owns these.
        assert_eq!(
            ok("-map 0:a:1? -vf scale=iw*2:ih"),
            ["-map", "0:a:1?", "-vf", "scale=iw*2:ih"]
        );
    }

    #[test]
    fn double_dash_token_passes_through() {
        // Classification (and rejection) happens in the parser, not here.
        assert_eq!(ok("-- -y"), ["--", "-y"]);
    }

    #[test]
    fn empty_and_blank_commands_yield_no_tokens() {
        assert_eq!(ok(""), Vec::<String>::new());
        assert_eq!(ok("   \t "), Vec::<String>::new());
        assert_eq!(ok("ffmpeg"), Vec::<String>::new());
    }
}
