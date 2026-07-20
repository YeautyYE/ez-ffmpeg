//! Typed diagnostics for the CLI-compat layer.
//!
//! Every rejection is anchored to the offending token (its argv index and
//! text), explains why the token is outside the supported subset, and ends
//! with the static call-to-action line — a rejected command must never fail
//! silently or vaguely.

/// Static call-to-action appended to every subset rejection.
pub(crate) const CTA: &str =
    "want this supported? open an issue: https://github.com/YeautyYE/ez-ffmpeg/issues";

/// ` (token #N)` when the position is known, empty otherwise.
fn fmt_at(index: &Option<usize>) -> String {
    match index {
        Some(index) => format!(" (token #{index})"),
        None => String::new(),
    }
}

/// Where in the command an option was found (positional scoping: options
/// apply to the next file on the command line).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CliScope {
    /// Before any file: applies to the whole run (e.g. `-y`).
    Global,
    /// Before `-i`: applies to the input file.
    Input,
    /// After the input, before the output path: applies to the output file.
    Output,
    /// After the output path: in ffmpeg grammar an option here would apply
    /// to a FOLLOWING output file, which the single-output subset does not
    /// have.
    AfterOutput,
}

impl std::fmt::Display for CliScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliScope::Global => write!(f, "global scope"),
            CliScope::Input => write!(f, "input #0"),
            CliScope::Output => write!(f, "output #0"),
            CliScope::AfterOutput => {
                write!(f, "after output #0 (would apply to a following output)")
            }
        }
    }
}

/// Error type of the CLI-compat entry points ([`from_cli_args`],
/// [`from_cli`], [`emit_rust_code`], [`emit_rust_code_from_args`]).
///
/// The subset contract: every argv token must classify against the
/// compatibility manifest or the whole command is rejected with one of these
/// variants — the layer never guesses, approximates, or silently drops a
/// token.
///
/// [`from_cli_args`]: crate::core::cli::from_cli_args
/// [`from_cli`]: crate::core::cli::from_cli
/// [`emit_rust_code`]: crate::core::cli::emit_rust_code
/// [`emit_rust_code_from_args`]: crate::core::cli::emit_rust_code_from_args
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum CliError {
    /// The single-string form could not be tokenized under the documented
    /// POSIX word-splitting contract (shell constructs, unterminated quotes,
    /// bare newlines, trailing backslash…). `offset` is the byte offset of
    /// the offending character in the original string.
    #[error("tokenize error at byte {offset}: {message}\n  the string form implements POSIX word splitting only — no variables, globs, tilde, pipes, redirects, comments or command lists; pass an argv slice to from_cli_args to sidestep shell quoting entirely\n  {CTA}")]
    Tokenize { message: String, offset: usize },

    /// A token in option position did not classify into the supported option
    /// table. `reason` explains the status (unknown, documented gap, alias,
    /// per-stream indexed variant, …); `hint` may name the nearest supported
    /// spelling.
    #[error("unsupported option `{option}` (token #{index}, {scope})\n  {reason}{}\n  {CTA}", .hint.as_deref().map(|h| format!("\n  {h}")).unwrap_or_default())]
    UnsupportedOption {
        option: String,
        index: usize,
        scope: CliScope,
        reason: String,
        hint: Option<String>,
    },

    /// The option is supported but this value form is not.
    #[error("unsupported value `{value}` for `{option}` (token #{index})\n  {reason}\n  {CTA}")]
    UnsupportedValue {
        option: String,
        value: String,
        index: usize,
        reason: String,
    },

    /// The command's structure is outside the subset (missing/duplicated
    /// files, non-canonical ordering, trailing tokens, …). This is a subset
    /// limitation, not necessarily invalid ffmpeg syntax.
    #[error("unsupported command layout at token #{index} (`{token}`)\n  {reason}\n  {CTA}")]
    UnsupportedLayout {
        token: String,
        index: usize,
        reason: String,
    },

    /// Two options that cannot coexist (e.g. `-t` and `-to` in the same
    /// scope, `-map` together with `-vf`). The indexes anchor each side's
    /// first occurrence in the argv when known.
    #[error("conflicting options `{first}`{} and `{second}`{}\n  {reason}\n  {CTA}", fmt_at(.first_index), fmt_at(.second_index))]
    ConflictingOptions {
        first: String,
        second: String,
        first_index: Option<usize>,
        second_index: Option<usize>,
        reason: String,
    },

    /// The command has no `-y`. The CLI would prompt interactively before
    /// overwriting; this crate always opens outputs with O_CREAT|O_TRUNC and
    /// has no prompt, so a command without `-y` cannot be run faithfully.
    #[error("missing mandatory `-y`\n  without -y the ffmpeg CLI prompts before overwriting an existing output; this library always creates/truncates the output and cannot reproduce that prompt, so the subset requires an explicit -y\n  {CTA}")]
    MissingOverwriteFlag,

    /// Every token classified, but the command shape is not in the verified
    /// set of the compatibility manifest — runtime execution is refused.
    /// The emitters still accept this command and label their output as
    /// unverified scaffolding.
    #[error("command shape is not verified for execution\n  parsed options: [{}]\n  only shapes backed by a semantic golden may run; use emit_rust_code / emit_rust_code_from_args to generate unverified scaffolding code instead\n  {CTA}", .parsed_options.join(", "))]
    NotVerified { parsed_options: Vec<String> },

    /// Every token classified, but the command's option-set fingerprint
    /// matches neither a verified shape nor a documented emit-only entry.
    /// The manifest enumerates its emit surface explicitly — arbitrary
    /// combinations are rejected, not silently scaffolded.
    #[error("command shape is not in the compatibility manifest\n  parsed options: [{}]\n  neither a verified shape nor a documented emit-only entry; nothing is generated for unenumerated shapes\n  {CTA}", .parsed_options.join(", "))]
    UnmatchedShape { parsed_options: Vec<String> },

    /// A `-vf` command's source stream is not structurally unique: the
    /// opened input carries more or fewer than exactly one video stream.
    /// The CLI would score-select one stream and filter it; the subset runs
    /// the filter only when no selection is involved at all (the hard
    /// simple-filter prerequisite), so ambiguous inputs are rejected after
    /// probing instead of silently filtering a chosen stream.
    #[error("-vf requires an input with exactly one video stream; this input has {video_streams}\n  the ffmpeg CLI would score-select one stream to filter; the subset only executes filters over a structurally unique source\n  {CTA}")]
    AmbiguousFilterSource { video_streams: usize },

    /// The linked FFmpeg libraries are not one of the verified runtime
    /// profiles. Raised before any I/O.
    #[error("linked FFmpeg is not a verified runtime profile\n  linked: libavcodec {linked_avcodec}, libavformat {linked_avformat}; verified profiles: {verified}\n  emit_rust_code still works — only in-process execution is gated\n  {CTA}")]
    UnverifiedRuntimeProfile {
        linked_avcodec: String,
        linked_avformat: String,
        verified: String,
    },

    /// The command classified and its shape is verified, but building the
    /// pipeline failed (I/O, codec availability, filter validation…). This
    /// wraps the crate's own typed error.
    #[error("building the pipeline failed: {0}")]
    Build(#[from] crate::error::Error),
}
