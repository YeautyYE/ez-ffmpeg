//! `FfmpegContext::build()` must be free of output-file side effects: it validates a
//! configuration (and opens/probes the inputs) but must NOT create or truncate the
//! output file. Opening the output is deferred to runtime mux initialization — from
//! the mux worker just before `avformat_write_header` for a streamed output, or from
//! the streamless dispatch for a zero-stream one — so that building a context to
//! validate it, or a build that later fails a check, never clobbers an existing file.
//! Before that fix, `open_output_file` called `avio_open2(AVIO_FLAG_WRITE)` at build
//! time, which opens `O_CREAT|O_TRUNC` and destroyed the target immediately.

use ez_ffmpeg::{FfmpegContext, Input, Output};

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_build_side_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

#[test]
fn build_does_not_truncate_an_existing_output_file() {
    let out = tmp_path("existing.mp4");
    let sentinel = b"PRE-EXISTING CONTENT that build() must leave intact".to_vec();
    std::fs::write(&out, &sentinel).unwrap();

    // A valid, complete config (a finite lavfi source auto-mapped to an mpeg4 mp4
    // output). build() succeeds but the job is never started.
    let ctx = FfmpegContext::builder()
        .input(Input::from("color=c=red:s=64x64:r=15:d=0.2").set_format("lavfi"))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("valid config must build");

    // The output file must still hold its original bytes: build() opened nothing. A
    // build-time avio_open2(AVIO_FLAG_WRITE) would have truncated it to zero.
    let after = std::fs::read(&out).expect("output file must still exist after build()");
    assert_eq!(
        after, sentinel,
        "build() truncated/overwrote the output file; build() must be side-effect-free"
    );

    // Dropping the built-but-never-started context must also leave the file intact.
    drop(ctx);
    let after_drop = std::fs::read(&out).expect("output file must still exist after drop");
    assert_eq!(
        after_drop, sentinel,
        "dropping an unstarted context truncated the output file"
    );
}

/// Runs `ctx` to completion, returning the job result. An open failure may surface
/// either from `start()` (a synchronously-initialized muxer) or from `wait()`, so BOTH
/// run inside the watched thread and are funneled into one `Result` — a hang in either
/// becomes a timeout panic via the watchdog, not an unbounded block.
fn run_to_result(ctx: FfmpegContext, secs: u64) -> ez_ffmpeg::error::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(ctx.start().and_then(|scheduler| scheduler.wait()));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("job did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("wait() thread panicked before reporting")
        }
    }
}

#[test]
fn opening_an_output_in_a_missing_directory_fails_at_run_time() {
    // The output's parent directory does not exist, so `avio_open2` cannot create the
    // file. Because the open is deferred out of build(), build() must SUCCEED, and the
    // failure must surface when the job runs — not as a silent success, and not as a
    // hang. (Before the deferral this failed at build() time; the error type is the
    // same OpenOutput, only the timing moved.)
    let base = std::env::temp_dir().join(format!("ez_ffmpeg_build_side_{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    let parent = base.join("no_such_subdir");
    // A prior run reusing this PID could have left the directory behind; ensure it is
    // truly absent so the open is forced to fail.
    let _ = std::fs::remove_dir_all(&parent);
    assert!(
        !parent.exists(),
        "the output's parent dir must be absent for this test"
    );
    let missing = parent.join("out.mp4");
    let missing_str = missing.to_string_lossy().into_owned();

    let ctx = FfmpegContext::builder()
        .input(Input::from("color=c=red:s=64x64:r=15:d=0.2").set_format("lavfi"))
        .output(Output::from(missing_str.as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("build must succeed even when the output path cannot be opened");

    // The deferred open must fail with the SAME OpenOutput the build path used to
    // return; asserting only is_err() would also pass on an unrelated encoder/header
    // error.
    let result = run_to_result(ctx, 30);
    assert!(
        matches!(result, Err(ez_ffmpeg::error::Error::OpenOutput(_))),
        "a job whose output cannot be opened must fail with OpenOutput at run time: {result:?}"
    );
}

/// A zero-stream (`AVFMT_NOSTREAMS`) file-backed muxer: `ffmetadata` with no input.
/// It skips `_mux_init`, so the `queue_sender == None` branch is the ONLY place it is
/// opened — without that open the file is never created. `set_format` forces the
/// otherwise-inferred format so the streamless path is exercised deterministically.
#[test]
fn streamless_ffmetadata_output_is_created_at_run_time() {
    let out = tmp_path("meta_created.ffmeta");
    let _ = std::fs::remove_file(&out);
    let ctx = FfmpegContext::builder()
        .output(Output::from(out.as_str()).set_format("ffmetadata"))
        .build()
        .expect("streamless ffmetadata build");
    run_to_result(ctx, 30).expect("streamless job runs to completion");
    // The file's existence is the oracle: reverting the streamless-branch open leaves
    // it uncreated.
    assert!(
        std::path::Path::new(&out).exists(),
        "a file-backed streamless output must be created at run time"
    );
}

/// The bad-path prong of the streamless fix: a streamless output whose parent dir is
/// absent must surface `OpenOutput` at run time, not succeed silently.
#[test]
fn streamless_output_open_failure_surfaces_at_run_time() {
    let base = std::env::temp_dir().join(format!("ez_ffmpeg_build_side_{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    let parent = base.join("no_such_subdir_meta");
    let _ = std::fs::remove_dir_all(&parent);
    assert!(!parent.exists());
    let missing = parent.join("out.ffmeta");
    let ctx = FfmpegContext::builder()
        .output(Output::from(missing.to_string_lossy().as_ref()).set_format("ffmetadata"))
        .build()
        .expect("build succeeds; the streamless open is deferred to run time");
    let result = run_to_result(ctx, 30);
    assert!(
        matches!(result, Err(ez_ffmpeg::error::Error::OpenOutput(_))),
        "a streamless output whose parent is missing must fail with OpenOutput: {result:?}"
    );
}
