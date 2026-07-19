//! `FfmpegContext::build()` must not initialize the filters of a
//! `filter_desc`: the build-time probe parses and creates them to discover
//! the graph's open pads, but running their `init` belongs to the job. Init
//! is where filter side effects live — `movie=` opens its file, model- and
//! font-loading filters read their resources, and destination-writing filters
//! (`metadata=file=...`) create or truncate their target. Before the fix the
//! probe went through `avfilter_graph_segment_apply`, which runs init, so all
//! of that happened at build() time and a filter's file/model errors aborted
//! build() for a job that was never started.
//!
//! The flip side stays guaranteed: everything checkable WITHOUT init still
//! fails at build() — malformed syntax, unknown filter names, unknown
//! options, dangling link labels, more labels than pads.

use ez_ffmpeg::{FfmpegContext, Input, Output};

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_build_probe_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// Embeds a filesystem path in a `filter_desc`. Filtergraph text is parsed
/// twice — the graph parser and the filter's option parser each consume one
/// level of `\` escapes — so the `:` after a Windows drive letter needs a
/// doubled escape. Separators are normalized to `/`, which Windows file APIs
/// accept. On Unix paths this is the identity.
fn graph_path(path: &str) -> String {
    path.replace('\\', "/").replace(':', "\\\\:")
}

fn video_input() -> Input {
    Input::from("color=c=red:s=64x64:r=15:d=0.2").set_format("lavfi")
}

fn audio_input() -> Input {
    Input::from("sine=frequency=440:duration=0.2").set_format("lavfi")
}

/// Runs `ctx` to completion. `start()` and `wait()` both run inside the
/// watched thread so a failure from either surfaces as one `Result` and a
/// hang becomes a timeout panic instead of an unbounded block.
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
fn missing_movie_source_builds_and_fails_at_run_time() {
    // `movie=` opens its file in init. The probe must not init, so a missing
    // file cannot fail build(); the error must surface from the runtime graph
    // configuration, reaching wait() as a typed FilterGraph error — not as a
    // silent success and not as a hang. (Before the fix this failed build().)
    let missing = tmp_path("no_such_movie.mkv");
    let _ = std::fs::remove_file(&missing);
    assert!(!std::path::Path::new(&missing).exists());

    let ctx = FfmpegContext::builder()
        .input(video_input())
        .filter_desc(format!(
            "movie={}[wm];[0:v][wm]overlay[out]",
            graph_path(&missing)
        ))
        .output(Output::from(tmp_path("movie_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("build() must not open the movie source");

    let result = run_to_result(ctx, 30);
    assert!(
        matches!(result, Err(ez_ffmpeg::error::Error::FilterGraph(_))),
        "a missing movie source must fail the RUNNING job with a FilterGraph error: {result:?}"
    );
}

#[test]
fn build_does_not_create_or_truncate_a_filter_destination_file() {
    // `metadata=mode=print:file=X` opens X for writing (O_CREAT|O_TRUNC) in
    // its init. build() must leave the destination alone: a pre-existing file
    // keeps its bytes, both after build() and after dropping the unstarted
    // context. Only a started job may open it.
    let dest = tmp_path("metadata_dest.txt");
    let sentinel = b"PRE-EXISTING CONTENT that build() must leave intact".to_vec();
    std::fs::write(&dest, &sentinel).unwrap();

    let ctx = FfmpegContext::builder()
        .input(video_input())
        .filter_desc(format!(
            "[0:v]metadata=mode=print:file={}[out]",
            graph_path(&dest)
        ))
        .output(Output::from(tmp_path("metadata_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("valid config must build");

    let after = std::fs::read(&dest).expect("destination must still exist after build()");
    assert_eq!(
        after, sentinel,
        "build() truncated a filter's destination file; filter init must not run at build time"
    );

    drop(ctx);
    let after_drop = std::fs::read(&dest).expect("destination must still exist after drop");
    assert_eq!(
        after_drop, sentinel,
        "dropping an unstarted context truncated a filter's destination file"
    );
}

#[test]
fn unknown_filter_name_still_fails_build() {
    let err = FfmpegContext::builder()
        .input(video_input())
        .filter_desc("nosuchfilter_ez_probe")
        .output(Output::from(tmp_path("unknown_filter.mp4").as_str()))
        .build()
        .err();
    assert!(
        matches!(err, Some(ez_ffmpeg::error::Error::FilterGraphParse(_))),
        "an unknown filter name must fail build(): {err:?}"
    );
}

#[test]
fn malformed_filter_desc_still_fails_build() {
    let err = FfmpegContext::builder()
        .input(video_input())
        .filter_desc("scale=320:240[unclosed")
        .output(Output::from(tmp_path("malformed.mp4").as_str()))
        .build()
        .err();
    assert!(
        matches!(err, Some(ez_ffmpeg::error::Error::FilterGraphParse(_))),
        "malformed filter_desc syntax must fail build(): {err:?}"
    );
}

#[test]
fn unknown_filter_option_still_fails_build() {
    let err = FfmpegContext::builder()
        .input(video_input())
        .filter_desc("scale=no_such_option=1")
        .output(Output::from(tmp_path("badopt.mp4").as_str()))
        .build()
        .err();
    assert!(
        matches!(err, Some(ez_ffmpeg::error::Error::FilterGraphParse(_))),
        "an unknown filter option must fail build(): {err:?}"
    );
}

#[test]
fn dangling_input_label_still_fails_build() {
    // [nolabel] matches no filter output and no stream specifier, so binding
    // the open pad fails — at build(), as before.
    let err = FfmpegContext::builder()
        .input(video_input())
        .filter_desc("[nolabel]scale=64:64[out]")
        .output(Output::from(tmp_path("dangling.mp4").as_str()))
        .build()
        .err();
    assert!(
        err.is_some(),
        "a dangling input link label must fail build()"
    );
}

#[test]
fn too_many_input_labels_still_fail_build() {
    // hflip has exactly one input; two labels is the arity error graphparser
    // raises in its link pass, which the probe mirrors without init.
    let err = FfmpegContext::builder()
        .input(video_input())
        .filter_desc("[0:v][1:v]hflip[out]")
        .output(Output::from(tmp_path("arity.mp4").as_str()))
        .build()
        .err();
    assert!(
        matches!(err, Some(ez_ffmpeg::error::Error::FilterGraphParse(_))),
        "more labels than pads must fail build(): {err:?}"
    );
}

#[test]
fn existing_movie_source_still_builds_and_runs() {
    // Positive prong of the movie deferral: with a real file the probed
    // topology (movie's output consumed by overlay, no inference needed for
    // open pads) must produce a graph the runtime parse accepts and runs.
    let fixture = tmp_path("movie_fixture.mp4");
    run_to_result(
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=32x32:r=15:d=0.2").set_format("lavfi"))
            .output(Output::from(fixture.as_str()).set_video_codec("mpeg4"))
            .build()
            .expect("fixture build"),
        30,
    )
    .expect("fixture job");

    let ctx = FfmpegContext::builder()
        .input(video_input())
        .filter_desc(format!(
            "movie={}[wm];[0:v][wm]overlay[out]",
            graph_path(&fixture)
        ))
        .output(Output::from(tmp_path("movie_ok_out.mp4").as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("movie graph with an existing source must build");
    run_to_result(ctx, 30).expect("movie graph with an existing source must run to completion");
}

#[test]
fn split_overlay_graph_still_builds_and_runs() {
    // Regression: a multi-chain graph with a dynamic-output filter (split)
    // must keep working end to end on the probed topology.
    let ctx = FfmpegContext::builder()
        .input(video_input())
        .filter_desc("[0:v]split[a][b];[a]hflip[x];[x][b]overlay[out]")
        .output(Output::from(tmp_path("split_overlay.mp4").as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("split/overlay graph must build");
    run_to_result(ctx, 30).expect("split/overlay graph must run to completion");
}

#[test]
fn amix_graph_still_builds_and_runs() {
    // Regression: a dynamic-INPUT filter (amix) fed by two inputs; its pads
    // only exist because the probe initializes topology-shaping filters.
    let ctx = FfmpegContext::builder()
        .input(audio_input())
        .input(audio_input())
        .filter_desc("[0:a][1:a]amix=inputs=2[out]")
        .output(Output::from(tmp_path("amix.wav").as_str()).set_audio_codec("pcm_s16le"))
        .build()
        .expect("amix graph must build");
    run_to_result(ctx, 30).expect("amix graph must run to completion");
}
