//! Shared helpers for the integration-test binaries (`shortest.rs`,
//! `streamcopy_deadlock.rs`). Cargo compiles `tests/common/mod.rs` into each
//! test binary as an ordinary module (files under a `tests/` subdirectory are
//! NOT collected as their own test binary), so this is the idiomatic home for
//! helpers shared across integration tests.
#![allow(dead_code)]

use ez_ffmpeg::FfmpegScheduler;
use std::time::Duration;

/// Build a per-binary temp file path. `subdir` namespaces one test binary's
/// scratch files from another's (and labels them under `$TMPDIR` for debugging);
/// `std::process::id()` isolates concurrent runs. Each caller passes its own
/// `subdir`, so the two binaries keep their distinct temp directories.
pub fn tmp_path_in(subdir: &str, name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("{subdir}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// A hang is a test failure, not a suite timeout. Runs `scheduler.wait()` on a
/// side thread and turns a `secs`-second stall into a `panic!` naming `scenario`,
/// so a deadlock surfaces here instead of stalling the whole suite.
pub fn wait_with_watchdog(
    scheduler: FfmpegScheduler<ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running>,
    secs: u64,
    scenario: &str,
) -> ez_ffmpeg::error::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.wait());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: wait() thread panicked before reporting")
        }
    }
}
