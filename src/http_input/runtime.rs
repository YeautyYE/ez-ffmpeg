//! Per-client current-thread Tokio runtime and command channel.

use crate::http_input::error::HttpInputError;
use crate::http_input::stream::{run_job, StreamJob};
use std::sync::Arc;
use std::thread::JoinHandle;

pub(crate) enum RuntimeCmd {
    Run(Box<StreamJob>),
    Shutdown,
}

/// Cheap handle cloned into each AVIO state.
#[derive(Clone)]
pub(crate) struct RuntimeHandle {
    cmd_tx: tokio::sync::mpsc::UnboundedSender<RuntimeCmd>,
    // Only the owning HttpClientInner joins the thread.
    thread: Arc<MutexJoin>,
}

struct MutexJoin {
    thread: std::sync::Mutex<Option<JoinHandle<()>>>,
}

impl RuntimeHandle {
    pub(crate) fn start() -> Result<Self, HttpInputError> {
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("ez-ffmpeg-http".into())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .enable_time()
                    .build()
                {
                    Ok(rt) => {
                        let _ = ready_tx.send(Ok(()));
                        rt
                    }
                    Err(e) => {
                        let _ = ready_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                rt.block_on(async move {
                    while let Some(cmd) = cmd_rx.recv().await {
                        match cmd {
                            RuntimeCmd::Run(job) => {
                                tokio::spawn(run_job(*job));
                            }
                            RuntimeCmd::Shutdown => break,
                        }
                    }
                });
            })
            .map_err(|e| HttpInputError::Transport {
                message: e.to_string(),
            })?;

        match ready_rx.recv() {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => {
                return Err(HttpInputError::Transport { message: msg });
            }
            Err(_) => {
                return Err(HttpInputError::Transport {
                    message: "http runtime thread exited during start".into(),
                });
            }
        }

        Ok(Self {
            cmd_tx,
            thread: Arc::new(MutexJoin {
                thread: std::sync::Mutex::new(Some(thread)),
            }),
        })
    }

    pub(crate) fn submit(&self, job: StreamJob) -> Result<(), HttpInputError> {
        self.cmd_tx
            .send(RuntimeCmd::Run(Box::new(job)))
            .map_err(|_| HttpInputError::Transport {
                message: "http runtime is gone".into(),
            })
    }
}

impl Drop for MutexJoin {
    fn drop(&mut self) {
        // Best-effort: the channel sender in RuntimeHandle is already gone
        // when the last handle drops, so the loop exits on cmd_rx close.
        //
        // Join through a helper thread with a bounded wait instead of an
        // unbounded `join()`: the runtime thread may still be draining a
        // stuck job (e.g. a peer that never resets the connection), and the
        // caller dropping the last client must not hang forever. On timeout
        // both the runtime thread and the helper are detached; they exit on
        // their own once the pending I/O resolves, and the process can still
        // terminate normally because detached threads do not block exit.
        if let Some(thread) = self.thread.lock().unwrap_or_else(|e| e.into_inner()).take() {
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let waiter = std::thread::Builder::new()
                .name("ez-ffmpeg-http-join".into())
                .spawn(move || {
                    let _ = thread.join();
                    let _ = done_tx.send(());
                });
            if waiter.is_ok() {
                let _ = done_rx.recv_timeout(std::time::Duration::from_secs(2));
            }
        }
    }
}

impl Drop for RuntimeHandle {
    fn drop(&mut self) {
        if Arc::strong_count(&self.thread) == 1 {
            let _ = self.cmd_tx.send(RuntimeCmd::Shutdown);
        }
    }
}
