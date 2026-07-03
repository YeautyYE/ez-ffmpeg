use std::sync::{Arc, Condvar, Mutex};

#[derive(Clone)]
pub struct ThreadSynchronizer {
    inner: Arc<Inner>,
}

struct Inner {
    counter: Mutex<usize>,
    condvar: Condvar,
    #[cfg(feature = "async")]
    waker: Mutex<Option<std::task::Waker>>,
}

impl ThreadSynchronizer {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                counter: Mutex::new(0),
                condvar: Condvar::new(),
                #[cfg(feature = "async")]
                waker: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn thread_start(&self) {
        let mut counter = self.inner.counter.lock().unwrap();
        *counter += 1;
    }

    /// Decrements the counter; when this was the last thread, runs `on_last`
    /// BEFORE waking any waiter. Publishing the terminal state inside this
    /// window is what makes a woken wait()/poll() observe it: waking first
    /// and storing afterwards loses the (already consumed) async waker and
    /// leaves the future pending forever.
    pub(crate) fn thread_done_with(&self, on_last: impl FnOnce()) {
        let mut counter = self.inner.counter.lock().unwrap();
        *counter -= 1;

        if *counter == 0 {
            on_last();
            self.inner.condvar.notify_one();

            #[cfg(feature = "async")]
            if let Some(waker) = self.inner.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    pub(crate) fn wait_for_all_threads(&self) {
        let mut counter = self.inner.counter.lock().unwrap();
        while *counter > 0 {
            counter = self.inner.condvar.wait(counter).unwrap();
        }
    }

    #[cfg(feature = "async")]
    pub(crate) fn set_waker(&self, waker: std::task::Waker) {
        let mut waker_slot = self.inner.waker.lock().unwrap();
        *waker_slot = Some(waker);
    }
}

/// RAII slot holder for a scheduler-tracked thread.
///
/// The slot must be claimed with `thread_start()` BEFORE the thread is
/// spawned (claiming inside the thread races stop()/wait() into observing a
/// zero counter). The guard adopts that pre-claimed slot and releases it on
/// drop — including on panic, which a manual `thread_done()` call misses.
/// The last released slot publishes STATUS_END before waking any waiter.
pub(crate) struct ThreadDoneGuard {
    thread_sync: ThreadSynchronizer,
    scheduler_status: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl ThreadDoneGuard {
    pub(crate) fn adopt(
        thread_sync: ThreadSynchronizer,
        scheduler_status: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) -> Self {
        Self {
            thread_sync,
            scheduler_status,
        }
    }
}

impl Drop for ThreadDoneGuard {
    fn drop(&mut self) {
        let status = &self.scheduler_status;
        self.thread_sync.thread_done_with(|| {
            status.store(
                crate::core::scheduler::ffmpeg_scheduler::STATUS_END,
                std::sync::atomic::Ordering::Release,
            );
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn on_last_runs_before_waiters_wake() {
        let sync = ThreadSynchronizer::new();
        sync.thread_start();

        let flag = Arc::new(AtomicBool::new(false));
        let sync_clone = sync.clone();
        let flag_clone = Arc::clone(&flag);
        let handle = thread::spawn(move || {
            sync_clone.wait_for_all_threads();
            flag_clone.load(Ordering::Acquire)
        });

        thread::sleep(Duration::from_millis(50));

        let flag_clone = Arc::clone(&flag);
        sync.thread_done_with(move || flag_clone.store(true, Ordering::Release));

        assert!(
            handle.join().unwrap(),
            "the last-thread closure must run before any waiter wakes"
        );
    }
}
