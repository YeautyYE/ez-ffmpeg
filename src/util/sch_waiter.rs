//! Port of fftools `SchWaiter` (FFmpeg 7.x fftools/ffmpeg_sched.c): the
//! choke/unchoke gate the scheduler uses to pace demuxers (fftools also
//! gates filtergraph sources with it; ez chokes only demuxers). Field names
//! (`choked`, `choked_prev`, `choked_next`) and the wait/set pair mirror
//! `waiter_wait`/`waiter_set`; the bounded `wait_timeout` loop is an ez
//! addition so a worker that dies without notifying cannot strand a waiter.

use ffmpeg_sys_next::av_gettime_relative;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// A synchronization primitive that allows a thread to wait until a condition becomes false.
///
/// This struct is used to avoid busy-waiting when a resource is not available.  Threads can
/// call `wait` which will block until another thread calls `set` with `false`.
pub(crate) struct SchWaiter {
    /// A mutex protecting the `Condvar`.
    lock: Mutex<()>,
    /// A condition variable used for waiting.
    cond: Condvar,
    /// An atomic boolean indicating whether the waiter is "choked" (should wait).
    choked: AtomicBool,
    choked_prev: AtomicBool,
    choked_next: AtomicBool,
}

impl Default for SchWaiter {
    fn default() -> Self {
        Self::new()
    }
}

impl SchWaiter {
    /// Creates a new `SchWaiter`.
    pub(crate) fn new() -> Self {
        Self {
            lock: Mutex::new(()),
            cond: Condvar::new(),
            choked: AtomicBool::new(false),
            choked_prev: AtomicBool::new(false),
            choked_next: AtomicBool::new(false),
        }
    }

    /// Waits until the `choked` condition becomes false.
    ///
    /// If `choked` is initially false, this function returns immediately. Otherwise, it
    /// acquires the lock, and waits on the condition variable until `choked` becomes false.
    #[allow(dead_code)]
    pub(crate) fn wait(&self) {
        // early return
        if !self.choked.load(Ordering::Acquire) {
            return;
        }

        let mut guard = self.lock.lock().unwrap();
        // avoid spurious wakeup
        while self.choked.load(Ordering::Acquire) {
            guard = self.cond.wait(guard).unwrap();
        }
    }

    pub(crate) fn get_choked(&self) -> bool {
        self.choked.load(Ordering::Acquire)
    }

    pub(crate) fn wait_with_scheduler_status(
        &self,
        scheduler_status: &Arc<AtomicUsize>,
        cal_wait_time: bool,
    ) -> i64 {
        use crate::core::scheduler::ffmpeg_scheduler::is_stopping;

        // early return
        if !self.choked.load(Ordering::Acquire) {
            return 0;
        }
        // Both terminal states release the waiter: abort() publishes
        // STATUS_ABORT, not STATUS_END.
        if is_stopping(scheduler_status.load(Ordering::Acquire)) {
            return 0;
        }

        let start = if cal_wait_time {
            unsafe { av_gettime_relative() }
        } else {
            0
        };
        let mut guard = self.lock.lock().unwrap();
        // Bounded waits instead of a pure condvar sleep: a worker that
        // fails publishes the terminal status via set_scheduler_error but
        // has no handle to notify this waiter — a pure wait would sleep
        // through the shutdown and deadlock the join. 100ms matches the
        // polling cadence of every other worker loop.
        while self.choked.load(Ordering::Acquire)
            && !is_stopping(scheduler_status.load(Ordering::Acquire))
        {
            let (g, _timeout) = self
                .cond
                .wait_timeout(guard, std::time::Duration::from_millis(100))
                .unwrap();
            guard = g;
        }

        if cal_wait_time {
            unsafe { av_gettime_relative() - start }
        } else {
            0
        }
    }

    /// Sets the `choked` condition to the specified value and notifies a waiting thread.
    ///
    /// This function acquires the lock, updates the `choked` value, and notifies a single
    /// waiting thread (if any).
    pub(crate) fn set(&self, choked: bool) {
        let _guard = self.lock.lock().unwrap();
        self.choked.store(choked, Ordering::Release);
        self.cond.notify_one();
    }

    pub(crate) fn set_choked_prev(&self, value: bool) {
        self.choked_prev.store(value, Ordering::Release);
    }

    pub(crate) fn get_choked_prev(&self) -> bool {
        self.choked_prev.load(Ordering::Acquire)
    }

    pub(crate) fn set_choked_next(&self, value: bool) {
        self.choked_next.store(value, Ordering::Release);
    }

    pub(crate) fn get_choked_next(&self) -> bool {
        self.choked_next.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_wait_when_not_choked() {
        let waiter = Arc::new(SchWaiter::new());
        let waiter_clone = Arc::clone(&waiter);

        let handle = thread::spawn(move || {
            waiter_clone.wait();
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_wait_when_choked() {
        let waiter = Arc::new(SchWaiter::new());
        let waiter_clone = Arc::clone(&waiter);

        let handle = thread::spawn(move || {
            waiter_clone.wait();
        });

        thread::sleep(Duration::from_millis(100));

        waiter.set(false);

        handle.join().unwrap();
    }

    #[test]
    fn test_set_choked() {
        let waiter = Arc::new(SchWaiter::new());
        let waiter_clone = Arc::clone(&waiter);

        waiter.set(true);

        let handle = thread::spawn(move || {
            waiter_clone.wait();
        });

        thread::sleep(Duration::from_millis(100));

        waiter.set(false);

        handle.join().unwrap();
    }

    #[test]
    fn wait_with_scheduler_status_releases_on_abort() {
        use crate::core::scheduler::ffmpeg_scheduler::{STATUS_ABORT, STATUS_RUN};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::mpsc;

        let waiter = Arc::new(SchWaiter::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        waiter.set(true);

        let (tx, rx) = mpsc::channel();
        let waiter_clone = Arc::clone(&waiter);
        let status_clone = Arc::clone(&status);
        thread::spawn(move || {
            waiter_clone.wait_with_scheduler_status(&status_clone, false);
            let _ = tx.send(());
        });

        thread::sleep(Duration::from_millis(100));

        // abort() publishes STATUS_ABORT (not STATUS_END) and then notifies:
        // a choked demuxer must be released by either stopping state.
        status.store(STATUS_ABORT, Ordering::Release);
        waiter.set(true); // notify while still choked: only the status can release

        rx.recv_timeout(Duration::from_secs(2))
            .expect("a choked waiter must be released when the scheduler aborts");
    }

    #[test]
    fn test_no_deadlock() {
        let waiter = Arc::new(SchWaiter::new());
        let waiter_clone = Arc::clone(&waiter);

        let handle = thread::spawn(move || {
            waiter_clone.set(true);
            waiter_clone.wait();
        });

        thread::sleep(Duration::from_millis(100));

        waiter.set(false);

        handle.join().unwrap();
    }
}
