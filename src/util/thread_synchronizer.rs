//! Join-all bookkeeping for scheduler workers. fftools joins each
//! `SchTask` pthread directly in `sch_stop` (ffmpeg_sched.c); ez counts
//! live workers behind a condvar instead, because threads are detached
//! `std::thread` spawns whose handles the scheduler does not keep.

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
        let mut counter = self
            .inner
            .counter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *counter += 1;
    }

    /// Decrements the counter; when this was the last thread, runs `on_last`
    /// BEFORE waking any waiter. Publishing the terminal state inside this
    /// window is what makes a woken wait()/poll() observe it: waking first
    /// and storing afterwards loses the (already consumed) async waker and
    /// leaves the future pending forever.
    pub(crate) fn thread_done_with(&self, on_last: impl FnOnce()) {
        // Take the waker to fire it OUTSIDE the counter lock. `Waker::wake()` is
        // a safe API with no panic guarantee; called while holding the counter
        // mutex a panicking waker would POISON it, and a re-entrant release (an
        // armed MuxSlotGuard whose disarm was skipped by that same unwind) would
        // then double-panic on the poisoned lock and abort — or, once recovered,
        // underflow the counter. `on_last()` + the condvar notify stay under the
        // lock so a woken wait()/poll() still observes the terminal state
        // (publish-before-wake, load-bearing for the async waker too).
        #[cfg(feature = "async")]
        let mut waker_to_fire: Option<std::task::Waker> = None;
        {
            let mut counter = self
                .inner
                .counter
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            *counter -= 1;

            if *counter == 0 {
                on_last();
                self.inner.condvar.notify_one();

                #[cfg(feature = "async")]
                {
                    waker_to_fire = self
                        .inner
                        .waker
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .take();
                }
            }
        }

        // Fire outside the lock and CONTAIN a panicking waker: it must neither
        // poison the counter nor unwind into a caller's teardown and skip its
        // disarm.
        #[cfg(feature = "async")]
        if let Some(waker) = waker_to_fire {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || waker.wake()));
        }
    }

    pub(crate) fn wait_for_all_threads(&self) {
        let mut counter = self
            .inner
            .counter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while *counter > 0 {
            counter = self
                .inner
                .condvar
                .wait(counter)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    /// Non-blocking check: whether every started thread has finished (the
    /// counter reached 0). The async Future gates readiness on this rather than
    /// on the terminal status: on a panic a worker can publish its terminal
    /// status and record its error in either order, but it always releases its
    /// thread slot AFTER recording the error, so `counter == 0` means the result
    /// is fully settled.
    #[cfg(feature = "async")]
    pub(crate) fn all_threads_done(&self) -> bool {
        *self
            .inner
            .counter
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == 0
    }

    #[cfg(feature = "async")]
    pub(crate) fn set_waker(&self, waker: std::task::Waker) {
        let mut waker_slot = self
            .inner
            .waker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
///
/// A panicking worker is also RECORDED as the scheduler error: its channels
/// disconnect, downstream reads that as a clean EOF, and wait()/stop() used
/// to report `Ok(())` over a silently truncated output. The record happens
/// BEFORE the slot release — the release may be the last one, waking a
/// wait() that must already observe the error.
pub(crate) struct ThreadDoneGuard {
    thread_sync: ThreadSynchronizer,
    scheduler_status: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    scheduler_result: std::sync::Arc<std::sync::Mutex<Option<crate::error::Result<()>>>>,
}

impl ThreadDoneGuard {
    pub(crate) fn adopt(
        thread_sync: ThreadSynchronizer,
        scheduler_status: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        scheduler_result: std::sync::Arc<std::sync::Mutex<Option<crate::error::Result<()>>>>,
    ) -> Self {
        Self {
            thread_sync,
            scheduler_status,
            scheduler_result,
        }
    }
}

impl Drop for ThreadDoneGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // set_scheduler_error is first-error-wins and poison-safe; it also
            // publishes STATUS_END and wakes paused workers, so the survivors
            // stop promptly instead of finishing a torn pipeline.
            let name = std::thread::current()
                .name()
                .unwrap_or("worker")
                .to_string();
            crate::core::scheduler::ffmpeg_scheduler::set_scheduler_error(
                &self.scheduler_status,
                &self.scheduler_result,
                crate::error::Error::WorkerPanicked(name),
            );
        }
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

#[cfg(test)]
mod panic_tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    /// H4: a panicking worker's guard must record WorkerPanicked BEFORE the
    /// slot release wakes wait()/stop() — otherwise the job reports Ok(())
    /// over a truncated output.
    #[test]
    fn panicking_worker_records_error_before_releasing_the_slot() {
        let sync = ThreadSynchronizer::new();
        let status = Arc::new(std::sync::atomic::AtomicUsize::new(
            crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN,
        ));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));

        sync.thread_start();
        let guard = ThreadDoneGuard::adopt(sync.clone(), status.clone(), result.clone());
        let handle = std::thread::Builder::new()
            .name("panicky-worker".to_string())
            .spawn(move || {
                let _guard = guard;
                panic!("test-injected worker panic");
            })
            .unwrap();

        // wait_for_all_threads returning proves the slot was released; the
        // error must already be observable at that point.
        sync.wait_for_all_threads();
        let recorded = result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match recorded {
            Some(Err(crate::error::Error::WorkerPanicked(name))) => {
                assert_eq!(name, "panicky-worker");
            }
            other => panic!("expected WorkerPanicked, got {other:?}"),
        }
        assert!(crate::core::scheduler::ffmpeg_scheduler::is_stopping(
            status.load(std::sync::atomic::Ordering::Acquire)
        ));
        let _ = handle.join();
    }

    /// A normally-exiting worker must record nothing.
    #[test]
    fn clean_worker_records_no_error() {
        let sync = ThreadSynchronizer::new();
        let status = Arc::new(std::sync::atomic::AtomicUsize::new(
            crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN,
        ));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));

        sync.thread_start();
        let guard = ThreadDoneGuard::adopt(sync.clone(), status.clone(), result.clone());
        std::thread::spawn(move || {
            let _guard = guard;
        })
        .join()
        .unwrap();

        assert!(result.lock().unwrap().is_none());
    }

    /// `Waker::wake()` is a safe API with no panic guarantee. It is now
    /// fired OUTSIDE the counter lock and wrapped in `catch_unwind`, so a
    /// panicking waker must NOT propagate out of `thread_done_with` (which would
    /// skip a caller's manual disarm and double-release the slot) and must NOT
    /// poison the counter mutex. After the last release fires such a waker, the
    /// counter must be 0 and a fresh start/done cycle must still lock cleanly.
    #[cfg(feature = "async")]
    #[test]
    fn panicking_waker_is_contained_and_does_not_poison_the_counter() {
        use std::task::{RawWaker, RawWakerVTable, Waker};

        unsafe fn clone_panic(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &PANIC_VTABLE)
        }
        unsafe fn wake_panic(_: *const ()) {
            panic!("test-injected waker panic");
        }
        unsafe fn drop_noop(_: *const ()) {}
        static PANIC_VTABLE: RawWakerVTable =
            RawWakerVTable::new(clone_panic, wake_panic, wake_panic, drop_noop);

        let sync = ThreadSynchronizer::new();
        sync.thread_start();
        // SAFETY: the vtable's fns are all valid for a null data pointer.
        sync.set_waker(unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &PANIC_VTABLE)) });

        // The last release drives the counter to 0 and fires the panicking
        // waker; thread_done_with must swallow that panic (this call returns
        // normally rather than unwinding the test thread).
        sync.thread_done_with(|| {});

        // The mutex is not poisoned and the counter is consistent: a fresh cycle
        // locks cleanly and wait_for_all_threads returns immediately.
        sync.thread_start();
        sync.thread_done_with(|| {});
        sync.wait_for_all_threads();
    }

    /// the async Future gates readiness on `all_threads_done()` (the
    /// counter reaching 0), NOT on the terminal status a panicking worker may
    /// publish mid-unwind. Verify the accessor tracks the counter exactly.
    #[cfg(feature = "async")]
    #[test]
    fn all_threads_done_tracks_the_counter() {
        let sync = ThreadSynchronizer::new();
        assert!(sync.all_threads_done(), "no live threads => done");
        sync.thread_start();
        assert!(!sync.all_threads_done(), "one live thread => not done");
        sync.thread_start();
        sync.thread_done_with(|| {});
        assert!(
            !sync.all_threads_done(),
            "one of two threads still live => not done"
        );
        sync.thread_done_with(|| {});
        assert!(sync.all_threads_done(), "all slots released => done");
    }
}
