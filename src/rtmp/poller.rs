// src/rtmp/poller.rs - Cross-platform IO multiplexer
//
// Provides a unified IO multiplexing abstraction:
// - Linux: epoll (edge-triggered)
// - macOS/BSD: kqueue (EV_CLEAR edge-triggered)
// - Windows: WSAPoll (level-triggered)
//
// Design principles:
// - No new dependencies, uses std + libc FFI
// - Edge-triggered mode requires drain until WouldBlock
// - EINTR auto-retry

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Event interest flags
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Interest {
    pub readable: bool,
    pub writable: bool,
}

impl Interest {
    pub const READABLE: Interest = Interest {
        readable: true,
        writable: false,
    };

    #[cfg(test)]
    pub const WRITABLE: Interest = Interest {
        readable: false,
        writable: true,
    };

    pub fn add_writable(self) -> Interest {
        Interest {
            writable: true,
            ..self
        }
    }
}

/// IO event
#[derive(Debug, Clone, Copy)]
pub struct Event {
    pub token: usize,
    pub readable: bool,
    pub writable: bool,
    pub error: bool,
    pub hangup: bool,
}

impl Event {
    pub fn is_readable(&self) -> bool {
        self.readable
    }

    pub fn is_writable(&self) -> bool {
        self.writable
    }

    pub fn is_error(&self) -> bool {
        self.error
    }

    pub fn is_hangup(&self) -> bool {
        self.hangup
    }
}

/// Reserved poller token for the reactor's [`Waker`] (PERF-3).
///
/// Connection tokens pack `(generation << TOKEN_ID_BITS) | id` with each half
/// sized to half the pointer width (`usize::BITS / 2` bits — 32 on 64-bit
/// targets, 16 on 32-bit), and `effective_max_connections` caps ids below
/// `TOKEN_ID_MASK`, so a connection token can never equal `usize::MAX` even at
/// the maximum generation. The reactor additionally matches this token before
/// decoding it as a connection token.
pub const WAKER_TOKEN: usize = usize::MAX;

// ============================================================================
// Platform-specific implementations
// ============================================================================

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::os::unix::io::RawFd;

    pub type RawHandle = RawFd;

    // libc provides the epoll bindings, including the arch-dependent packed
    // layout of epoll_event (x86_64 packs it to 12 bytes; a plain #[repr(C)]
    // struct would be 16 bytes and corrupt every event after the first).

    pub struct Poller {
        epfd: RawFd,
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            // SAFETY: epoll_create1(0) is a safe syscall that:
            // - Takes no pointers or external resources
            // - Returns a new file descriptor or -1 on error
            // - Error is checked immediately after the call
            // Thread safety: Creating an epoll instance is thread-safe
            let epfd = unsafe { libc::epoll_create1(0) };
            if epfd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Poller { epfd })
        }

        pub fn register(
            &mut self,
            fd: RawHandle,
            token: usize,
            interest: Interest,
        ) -> io::Result<()> {
            let mut event = libc::epoll_event {
                events: interest_to_epoll(interest) | libc::EPOLLET as u32,
                u64: token as u64,
            };

            // SAFETY: epoll_ctl with EPOLL_CTL_ADD requires:
            // - self.epfd is valid (created in new(), owned by self)
            // - fd is a valid file descriptor (caller's responsibility per API contract)
            // - &mut event points to a valid, properly initialized epoll_event on the stack
            // Error is checked immediately; operation is atomic w.r.t. this epoll instance
            // Thread safety: Poller requires &mut self, ensuring exclusive access
            let ret = unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_ADD, fd, &mut event) };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub fn modify(
            &mut self,
            fd: RawHandle,
            token: usize,
            interest: Interest,
        ) -> io::Result<()> {
            let mut event = libc::epoll_event {
                events: interest_to_epoll(interest) | libc::EPOLLET as u32,
                u64: token as u64,
            };

            // SAFETY: epoll_ctl with EPOLL_CTL_MOD requires:
            // - self.epfd is valid (created in new(), owned by self)
            // - fd was previously registered (caller's responsibility per API contract)
            // - &mut event points to a valid, properly initialized epoll_event on the stack
            // Error is checked immediately; operation is atomic w.r.t. this epoll instance
            // Thread safety: Poller requires &mut self, ensuring exclusive access
            let ret = unsafe { libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_MOD, fd, &mut event) };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub fn deregister(&mut self, fd: RawHandle) -> io::Result<()> {
            // SAFETY: epoll_ctl with EPOLL_CTL_DEL requires:
            // - self.epfd is valid (created in new(), owned by self)
            // - fd was previously registered (caller's responsibility per API contract)
            // - event pointer can be null for EPOLL_CTL_DEL (per Linux kernel 2.6.9+)
            // Error is checked immediately; operation is atomic w.r.t. this epoll instance
            // Thread safety: Poller requires &mut self, ensuring exclusive access
            let ret = unsafe {
                libc::epoll_ctl(self.epfd, libc::EPOLL_CTL_DEL, fd, std::ptr::null_mut())
            };
            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        /// Waits for IO readiness and refills `events` with this wakeup's
        /// events. The buffer is cleared first, so the caller can reuse one
        /// `Vec` across wakeups instead of paying a fresh allocation per poll.
        pub fn poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()> {
            events.clear();
            let timeout_ms = timeout.map(|d| d.as_millis() as i32).unwrap_or(-1);

            // SAFETY: std::mem::zeroed() for an epoll_event array is safe:
            // - epoll_event is a POD type with no invalid bit patterns
            // - All zero bytes represent valid (empty) events
            // - The array is immediately overwritten by epoll_wait
            let mut raw_events: [libc::epoll_event; 256] = unsafe { std::mem::zeroed() };

            loop {
                // SAFETY: epoll_wait requires:
                // - self.epfd is valid (created in new(), owned by self)
                // - raw_events.as_mut_ptr() points to valid, writable memory for 256 events
                // - raw_events.len() correctly reports the array capacity
                // - timeout_ms is a valid i32 (-1 for infinite, >=0 for milliseconds)
                // Error (including EINTR) is checked immediately
                // Thread safety: Poller requires &mut self, ensuring exclusive access
                let ret = unsafe {
                    libc::epoll_wait(
                        self.epfd,
                        raw_events.as_mut_ptr(),
                        raw_events.len() as i32,
                        timeout_ms,
                    )
                };

                if ret < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue; // EINTR - retry
                    }
                    return Err(err);
                }

                for ev in raw_events.iter().take(ret as usize) {
                    // Copy the fields out by value: epoll_event is packed on
                    // x86_64, so no references into it may be created.
                    let bits = ev.events;
                    let token = ev.u64 as usize;
                    events.push(Event {
                        token,
                        readable: bits & libc::EPOLLIN as u32 != 0,
                        writable: bits & libc::EPOLLOUT as u32 != 0,
                        error: bits & libc::EPOLLERR as u32 != 0,
                        hangup: bits & libc::EPOLLHUP as u32 != 0,
                    });
                }
                return Ok(());
            }
        }
    }

    impl Drop for Poller {
        fn drop(&mut self) {
            // SAFETY: close() on self.epfd is safe because:
            // - self.epfd is valid (created in new(), owned exclusively by self)
            // - This is the only place where epfd is closed (Drop is called once)
            // - After drop, self is deallocated so no double-close is possible
            // Thread safety: Drop takes &mut self, ensuring exclusive access
            unsafe { libc::close(self.epfd) };
        }
    }

    fn interest_to_epoll(interest: Interest) -> u32 {
        let mut events = 0;
        if interest.readable {
            events |= libc::EPOLLIN as u32;
        }
        if interest.writable {
            events |= libc::EPOLLOUT as u32;
        }
        events
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
mod bsd {
    use super::*;
    use std::os::unix::io::RawFd;

    pub type RawHandle = RawFd;

    // kqueue constants
    const EVFILT_READ: i16 = -1;
    const EVFILT_WRITE: i16 = -2;

    const EV_ADD: u16 = 0x0001;
    const EV_DELETE: u16 = 0x0002;
    const EV_ENABLE: u16 = 0x0004;
    const EV_CLEAR: u16 = 0x0020; // Edge-triggered equivalent
    const EV_EOF: u16 = 0x8000;
    const EV_ERROR: u16 = 0x4000;
    // Force kevent() to report each submitted change's status in the eventlist
    // (EV_ERROR set, data = errno, 0 on success) instead of stopping at the
    // first failure and returning a single ambiguous errno.
    const EV_RECEIPT: u16 = 0x0040;

    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Timespec {
        tv_sec: isize,
        tv_nsec: isize,
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct Kevent {
        ident: usize,
        filter: i16,
        flags: u16,
        fflags: u32,
        data: isize,
        udata: *mut std::ffi::c_void,
    }

    // # Safety
    //
    // These FFI functions directly call BSD kqueue system calls.
    // Callers must ensure:
    // - `kq` is a valid kqueue descriptor created by `kqueue()`
    // - `changelist` points to a valid array of `Kevent` with at least `nchanges` elements
    // - `eventlist` points to a valid array with at least `nevents` capacity
    // - `timeout` points to a valid `Timespec` or is null for blocking
    // - File descriptors referenced in kevents are valid and not closed while registered
    extern "C" {
        fn kqueue() -> i32;
        fn kevent(
            kq: i32,
            changelist: *const Kevent,
            nchanges: i32,
            eventlist: *mut Kevent,
            nevents: i32,
            timeout: *const Timespec,
        ) -> i32;
        fn close(fd: i32) -> i32;
    }

    pub struct Poller {
        kq: RawFd,
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            // SAFETY: kqueue() is a safe syscall that:
            // - Takes no pointers or external resources
            // - Returns a new file descriptor or -1 on error
            // - Error is checked immediately after the call
            // Thread safety: Creating a kqueue instance is thread-safe
            let kq = unsafe { kqueue() };
            if kq < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(Poller { kq })
        }

        pub fn register(
            &mut self,
            fd: RawHandle,
            token: usize,
            interest: Interest,
        ) -> io::Result<()> {
            let mut changes = Vec::with_capacity(2);

            if interest.readable {
                changes.push(Kevent {
                    ident: fd as usize,
                    filter: EVFILT_READ,
                    flags: EV_ADD | EV_ENABLE | EV_CLEAR,
                    fflags: 0,
                    data: 0,
                    udata: token as *mut _,
                });
            }

            if interest.writable {
                changes.push(Kevent {
                    ident: fd as usize,
                    filter: EVFILT_WRITE,
                    flags: EV_ADD | EV_ENABLE | EV_CLEAR,
                    fflags: 0,
                    data: 0,
                    udata: token as *mut _,
                });
            }

            if changes.is_empty() {
                return Ok(());
            }

            // SAFETY: kevent() for registration requires:
            // - self.kq is valid (created in new(), owned by self)
            // - changes.as_ptr() points to valid Kevent array with correct length
            // - eventlist is null (we're only submitting changes, not polling)
            // - timeout is null (no wait needed for change submission)
            // Error is checked immediately
            // Thread safety: Poller requires &mut self, ensuring exclusive access
            let ret = unsafe {
                kevent(
                    self.kq,
                    changes.as_ptr(),
                    changes.len() as i32,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            };

            if ret < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }

        pub fn modify(
            &mut self,
            fd: RawHandle,
            token: usize,
            interest: Interest,
        ) -> io::Result<()> {
            // kqueue: For modify, we use EV_ADD which will update existing registration
            // Note: We need to explicitly disable filters we don't want anymore
            let mut changes = Vec::with_capacity(2);

            // For EVFILT_READ
            if interest.readable {
                changes.push(Kevent {
                    ident: fd as usize,
                    filter: EVFILT_READ,
                    flags: EV_ADD | EV_ENABLE | EV_CLEAR,
                    fflags: 0,
                    data: 0,
                    udata: token as *mut _,
                });
            } else {
                // Disable read filter
                changes.push(Kevent {
                    ident: fd as usize,
                    filter: EVFILT_READ,
                    flags: EV_DELETE,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                });
            }

            // For EVFILT_WRITE
            if interest.writable {
                changes.push(Kevent {
                    ident: fd as usize,
                    filter: EVFILT_WRITE,
                    flags: EV_ADD | EV_ENABLE | EV_CLEAR,
                    fflags: 0,
                    data: 0,
                    udata: token as *mut _,
                });
            } else {
                // Disable write filter
                changes.push(Kevent {
                    ident: fd as usize,
                    filter: EVFILT_WRITE,
                    flags: EV_DELETE,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                });
            }

            // Submit each change with EV_RECEIPT so kevent() reports the result
            // of every change in the eventlist (EV_ERROR set, data = errno, 0 on
            // success) without blocking. Without EV_RECEIPT, kevent() stops at the
            // first failing change and returns one errno, so a benign ENOENT from
            // an EV_DELETE (disabling a filter that was never enabled — routine
            // when clearing writable interest after a flush) is indistinguishable
            // from a real EV_ADD failure. That ambiguity forced the old code to
            // swallow every error, which also masked genuine EV_ADD failures and
            // stranded queued writes on the affected connection.
            for change in changes.iter_mut() {
                change.flags |= EV_RECEIPT;
            }
            let mut results = changes.clone();
            // SAFETY: self.kq is valid (owned by self); changes/results are valid
            // Kevent arrays of the same length; EV_RECEIPT yields one result per
            // change; timeout is null (change submission does not wait).
            let ret = loop {
                let ret = unsafe {
                    kevent(
                        self.kq,
                        changes.as_ptr(),
                        changes.len() as i32,
                        results.as_mut_ptr(),
                        results.len() as i32,
                        std::ptr::null(),
                    )
                };
                if ret < 0 {
                    let err = std::io::Error::last_os_error();
                    // EINTR: a signal interrupted change submission. Retry rather
                    // than surface an error — the caller (update_dirty_interests)
                    // would otherwise close a healthy connection. Matches poll().
                    if err.kind() == std::io::ErrorKind::Interrupted {
                        continue;
                    }
                    // A real syscall failure (bad kq, EFAULT, ...).
                    return Err(err);
                }
                break ret;
            };
            // EV_RECEIPT reports one result per submitted change; a short receipt
            // would leave some change unchecked (treated as success below).
            debug_assert_eq!(
                ret as usize,
                changes.len(),
                "kevent(EV_RECEIPT) returned {ret} results for {} changes",
                changes.len()
            );

            // Fail on any real per-change error, but ignore ENOENT on an EV_DELETE
            // (the filter we asked to disable simply was not registered).
            for res in &results[..ret as usize] {
                if res.flags & EV_ERROR == 0 || res.data == 0 {
                    continue;
                }
                let is_delete = (res.filter == EVFILT_READ && !interest.readable)
                    || (res.filter == EVFILT_WRITE && !interest.writable);
                if is_delete && res.data as i32 == libc::ENOENT {
                    continue;
                }
                return Err(std::io::Error::from_raw_os_error(res.data as i32));
            }
            Ok(())
        }

        pub fn deregister(&mut self, fd: RawHandle) -> io::Result<()> {
            let changes = [
                Kevent {
                    ident: fd as usize,
                    filter: EVFILT_READ,
                    flags: EV_DELETE,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                },
                Kevent {
                    ident: fd as usize,
                    filter: EVFILT_WRITE,
                    flags: EV_DELETE,
                    fflags: 0,
                    data: 0,
                    udata: std::ptr::null_mut(),
                },
            ];

            // Ignore errors - filter might not be registered
            // SAFETY: kevent() for deregistration requires:
            // - self.kq is valid (created in new(), owned by self)
            // - changes.as_ptr() points to valid Kevent array with correct length
            // - eventlist is null (we're only submitting changes, not polling)
            // - timeout is null (no wait needed for change submission)
            // EV_DELETE errors are intentionally ignored (filter might not exist)
            // Thread safety: Poller requires &mut self, ensuring exclusive access
            unsafe {
                kevent(
                    self.kq,
                    changes.as_ptr(),
                    changes.len() as i32,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                );
            }
            Ok(())
        }

        /// Waits for IO readiness and refills `events` with this wakeup's
        /// events. The buffer is cleared first, so the caller can reuse one
        /// `Vec` across wakeups instead of paying a fresh allocation per poll.
        pub fn poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()> {
            events.clear();
            let timespec = timeout.map(|d| Timespec {
                tv_sec: d.as_secs() as isize,
                tv_nsec: d.subsec_nanos() as isize,
            });

            let timeout_ptr = timespec
                .as_ref()
                .map(|t| t as *const _)
                .unwrap_or(std::ptr::null());

            // SAFETY: std::mem::zeroed() for Kevent array is safe because:
            // - Kevent is a POD type with no invalid bit patterns
            // - All zero bytes represent valid (empty) events
            // - The array is immediately overwritten by kevent()
            let mut raw_events: [Kevent; 256] = unsafe { std::mem::zeroed() };

            loop {
                // SAFETY: kevent() for polling requires:
                // - self.kq is valid (created in new(), owned by self)
                // - changelist is null (no changes to submit)
                // - raw_events.as_mut_ptr() points to valid, writable memory for 256 Kevents
                // - raw_events.len() correctly reports the array capacity
                // - timeout_ptr points to valid Timespec or is null for blocking
                // Error (including EINTR) is checked immediately
                // Thread safety: Poller requires &mut self, ensuring exclusive access
                let ret = unsafe {
                    kevent(
                        self.kq,
                        std::ptr::null(),
                        0,
                        raw_events.as_mut_ptr(),
                        raw_events.len() as i32,
                        timeout_ptr,
                    )
                };

                if ret < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue; // EINTR - retry
                    }
                    return Err(err);
                }

                // Aggregate events by token
                use std::collections::HashMap;
                let mut event_map: HashMap<usize, Event> = HashMap::new();

                for i in 0..ret as usize {
                    let ev = &raw_events[i];
                    let token = ev.udata as usize;

                    let entry = event_map.entry(token).or_insert(Event {
                        token,
                        readable: false,
                        writable: false,
                        error: (ev.flags & EV_ERROR) != 0,
                        hangup: (ev.flags & EV_EOF) != 0,
                    });

                    match ev.filter {
                        EVFILT_READ => entry.readable = true,
                        EVFILT_WRITE => entry.writable = true,
                        _ => {}
                    }

                    if (ev.flags & EV_ERROR) != 0 {
                        entry.error = true;
                    }
                    if (ev.flags & EV_EOF) != 0 {
                        entry.hangup = true;
                    }
                }

                events.extend(event_map.into_values());
                return Ok(());
            }
        }
    }

    impl Drop for Poller {
        fn drop(&mut self) {
            // SAFETY: close() on self.kq is safe because:
            // - self.kq is valid (created in new(), owned exclusively by self)
            // - This is the only place where kq is closed (Drop is called once)
            // - After drop, self is deallocated so no double-close is possible
            // Thread safety: Drop takes &mut self, ensuring exclusive access
            unsafe { close(self.kq) };
        }
    }
}

#[cfg(target_os = "windows")]
mod windows {
    use super::*;
    use std::os::windows::io::RawSocket;

    pub type RawHandle = RawSocket;

    // WSAPoll constants
    const POLLIN: i16 = 0x0100;
    const POLLOUT: i16 = 0x0010;
    const POLLERR: i16 = 0x0001;
    const POLLHUP: i16 = 0x0002;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct WSAPollFd {
        fd: RawSocket,
        events: i16,
        revents: i16,
    }

    #[repr(C)]
    struct WSAData {
        version: u16,
        high_version: u16,
        max_sockets: u16,
        max_udp_dg: u16,
        vendor_info: *mut i8,
        description: [i8; 257],
        system_status: [i8; 129],
    }

    /// # Safety
    ///
    /// These FFI functions directly call Windows Winsock2 API.
    /// Callers must ensure:
    /// - `WSAStartup` is called before any other Winsock functions
    /// - `fds` points to a valid array of `WSAPollFd` with at least `nfds` elements
    /// - `data` points to a valid `WSAData` structure
    /// - Sockets referenced in `fds` are valid and not closed while polling
    #[link(name = "ws2_32")]
    extern "system" {
        fn WSAPoll(fds: *mut WSAPollFd, nfds: u32, timeout: i32) -> i32;
        fn WSAStartup(version: u16, data: *mut WSAData) -> i32;
        fn WSACleanup() -> i32;
        fn WSAGetLastError() -> i32;
    }

    struct FdEntry {
        fd: RawSocket,
        token: usize,
        interest: Interest,
    }

    pub struct Poller {
        entries: Vec<FdEntry>,
        initialized: bool,
    }

    impl Poller {
        pub fn new() -> io::Result<Self> {
            // Initialize Winsock
            // SAFETY: std::mem::zeroed() for WSAData is safe because:
            // - WSAData is a POD type with no invalid bit patterns
            // - All fields will be overwritten by WSAStartup
            let mut wsa_data: WSAData = unsafe { std::mem::zeroed() };
            // SAFETY: WSAStartup requires:
            // - version 0x0202 requests Winsock 2.2 (valid version)
            // - &mut wsa_data points to valid, writable WSAData structure
            // Error is checked immediately after the call
            // Thread safety: WSAStartup uses internal reference counting for initialization
            let ret = unsafe { WSAStartup(0x0202, &mut wsa_data) };
            if ret != 0 {
                return Err(io::Error::from_raw_os_error(ret));
            }

            Ok(Poller {
                entries: Vec::with_capacity(64),
                initialized: true,
            })
        }

        pub fn register(
            &mut self,
            fd: RawHandle,
            token: usize,
            interest: Interest,
        ) -> io::Result<()> {
            // Check if already registered
            if self.entries.iter().any(|e| e.fd == fd) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "fd already registered",
                ));
            }

            self.entries.push(FdEntry {
                fd,
                token,
                interest,
            });
            Ok(())
        }

        pub fn modify(
            &mut self,
            fd: RawHandle,
            token: usize,
            interest: Interest,
        ) -> io::Result<()> {
            if let Some(entry) = self.entries.iter_mut().find(|e| e.fd == fd) {
                entry.token = token;
                entry.interest = interest;
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "fd not registered"))
            }
        }

        pub fn deregister(&mut self, fd: RawHandle) -> io::Result<()> {
            if let Some(pos) = self.entries.iter().position(|e| e.fd == fd) {
                self.entries.swap_remove(pos);
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "fd not registered"))
            }
        }

        /// Waits for IO readiness and refills `events` with this wakeup's
        /// events. The buffer is cleared first, so the caller can reuse one
        /// `Vec` across wakeups instead of paying a fresh allocation per poll.
        pub fn poll(&mut self, timeout: Option<Duration>, events: &mut Vec<Event>) -> io::Result<()> {
            events.clear();
            if self.entries.is_empty() {
                // No fds to poll - sleep for timeout and return empty
                if let Some(dur) = timeout {
                    std::thread::sleep(dur);
                }
                return Ok(());
            }

            let timeout_ms = timeout.map(|d| d.as_millis() as i32).unwrap_or(-1);

            let mut pollfds: Vec<WSAPollFd> = self
                .entries
                .iter()
                .map(|e| WSAPollFd {
                    fd: e.fd,
                    events: interest_to_poll(&e.interest),
                    revents: 0,
                })
                .collect();

            loop {
                // SAFETY: WSAPoll requires:
                // - pollfds.as_mut_ptr() points to valid, writable WSAPollFd array
                // - pollfds.len() correctly reports the array length
                // - timeout_ms is a valid i32 (-1 for infinite, >=0 for milliseconds)
                // - All sockets in pollfds are valid (maintained by register/deregister)
                // Error is checked immediately
                // Thread safety: Poller requires &mut self, ensuring exclusive access
                let ret =
                    unsafe { WSAPoll(pollfds.as_mut_ptr(), pollfds.len() as u32, timeout_ms) };

                if ret < 0 {
                    // SAFETY: WSAGetLastError() is safe to call after a failed Winsock call
                    // - No parameters required
                    // - Returns thread-local error code (no shared state issues)
                    let err = unsafe { WSAGetLastError() };
                    // WSAEINTR = 10004
                    if err == 10004 {
                        continue; // Retry on interrupt
                    }
                    return Err(io::Error::from_raw_os_error(err));
                }

                for (i, pollfd) in pollfds.iter().enumerate() {
                    if pollfd.revents != 0 {
                        events.push(Event {
                            token: self.entries[i].token,
                            readable: (pollfd.revents & POLLIN) != 0,
                            writable: (pollfd.revents & POLLOUT) != 0,
                            error: (pollfd.revents & POLLERR) != 0,
                            hangup: (pollfd.revents & POLLHUP) != 0,
                        });
                    }
                }
                return Ok(());
            }
        }
    }

    impl Drop for Poller {
        fn drop(&mut self) {
            if self.initialized {
                // SAFETY: WSACleanup is safe to call because:
                // - self.initialized is true only if WSAStartup succeeded
                // - This is the only place where WSACleanup is called (Drop is called once)
                // - WSACleanup uses reference counting; balances the WSAStartup call
                // Thread safety: Drop takes &mut self, ensuring exclusive access
                unsafe { WSACleanup() };
            }
        }
    }

    fn interest_to_poll(interest: &Interest) -> i16 {
        let mut events: i16 = 0;
        if interest.readable {
            events |= POLLIN;
        }
        if interest.writable {
            events |= POLLOUT;
        }
        events
    }
}

// ============================================================================
// Re-export platform-specific implementation
// ============================================================================

#[cfg(target_os = "linux")]
pub use linux::{Poller, RawHandle};

#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
pub use bsd::{Poller, RawHandle};

#[cfg(target_os = "windows")]
pub use windows::{Poller, RawHandle};

// ============================================================================
// Waker - cross-platform reactor wakeup (PERF-3)
// ============================================================================
//
// A `Waker`/`WakeHandle` pair lets the in-process publisher send path
// interrupt the reactor's `poll()` the instant media arrives, instead of
// waiting up to POLL_TIMEOUT_MS. `Waker` is the reactor-side read end,
// registered with the `Poller` for readable interest and drained after each
// poll. `WakeHandle` is a cloneable `Send + Sync` producer handle; `wake()`
// coalesces on a userspace gate so only the first wake per drain cycle
// writes the backend fd (see the wrapper block below the backends).
//
//   - Linux:      eventfd (a single fd; the kernel sums concurrent writes)
//   - macOS/BSD:  self-pipe (kqueue EVFILT_READ; EVFILT_USER is an alternative)
//   - Windows:    connected loopback TCP socketpair (WSAPoll only polls sockets)

#[cfg(target_os = "linux")]
mod waker_backend {
    use super::*;
    use std::os::unix::io::RawFd;
    use std::sync::Arc;

    /// Shared eventfd, closed once when the last handle drops.
    struct WakerFd(RawFd);
    impl Drop for WakerFd {
        fn drop(&mut self) {
            // SAFETY: self.0 is a valid eventfd created in waker_pair(), owned
            // exclusively by this Arc; Drop runs once, when the last Arc drops.
            unsafe {
                libc::close(self.0);
            }
        }
    }

    pub struct Waker {
        fd: Arc<WakerFd>,
    }

    #[derive(Clone)]
    pub struct WakeHandle {
        fd: Arc<WakerFd>,
    }

    pub fn waker_pair() -> io::Result<(Waker, WakeHandle)> {
        // SAFETY: eventfd() with a literal initval/flags returns a new fd or -1,
        // checked immediately.
        let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let shared = Arc::new(WakerFd(fd));
        Ok((Waker { fd: shared.clone() }, WakeHandle { fd: shared }))
    }

    impl Waker {
        /// Raw handle to register with the Poller for readable interest.
        pub fn raw_handle(&self) -> RawFd {
            self.fd.0
        }

        /// Drain pending wake tokens, resetting the eventfd counter to 0.
        pub fn drain(&self) {
            let mut buf = [0u8; 8];
            loop {
                // SAFETY: reading 8 bytes from a valid non-blocking eventfd into
                // an 8-byte stack buffer. A successful read (n == 8) returns the
                // counter and resets it to 0; EFD_NONBLOCK yields EAGAIN (n < 0)
                // once empty.
                let n = unsafe { libc::read(self.fd.0, buf.as_mut_ptr() as *mut libc::c_void, 8) };
                if n != 8 {
                    break;
                }
            }
        }
    }

    impl WakeHandle {
        /// Signal the reactor. eventfd sums writes, so multiple wakes before a
        /// drain coalesce into a single readiness event. Returns whether a
        /// readiness token is now guaranteed on the fd: a successful write, or
        /// EAGAIN from a saturated counter (already readable without our
        /// token). Any other failure deposited nothing and reports `false` so
        /// the caller can recover.
        pub fn wake(&self) -> bool {
            let val: u64 = 1;
            loop {
                // SAFETY: writing 8 bytes from a u64 to a valid eventfd.
                let n = unsafe {
                    libc::write(self.fd.0, &val as *const u64 as *const libc::c_void, 8)
                };
                if n == 8 {
                    return true;
                }
                match io::Error::last_os_error().kind() {
                    io::ErrorKind::WouldBlock => return true,
                    io::ErrorKind::Interrupted => continue,
                    _ => return false,
                }
            }
        }
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd"
))]
mod waker_backend {
    use super::*;
    use std::os::unix::io::RawFd;
    use std::sync::Arc;

    /// Shared self-pipe, both ends closed once when the last handle drops.
    struct Pipe {
        read_fd: RawFd,
        write_fd: RawFd,
    }
    impl Drop for Pipe {
        fn drop(&mut self) {
            // SAFETY: both fds are valid pipe ends created in waker_pair(), owned
            // exclusively by this Arc; closed once, when the last Arc drops.
            unsafe {
                libc::close(self.read_fd);
                libc::close(self.write_fd);
            }
        }
    }

    pub struct Waker {
        pipe: Arc<Pipe>,
    }

    #[derive(Clone)]
    pub struct WakeHandle {
        pipe: Arc<Pipe>,
    }

    pub fn waker_pair() -> io::Result<(Waker, WakeHandle)> {
        let mut fds = [0 as libc::c_int; 2];
        // SAFETY: pipe() fills a valid 2-element c_int array or returns -1.
        let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if ret < 0 {
            return Err(io::Error::last_os_error());
        }
        let read_fd = fds[0] as RawFd;
        let write_fd = fds[1] as RawFd;
        // macOS has no pipe2; set O_NONBLOCK + FD_CLOEXEC explicitly on both ends.
        if let Err(e) =
            set_nonblocking_cloexec(read_fd).and_then(|_| set_nonblocking_cloexec(write_fd))
        {
            // SAFETY: closing the two fds we just created on the error path.
            unsafe {
                libc::close(read_fd);
                libc::close(write_fd);
            }
            return Err(e);
        }
        let shared = Arc::new(Pipe { read_fd, write_fd });
        Ok((
            Waker {
                pipe: shared.clone(),
            },
            WakeHandle { pipe: shared },
        ))
    }

    fn set_nonblocking_cloexec(fd: RawFd) -> io::Result<()> {
        // SAFETY: fcntl F_GETFL/F_SETFL/F_SETFD on a valid fd; each result checked.
        unsafe {
            let flags = libc::fcntl(fd, libc::F_GETFL);
            if flags < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) < 0 {
                return Err(io::Error::last_os_error());
            }
            if libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    impl Waker {
        pub fn raw_handle(&self) -> RawFd {
            self.pipe.read_fd
        }

        pub fn drain(&self) {
            let mut buf = [0u8; 64];
            loop {
                // SAFETY: reading into a valid 64-byte buffer from a non-blocking
                // pipe read end; drains all queued wake bytes, EAGAIN => empty.
                let n = unsafe {
                    libc::read(
                        self.pipe.read_fd,
                        buf.as_mut_ptr() as *mut libc::c_void,
                        buf.len(),
                    )
                };
                if n <= 0 || (n as usize) < buf.len() {
                    break;
                }
            }
        }
    }

    impl WakeHandle {
        /// Signal the reactor. Returns whether a readiness byte is now
        /// guaranteed on the pipe: a successful write, or EAGAIN from a full
        /// pipe (already readable without our byte). Any other failure
        /// deposited nothing and reports `false` so the caller can recover.
        pub fn wake(&self) -> bool {
            let byte: u8 = 1;
            loop {
                // SAFETY: writing 1 byte to a valid non-blocking pipe write end.
                let n = unsafe {
                    libc::write(
                        self.pipe.write_fd,
                        &byte as *const u8 as *const libc::c_void,
                        1,
                    )
                };
                if n == 1 {
                    return true;
                }
                match io::Error::last_os_error().kind() {
                    io::ErrorKind::WouldBlock => return true,
                    io::ErrorKind::Interrupted => continue,
                    _ => return false,
                }
            }
        }
    }
}

#[cfg(target_os = "windows")]
mod waker_backend {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::os::windows::io::{AsRawSocket, RawSocket};
    use std::sync::Arc;

    // Byte-level signalling on the loopback pair without needing &mut TcpStream,
    // matching the RawSocket convention used by the WSAPoll bindings above.
    #[link(name = "ws2_32")]
    extern "system" {
        fn send(s: RawSocket, buf: *const i8, len: i32, flags: i32) -> i32;
        fn recv(s: RawSocket, buf: *mut i8, len: i32, flags: i32) -> i32;
        fn WSAGetLastError() -> i32;
    }

    const WSAEINTR: i32 = 10004;
    const WSAEWOULDBLOCK: i32 = 10035;

    /// Connected loopback TCP pair; both sockets closed when the last Arc drops.
    struct Pair {
        reader: TcpStream,
        writer: TcpStream,
    }

    pub struct Waker {
        pair: Arc<Pair>,
    }

    #[derive(Clone)]
    pub struct WakeHandle {
        pair: Arc<Pair>,
    }

    pub fn waker_pair() -> io::Result<(Waker, WakeHandle)> {
        // Establish a connected loopback pair; WSAPoll can only poll sockets.
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        let writer = TcpStream::connect(addr)?;
        let (reader, _) = listener.accept()?;
        reader.set_nonblocking(true)?;
        writer.set_nonblocking(true)?;
        let shared = Arc::new(Pair { reader, writer });
        Ok((
            Waker {
                pair: shared.clone(),
            },
            WakeHandle { pair: shared },
        ))
    }

    impl Waker {
        pub fn raw_handle(&self) -> RawSocket {
            self.pair.reader.as_raw_socket()
        }

        pub fn drain(&self) {
            let mut buf = [0i8; 64];
            loop {
                // SAFETY: recv on a valid non-blocking loopback socket into a
                // 64-byte buffer; WSAEWOULDBLOCK / EOF (n <= 0) => nothing left.
                let n = unsafe {
                    recv(
                        self.pair.reader.as_raw_socket(),
                        buf.as_mut_ptr(),
                        buf.len() as i32,
                        0,
                    )
                };
                if n <= 0 || (n as usize) < buf.len() {
                    break;
                }
            }
        }
    }

    impl WakeHandle {
        /// Signal the reactor. Returns whether a readiness byte is now
        /// guaranteed queued toward the reader: a successful send, or
        /// WSAEWOULDBLOCK from a full send buffer (bytes already queued).
        /// Any other failure deposited nothing and reports `false` so the
        /// caller can recover.
        pub fn wake(&self) -> bool {
            let byte: i8 = 1;
            loop {
                // SAFETY: send 1 byte on a valid non-blocking loopback socket.
                let n = unsafe { send(self.pair.writer.as_raw_socket(), &byte as *const i8, 1, 0) };
                if n == 1 {
                    return true;
                }
                // SAFETY: WSAGetLastError reads the calling thread's Winsock
                // error slot and is the documented way to classify a failed
                // raw send — Win32 GetLastError (what io::Error::last_os_error
                // uses) is not guaranteed to carry it.
                match unsafe { WSAGetLastError() } {
                    WSAEWOULDBLOCK => return true,
                    WSAEINTR => continue,
                    _ => return false,
                }
            }
        }
    }
}

// ============================================================================
// Platform-neutral waker wrapper: userspace pending gate
// ============================================================================
//
// One `Arc<AtomicBool>` (`pending`) is shared by the `Waker` and every
// `WakeHandle` clone. It gates the backend write so that of all wakes
// coalesced between two drains only the winner attempts the backend write
// (one syscall in the common case; an EINTR retry can add more) — the
// kernel already merges concurrent tokens into the same readiness event
// (the eventfd sums, the pipe/socket queue bytes), so a skipped write never
// changes what the poll observes. A corollary: in steady state at most one
// wake token is outstanding on the backend fd (a drain racing the Windows
// socket's in-flight byte can transiently leave two transport bytes; the
// next drain-to-empty absorbs both). The backends therefore treat
// full-buffer results (EAGAIN / WSAEWOULDBLOCK) as token-present, not as
// failure — a full fd already holds bytes queued toward the reader.
//
// Invariant: `pending == true` implies either an unconsumed readiness token
// exists on the backend fd, or the winning wake is between its swap and the
// completion of its backend write — or, when that write definitively
// failed, its gate re-open is still ahead (see wake()) — or a drain is in
// progress whose clear, and every reactor re-check of the wake-signaled
// queues, is still ahead.
//
// Protocol:
//   wake():  the payload enqueue is sequenced before `pending.swap(true,
//            Release)`; only the false->true transition writes the fd. A
//            definitively failed backend write (nothing deposited) re-opens
//            the gate so a later wake retries.
//   drain(): read the backend fd to empty FIRST, then `pending.swap(false,
//            Acquire)`.
//
// The clear MUST follow the fd read. Clearing first strands the gate: a wake
// landing between the clear and the read re-writes the fd, the same read
// then absorbs that token, and the cycle ends with `pending == true` on an
// empty fd. Since drain() runs only on waker readiness events, no later
// clear can ever happen and every subsequent wake skips its write — the
// reactor silently degrades to its poll-timeout cadence. With drain-then-
// clear the clear is the final flag operation of each cycle, so any `true`
// that survives it was written by a swap that returned `false` and therefore
// performed the write.
//
// Ordering: both sides use `swap` (an atomic RMW), not `compare_exchange` —
// an RMW always reads the latest value in the flag's modification order, so
// a wake issued after a completed clear can never observe a stale `true` and
// skip a needed write (a failed compare_exchange is formally a plain load,
// which lacks that guarantee). The Release (wake) / Acquire (drain) pair
// carries every enqueue coalesced into a cycle happens-before the clear,
// hence before the reactor's subsequent re-checks of the signaled queues.
//
// The reactor loop must keep re-examining every wake-signaled source
// (status, publisher/handshake registrations, publisher channels) between
// drain() and the next blocking poll; wakes absorbed by the gate rely on
// those re-checks instead of a fresh fd token.

/// Reactor-side read end: registered with the [`Poller`], drained after each
/// waker readiness event.
pub struct Waker {
    backend: waker_backend::Waker,
    pending: Arc<AtomicBool>,
    /// Test seam: runs between the backend fd drain and the gate clear —
    /// the exact window the drain-then-clear ordering protects (test only).
    #[cfg(test)]
    drain_gap_hook: std::sync::Mutex<Option<Box<dyn FnMut() + Send>>>,
}

/// Cloneable `Send + Sync` producer handle; see the protocol block above.
#[derive(Clone)]
pub struct WakeHandle {
    backend: waker_backend::WakeHandle,
    pending: Arc<AtomicBool>,
}

pub fn waker_pair() -> io::Result<(Waker, WakeHandle)> {
    let (backend_waker, backend_handle) = waker_backend::waker_pair()?;
    let pending = Arc::new(AtomicBool::new(false));
    Ok((
        Waker {
            backend: backend_waker,
            pending: pending.clone(),
            #[cfg(test)]
            drain_gap_hook: std::sync::Mutex::new(None),
        },
        WakeHandle {
            backend: backend_handle,
            pending,
        },
    ))
}

impl Waker {
    /// Raw handle to register with the Poller for readable interest.
    pub fn raw_handle(&self) -> RawHandle {
        self.backend.raw_handle()
    }

    /// Drain pending wake tokens, then re-open the gate. The backend fd must
    /// be empty before the flag clears (see the protocol block above).
    pub fn drain(&self) {
        self.backend.drain();
        #[cfg(test)]
        if let Some(hook) = self.drain_gap_hook.lock().unwrap().as_mut() {
            hook();
        }
        self.pending.swap(false, Ordering::Acquire);
    }

    /// Install a callback fired between the fd drain and the gate clear
    /// (test only) — lets a test drive a wake into that window
    /// deterministically.
    #[cfg(test)]
    pub(crate) fn set_drain_gap_hook_for_test(&self, hook: Box<dyn FnMut() + Send>) {
        *self.drain_gap_hook.lock().unwrap() = Some(hook);
    }
}

impl WakeHandle {
    /// Signal the reactor. Only the winning wake per drain cycle attempts
    /// the backend write; the rest coalesce on the userspace gate.
    pub fn wake(&self) {
        if self.pending.swap(true, Ordering::Release) {
            return;
        }
        if !self.backend.wake() {
            // The winning write deposited no token (full-buffer results count
            // as deposited, so this is a teardown-class failure). Re-open the
            // gate so a later wake retries the write instead of coalescing
            // behind a token that does not exist. A wake that slipped in
            // between our swap and this re-open rides the next wake or the
            // reactor's poll-timeout re-checks — the same bounded fallback a
            // failed write had when every wake wrote unconditionally. The
            // gate can degrade one cycle to that fallback; it can no longer
            // latch shut.
            self.pending.swap(false, Ordering::Release);
        }
    }

    /// Current gate state (test only).
    #[cfg(test)]
    pub(crate) fn pending_for_test(&self) -> bool {
        self.pending.load(Ordering::Relaxed)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn test_poller_basic() {
        let mut poller = Poller::new().expect("Failed to create poller");

        // Create a TCP pair for testing
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");
        let (mut server, _) = listener.accept().expect("Failed to accept");
        server
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");

        let mut events = Vec::new();

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let client_fd = client.as_raw_fd();
            let server_fd = server.as_raw_fd();

            // Register client for readable
            poller
                .register(client_fd, 1, Interest::READABLE)
                .expect("Failed to register");
            // Register server for writable
            poller
                .register(server_fd, 2, Interest::WRITABLE)
                .expect("Failed to register");

            // Server should be immediately writable
            poller
                .poll(Some(Duration::from_millis(100)), &mut events)
                .expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 2 && e.is_writable()));

            // Write some data from server
            server.write_all(b"hello").expect("Failed to write");

            // Client should become readable
            poller
                .poll(Some(Duration::from_millis(100)), &mut events)
                .expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 1 && e.is_readable()));

            // Clean up
            poller.deregister(client_fd).expect("Failed to deregister");
            poller.deregister(server_fd).expect("Failed to deregister");
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            let client_fd = client.as_raw_socket();
            let server_fd = server.as_raw_socket();

            poller
                .register(client_fd, 1, Interest::READABLE)
                .expect("Failed to register");
            poller
                .register(server_fd, 2, Interest::WRITABLE)
                .expect("Failed to register");

            poller
                .poll(Some(Duration::from_millis(100)), &mut events)
                .expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 2 && e.is_writable()));

            server.write_all(b"hello").expect("Failed to write");

            poller
                .poll(Some(Duration::from_millis(100)), &mut events)
                .expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 1 && e.is_readable()));

            poller.deregister(client_fd).expect("Failed to deregister");
            poller.deregister(server_fd).expect("Failed to deregister");
        }
    }

    #[cfg(unix)]
    #[test]
    fn poll_reports_correct_tokens_for_multiple_ready_fds() {
        use std::os::unix::io::AsRawFd;

        let mut poller = Poller::new().expect("Failed to create poller");

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let _clients: Vec<TcpStream> = (0..3)
            .map(|_| TcpStream::connect(addr).expect("Failed to connect"))
            .collect();
        let servers: Vec<TcpStream> = (0..3)
            .map(|_| listener.accept().expect("Failed to accept").0)
            .collect();

        // All three sockets are immediately writable, so one poll returns
        // several events at once. Every entry of the kernel's event array
        // must round-trip its token — a struct layout mismatch (epoll_event
        // is packed on x86_64) corrupts every entry after the first.
        for (i, server) in servers.iter().enumerate() {
            server
                .set_nonblocking(true)
                .expect("Failed to set nonblocking");
            poller
                .register(server.as_raw_fd(), (i + 1) * 10, Interest::WRITABLE)
                .expect("Failed to register");
        }

        let mut events = Vec::new();
        poller
            .poll(Some(Duration::from_millis(200)), &mut events)
            .expect("Failed to poll");
        let mut tokens: Vec<usize> = events
            .iter()
            .filter(|e| e.is_writable())
            .map(|e| e.token)
            .collect();
        tokens.sort_unstable();

        assert_eq!(
            tokens,
            vec![10, 20, 30],
            "every simultaneously-ready fd must report its own token"
        );
    }

    #[test]
    fn poll_clears_the_reused_buffer_between_wakeups() {
        let mut poller = Poller::new().expect("Failed to create poller");

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");
        let (server, _) = listener.accept().expect("Failed to accept");
        server
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");

        #[cfg(unix)]
        let server_fd = {
            use std::os::unix::io::AsRawFd;
            server.as_raw_fd()
        };
        #[cfg(windows)]
        let server_fd = {
            use std::os::windows::io::AsRawSocket;
            server.as_raw_socket()
        };

        poller
            .register(server_fd, 7, Interest::WRITABLE)
            .expect("Failed to register");

        // First wakeup fills the buffer with the writable event.
        let mut events = Vec::new();
        poller
            .poll(Some(Duration::from_millis(100)), &mut events)
            .expect("Failed to poll");
        assert!(events.iter().any(|e| e.token == 7 && e.is_writable()));

        // Second wakeup reuses the SAME buffer after the fd is gone: poll must
        // clear it, not append to (or retain) the previous round's events.
        poller.deregister(server_fd).expect("Failed to deregister");
        poller
            .poll(Some(Duration::from_millis(50)), &mut events)
            .expect("Failed to poll");
        assert!(
            events.is_empty(),
            "a reused buffer must not carry stale events into the next wakeup"
        );
    }

    #[test]
    fn test_deregister_no_events() {
        let mut poller = Poller::new().expect("Failed to create poller");

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");

        let mut events = Vec::new();

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = client.as_raw_fd();

            poller
                .register(fd, 1, Interest::READABLE)
                .expect("Failed to register");
            poller.deregister(fd).expect("Failed to deregister");

            // After deregister, no events should be reported for this fd
            poller
                .poll(Some(Duration::from_millis(50)), &mut events)
                .expect("Failed to poll");
            assert!(!events.iter().any(|e| e.token == 1));
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            let fd = client.as_raw_socket();

            poller
                .register(fd, 1, Interest::READABLE)
                .expect("Failed to register");
            poller.deregister(fd).expect("Failed to deregister");

            poller
                .poll(Some(Duration::from_millis(50)), &mut events)
                .expect("Failed to poll");
            assert!(!events.iter().any(|e| e.token == 1));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn waker_wakes_the_poller_and_drains() {
        let mut poller = Poller::new().expect("Failed to create poller");
        let (waker, handle) = waker_pair().expect("Failed to create waker");
        poller
            .register(waker.raw_handle(), WAKER_TOKEN, Interest::READABLE)
            .expect("Failed to register waker");

        let mut events = Vec::new();

        // No wake yet: a short poll times out with no waker event.
        poller.poll(Some(Duration::from_millis(50)), &mut events).expect("poll");
        assert!(
            events.iter().all(|e| e.token != WAKER_TOKEN),
            "no wake => no waker event"
        );

        // Two coalesced wakes still produce a single readable event.
        handle.wake();
        handle.wake();
        poller.poll(Some(Duration::from_millis(500)), &mut events).expect("poll");
        assert!(
            events
                .iter()
                .any(|e| e.token == WAKER_TOKEN && e.is_readable()),
            "wake() must produce a readable event on WAKER_TOKEN"
        );

        // Drain clears the token; the next poll times out again.
        waker.drain();
        poller.poll(Some(Duration::from_millis(50)), &mut events).expect("poll");
        assert!(
            events.iter().all(|e| e.token != WAKER_TOKEN),
            "drain() must clear the wake token"
        );

        // Post-drain re-wake: the gate must have re-armed. Two more wakes
        // coalesce in userspace, so the eventfd counter holds exactly 1 —
        // unguarded writes would make the kernel sum it to 2.
        handle.wake();
        handle.wake();
        poller.poll(Some(Duration::from_millis(500)), &mut events).expect("poll");
        assert!(
            events
                .iter()
                .any(|e| e.token == WAKER_TOKEN && e.is_readable()),
            "a wake after drain() must produce a fresh readable event"
        );
        let mut buf = [0u8; 8];
        // SAFETY: reading 8 bytes from a valid eventfd into an 8-byte buffer;
        // a successful read returns the counter and resets it to 0.
        let n = unsafe { libc::read(waker.raw_handle(), buf.as_mut_ptr() as *mut libc::c_void, 8) };
        assert_eq!(n, 8, "eventfd must hold a token after a post-drain wake");
        assert_eq!(
            u64::from_ne_bytes(buf),
            1,
            "coalesced wakes must deposit exactly one token"
        );

        // drain() on the (manually emptied) fd must still clear the gate so
        // the next wake reaches the poller.
        waker.drain();
        handle.wake();
        poller.poll(Some(Duration::from_millis(500)), &mut events).expect("poll");
        assert!(
            events
                .iter()
                .any(|e| e.token == WAKER_TOKEN && e.is_readable()),
            "the gate must re-arm after every drain()"
        );
        waker.drain();

        poller.deregister(waker.raw_handle()).ok();
    }

    #[test]
    fn wake_pending_gate_skips_syscall_until_drained() {
        let (waker, handle) = waker_pair().expect("Failed to create waker");

        assert!(!handle.pending_for_test(), "a fresh pair starts un-armed");

        handle.wake();
        assert!(handle.pending_for_test(), "the first wake arms the gate");

        handle.wake();
        handle.wake();
        assert!(
            handle.pending_for_test(),
            "coalesced wakes keep the gate armed"
        );

        // Clones share the gate: a wake through a clone must be visible to
        // (and coalesce with) every other handle.
        let clone = handle.clone();
        assert!(clone.pending_for_test(), "clones observe the shared gate");

        waker.drain();
        assert!(
            !handle.pending_for_test(),
            "drain() clears the gate only after the fd is empty"
        );
        assert!(!clone.pending_for_test(), "clones observe the cleared gate");

        handle.wake();
        assert!(
            handle.pending_for_test(),
            "a post-drain wake arms the gate again"
        );
    }

    /// Deterministically drive a wake into the window between the backend fd
    /// read and the gate clear — the interleaving drain-then-clear exists to
    /// survive. The gated wake must coalesce (gate still armed, no token) and
    /// be absorbed by the clear, leaving the gate open. Under the broken
    /// clear-before-read ordering the same hook wake would observe an open
    /// gate, write a token the very same drain then swallows, and end the
    /// cycle latched shut over an empty fd — this test fails on both of its
    /// gate assertions in that world.
    #[test]
    fn wake_in_the_drain_gap_is_absorbed_and_cannot_strand_the_gate() {
        let mut poller = Poller::new().expect("Failed to create poller");
        let (waker, handle) = waker_pair().expect("Failed to create waker");
        poller
            .register(waker.raw_handle(), WAKER_TOKEN, Interest::READABLE)
            .expect("Failed to register waker");

        // Arm the gate and deposit a token.
        handle.wake();
        let mut events = Vec::new();
        poller
            .poll(Some(Duration::from_secs(5)), &mut events)
            .expect("poll");
        assert!(events
            .iter()
            .any(|e| e.token == WAKER_TOKEN && e.is_readable()));

        // The hook fires between the fd read and the clear, inside drain().
        let gap_handle = handle.clone();
        let gap_runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let gap_runs_in_hook = gap_runs.clone();
        waker.set_drain_gap_hook_for_test(Box::new(move || {
            gap_handle.wake();
            gap_runs_in_hook.fetch_add(1, Ordering::SeqCst);
        }));
        waker.drain();
        assert_eq!(gap_runs.load(Ordering::SeqCst), 1, "the gap hook must run");
        assert!(
            !handle.pending_for_test(),
            "a wake absorbed mid-drain must leave the gate open (a still-armed \
             gate here means the clear ran before the fd read)"
        );

        // The absorbed wake deposited no token, so the fd is quiet...
        poller
            .poll(Some(Duration::from_millis(50)), &mut events)
            .expect("poll");
        assert!(events.iter().all(|e| e.token != WAKER_TOKEN));

        // ...and the next wake must reach the poller — the strand detector.
        handle.wake();
        poller
            .poll(Some(Duration::from_secs(5)), &mut events)
            .expect("poll");
        assert!(
            events
                .iter()
                .any(|e| e.token == WAKER_TOKEN && e.is_readable()),
            "a wake following an absorbed-in-drain wake must produce a fresh \
             readiness event"
        );
        waker.drain();
        poller.deregister(waker.raw_handle()).ok();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wake_drain_pingpong_rearms_under_threads() {
        use std::sync::mpsc;

        let mut poller = Poller::new().expect("Failed to create poller");
        let (waker, handle) = waker_pair().expect("Failed to create waker");
        poller
            .register(waker.raw_handle(), WAKER_TOKEN, Interest::READABLE)
            .expect("Failed to register waker");

        const ROUNDS: usize = 200;
        let (turn_tx, turn_rx) = mpsc::channel::<()>();
        let producer = std::thread::spawn(move || {
            // Exactly one wake per turn; the consumer drains between turns,
            // so every round must re-arm the gate and deposit a fresh token.
            for _ in 0..ROUNDS {
                if turn_rx.recv().is_err() {
                    return;
                }
                handle.wake();
            }
        });

        let mut events = Vec::new();
        for round in 0..ROUNDS {
            turn_tx.send(()).expect("producer thread alive");
            poller
                .poll(Some(Duration::from_secs(5)), &mut events)
                .expect("poll");
            assert!(
                events
                    .iter()
                    .any(|e| e.token == WAKER_TOKEN && e.is_readable()),
                "round {}: a wake after a drain must reach the poller (a \
                 stranded gate skips the write and the poll times out)",
                round
            );
            waker.drain();
        }
        producer.join().expect("producer thread");

        // Every token consumed: the poller must be quiet again.
        poller
            .poll(Some(Duration::from_millis(50)), &mut events)
            .expect("poll");
        assert!(events.iter().all(|e| e.token != WAKER_TOKEN));

        poller.deregister(waker.raw_handle()).ok();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn wake_freerun_coalesces_and_rearms() {
        use std::sync::atomic::AtomicUsize;
        use std::time::Instant;

        let mut poller = Poller::new().expect("Failed to create poller");
        let (waker, handle) = waker_pair().expect("Failed to create waker");
        poller
            .register(waker.raw_handle(), WAKER_TOKEN, Interest::READABLE)
            .expect("Failed to register waker");

        const WAKES: usize = 200;
        let count = Arc::new(AtomicUsize::new(0));
        let producer = {
            let count = count.clone();
            let handle = handle.clone();
            std::thread::spawn(move || {
                for _ in 0..WAKES {
                    // The increment is the payload: its visibility to the
                    // consumer's post-drain re-check rides the gate's
                    // Release(wake)/Acquire(drain) pairing.
                    count.fetch_add(1, Ordering::Relaxed);
                    handle.wake();
                }
            })
        };

        // Drain until the full count is observed AFTER a drain: the protocol
        // guarantees every coalesced wake's payload is visible by the
        // re-check following the drain that absorbed (or followed) it.
        let mut events = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            assert!(
                Instant::now() < deadline,
                "consumer never observed all {} wakes (lost wake or stranded gate)",
                WAKES
            );
            poller
                .poll(Some(Duration::from_millis(500)), &mut events)
                .expect("poll");
            if events.iter().any(|e| e.token == WAKER_TOKEN) {
                waker.drain();
                if count.load(Ordering::Relaxed) == WAKES {
                    break;
                }
            }
        }
        producer.join().expect("producer thread");

        // At most one residual token can remain: a wake racing the final
        // drain's clear re-writes the fd at most once (a write requires the
        // swap to observe false, which only a completed clear publishes).
        poller
            .poll(Some(Duration::from_millis(500)), &mut events)
            .expect("poll");
        if events.iter().any(|e| e.token == WAKER_TOKEN) {
            waker.drain();
        }

        // Post-quiescence: fd empty, gate cleared.
        poller
            .poll(Some(Duration::from_millis(50)), &mut events)
            .expect("poll");
        assert!(
            events.iter().all(|e| e.token != WAKER_TOKEN),
            "after one settling drain the waker fd must be empty"
        );
        assert!(
            !handle.pending_for_test(),
            "the gate must be clear once the fd is empty and no drain is in flight"
        );

        // Direct lost-wake / stranded-gate detector: one more wake must
        // produce a fresh readiness event.
        handle.wake();
        poller
            .poll(Some(Duration::from_secs(5)), &mut events)
            .expect("poll");
        assert!(
            events
                .iter()
                .any(|e| e.token == WAKER_TOKEN && e.is_readable()),
            "a wake after quiescence must re-arm and reach the poller"
        );
        waker.drain();

        poller.deregister(waker.raw_handle()).ok();
    }

    #[test]
    fn test_modify_interest() {
        let mut poller = Poller::new().expect("Failed to create poller");

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");
        let (server, _) = listener.accept().expect("Failed to accept");
        server
            .set_nonblocking(true)
            .expect("Failed to set nonblocking");

        let mut events = Vec::new();

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let server_fd = server.as_raw_fd();

            // Register for readable only
            poller
                .register(server_fd, 1, Interest::READABLE)
                .expect("Failed to register");

            // Modify to writable
            poller
                .modify(server_fd, 1, Interest::WRITABLE)
                .expect("Failed to modify");

            // Should be writable now
            poller
                .poll(Some(Duration::from_millis(100)), &mut events)
                .expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 1 && e.is_writable()));

            poller.deregister(server_fd).expect("Failed to deregister");
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            let server_fd = server.as_raw_socket();

            poller
                .register(server_fd, 1, Interest::READABLE)
                .expect("Failed to register");
            poller
                .modify(server_fd, 1, Interest::WRITABLE)
                .expect("Failed to modify");

            poller
                .poll(Some(Duration::from_millis(100)), &mut events)
                .expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 1 && e.is_writable()));

            poller.deregister(server_fd).expect("Failed to deregister");
        }
    }
}
