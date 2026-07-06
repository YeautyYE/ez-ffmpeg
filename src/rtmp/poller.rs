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
/// Connection tokens encode `(generation << 32) | id`, so `usize::MAX` would
/// only collide if a slot reached `generation == id == u32::MAX` (4 billion
/// reuses of slot `0xFFFF_FFFF`). The reactor also matches this token before
/// decoding it as a connection, so even a collision is harmless.
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

        pub fn register(&mut self, fd: RawHandle, token: usize, interest: Interest) -> io::Result<()> {
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

        pub fn modify(&mut self, fd: RawHandle, token: usize, interest: Interest) -> io::Result<()> {
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

        pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<Vec<Event>> {
            let timeout_ms = timeout
                .map(|d| d.as_millis() as i32)
                .unwrap_or(-1);

            // SAFETY: std::mem::zeroed() for an epoll_event array is safe:
            // - epoll_event is a POD type with no invalid bit patterns
            // - All zero bytes represent valid (empty) events
            // - The array is immediately overwritten by epoll_wait
            let mut events: [libc::epoll_event; 256] = unsafe { std::mem::zeroed() };

            loop {
                // SAFETY: epoll_wait requires:
                // - self.epfd is valid (created in new(), owned by self)
                // - events.as_mut_ptr() points to valid, writable memory for 256 events
                // - events.len() correctly reports the array capacity
                // - timeout_ms is a valid i32 (-1 for infinite, >=0 for milliseconds)
                // Error (including EINTR) is checked immediately
                // Thread safety: Poller requires &mut self, ensuring exclusive access
                let ret = unsafe {
                    libc::epoll_wait(self.epfd, events.as_mut_ptr(), events.len() as i32, timeout_ms)
                };

                if ret < 0 {
                    let err = io::Error::last_os_error();
                    if err.kind() == io::ErrorKind::Interrupted {
                        continue; // EINTR - retry
                    }
                    return Err(err);
                }

                let mut result = Vec::with_capacity(ret as usize);
                for ev in events.iter().take(ret as usize) {
                    // Copy the fields out by value: epoll_event is packed on
                    // x86_64, so no references into it may be created.
                    let bits = ev.events;
                    let token = ev.u64 as usize;
                    result.push(Event {
                        token,
                        readable: bits & libc::EPOLLIN as u32 != 0,
                        writable: bits & libc::EPOLLOUT as u32 != 0,
                        error: bits & libc::EPOLLERR as u32 != 0,
                        hangup: bits & libc::EPOLLHUP as u32 != 0,
                    });
                }
                return Ok(result);
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

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd"))]
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

        pub fn register(&mut self, fd: RawHandle, token: usize, interest: Interest) -> io::Result<()> {
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

        pub fn modify(&mut self, fd: RawHandle, token: usize, interest: Interest) -> io::Result<()> {
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

        pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<Vec<Event>> {
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
            let mut events: [Kevent; 256] = unsafe { std::mem::zeroed() };

            loop {
                // SAFETY: kevent() for polling requires:
                // - self.kq is valid (created in new(), owned by self)
                // - changelist is null (no changes to submit)
                // - events.as_mut_ptr() points to valid, writable memory for 256 Kevents
                // - events.len() correctly reports the array capacity
                // - timeout_ptr points to valid Timespec or is null for blocking
                // Error (including EINTR) is checked immediately
                // Thread safety: Poller requires &mut self, ensuring exclusive access
                let ret = unsafe {
                    kevent(
                        self.kq,
                        std::ptr::null(),
                        0,
                        events.as_mut_ptr(),
                        events.len() as i32,
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
                    let ev = &events[i];
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

                return Ok(event_map.into_values().collect());
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

        pub fn register(&mut self, fd: RawHandle, token: usize, interest: Interest) -> io::Result<()> {
            // Check if already registered
            if self.entries.iter().any(|e| e.fd == fd) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "fd already registered",
                ));
            }

            self.entries.push(FdEntry { fd, token, interest });
            Ok(())
        }

        pub fn modify(&mut self, fd: RawHandle, token: usize, interest: Interest) -> io::Result<()> {
            if let Some(entry) = self.entries.iter_mut().find(|e| e.fd == fd) {
                entry.token = token;
                entry.interest = interest;
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "fd not registered",
                ))
            }
        }

        pub fn deregister(&mut self, fd: RawHandle) -> io::Result<()> {
            if let Some(pos) = self.entries.iter().position(|e| e.fd == fd) {
                self.entries.swap_remove(pos);
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "fd not registered",
                ))
            }
        }

        pub fn poll(&mut self, timeout: Option<Duration>) -> io::Result<Vec<Event>> {
            if self.entries.is_empty() {
                // No fds to poll - sleep for timeout and return empty
                if let Some(dur) = timeout {
                    std::thread::sleep(dur);
                }
                return Ok(Vec::new());
            }

            let timeout_ms = timeout
                .map(|d| d.as_millis() as i32)
                .unwrap_or(-1);

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
                let ret = unsafe { WSAPoll(pollfds.as_mut_ptr(), pollfds.len() as u32, timeout_ms) };

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

                let mut result = Vec::new();
                for (i, pollfd) in pollfds.iter().enumerate() {
                    if pollfd.revents != 0 {
                        result.push(Event {
                            token: self.entries[i].token,
                            readable: (pollfd.revents & POLLIN) != 0,
                            writable: (pollfd.revents & POLLOUT) != 0,
                            error: (pollfd.revents & POLLERR) != 0,
                            hangup: (pollfd.revents & POLLHUP) != 0,
                        });
                    }
                }
                return Ok(result);
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

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd"))]
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
// poll. `WakeHandle` is a cloneable `Send + Sync` producer handle whose
// `wake()` writes a coalesced token.
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
            unsafe { libc::close(self.0); }
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
                let n = unsafe {
                    libc::read(self.fd.0, buf.as_mut_ptr() as *mut libc::c_void, 8)
                };
                if n != 8 {
                    break;
                }
            }
        }
    }

    impl WakeHandle {
        /// Signal the reactor. eventfd sums writes, so multiple wakes before a
        /// drain coalesce into a single readiness event.
        pub fn wake(&self) {
            let val: u64 = 1;
            // SAFETY: writing 8 bytes from a u64 to a valid eventfd. A saturated
            // counter returns EAGAIN (EFD_NONBLOCK), which still means a wake is
            // pending; the result is intentionally ignored.
            let _ = unsafe {
                libc::write(self.fd.0, &val as *const u64 as *const libc::c_void, 8)
            };
        }
    }
}

#[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "openbsd", target_os = "netbsd"))]
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
        if let Err(e) = set_nonblocking_cloexec(read_fd)
            .and_then(|_| set_nonblocking_cloexec(write_fd))
        {
            // SAFETY: closing the two fds we just created on the error path.
            unsafe {
                libc::close(read_fd);
                libc::close(write_fd);
            }
            return Err(e);
        }
        let shared = Arc::new(Pipe { read_fd, write_fd });
        Ok((Waker { pipe: shared.clone() }, WakeHandle { pipe: shared }))
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
        pub fn wake(&self) {
            let byte: u8 = 1;
            // SAFETY: writing 1 byte to a valid non-blocking pipe write end. A
            // full pipe returns EAGAIN, which still means a wake is pending.
            let _ = unsafe {
                libc::write(
                    self.pipe.write_fd,
                    &byte as *const u8 as *const libc::c_void,
                    1,
                )
            };
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
    }

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
        Ok((Waker { pair: shared.clone() }, WakeHandle { pair: shared }))
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
        pub fn wake(&self) {
            let byte: i8 = 1;
            // SAFETY: send 1 byte on a valid non-blocking loopback socket; a full
            // send buffer returns WSAEWOULDBLOCK, wake still pending.
            let _ = unsafe { send(self.pair.writer.as_raw_socket(), &byte as *const i8, 1, 0) };
        }
    }
}

pub use waker_backend::{waker_pair, WakeHandle, Waker};

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::io::Write;

    #[test]
    fn test_poller_basic() {
        let mut poller = Poller::new().expect("Failed to create poller");

        // Create a TCP pair for testing
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client.set_nonblocking(true).expect("Failed to set nonblocking");
        let (mut server, _) = listener.accept().expect("Failed to accept");
        server.set_nonblocking(true).expect("Failed to set nonblocking");

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let client_fd = client.as_raw_fd();
            let server_fd = server.as_raw_fd();

            // Register client for readable
            poller.register(client_fd, 1, Interest::READABLE).expect("Failed to register");
            // Register server for writable
            poller.register(server_fd, 2, Interest::WRITABLE).expect("Failed to register");

            // Server should be immediately writable
            let events = poller.poll(Some(Duration::from_millis(100))).expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 2 && e.is_writable()));

            // Write some data from server
            server.write_all(b"hello").expect("Failed to write");

            // Client should become readable
            let events = poller.poll(Some(Duration::from_millis(100))).expect("Failed to poll");
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

            poller.register(client_fd, 1, Interest::READABLE).expect("Failed to register");
            poller.register(server_fd, 2, Interest::WRITABLE).expect("Failed to register");

            let events = poller.poll(Some(Duration::from_millis(100))).expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 2 && e.is_writable()));

            server.write_all(b"hello").expect("Failed to write");

            let events = poller.poll(Some(Duration::from_millis(100))).expect("Failed to poll");
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

        let events = poller
            .poll(Some(Duration::from_millis(200)))
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
    fn test_deregister_no_events() {
        let mut poller = Poller::new().expect("Failed to create poller");

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client.set_nonblocking(true).expect("Failed to set nonblocking");

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let fd = client.as_raw_fd();

            poller.register(fd, 1, Interest::READABLE).expect("Failed to register");
            poller.deregister(fd).expect("Failed to deregister");

            // After deregister, no events should be reported for this fd
            let events = poller.poll(Some(Duration::from_millis(50))).expect("Failed to poll");
            assert!(!events.iter().any(|e| e.token == 1));
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            let fd = client.as_raw_socket();

            poller.register(fd, 1, Interest::READABLE).expect("Failed to register");
            poller.deregister(fd).expect("Failed to deregister");

            let events = poller.poll(Some(Duration::from_millis(50))).expect("Failed to poll");
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

        // No wake yet: a short poll times out with no waker event.
        let events = poller.poll(Some(Duration::from_millis(50))).expect("poll");
        assert!(
            events.iter().all(|e| e.token != WAKER_TOKEN),
            "no wake => no waker event"
        );

        // Two coalesced wakes still produce a single readable event.
        handle.wake();
        handle.wake();
        let events = poller.poll(Some(Duration::from_millis(500))).expect("poll");
        assert!(
            events.iter().any(|e| e.token == WAKER_TOKEN && e.is_readable()),
            "wake() must produce a readable event on WAKER_TOKEN"
        );

        // Drain clears the token; the next poll times out again.
        waker.drain();
        let events = poller.poll(Some(Duration::from_millis(50))).expect("poll");
        assert!(
            events.iter().all(|e| e.token != WAKER_TOKEN),
            "drain() must clear the wake token"
        );

        poller.deregister(waker.raw_handle()).ok();
    }

    #[test]
    fn test_modify_interest() {
        let mut poller = Poller::new().expect("Failed to create poller");

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        client.set_nonblocking(true).expect("Failed to set nonblocking");
        let (server, _) = listener.accept().expect("Failed to accept");
        server.set_nonblocking(true).expect("Failed to set nonblocking");

        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let server_fd = server.as_raw_fd();

            // Register for readable only
            poller.register(server_fd, 1, Interest::READABLE).expect("Failed to register");

            // Modify to writable
            poller.modify(server_fd, 1, Interest::WRITABLE).expect("Failed to modify");

            // Should be writable now
            let events = poller.poll(Some(Duration::from_millis(100))).expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 1 && e.is_writable()));

            poller.deregister(server_fd).expect("Failed to deregister");
        }

        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawSocket;
            let server_fd = server.as_raw_socket();

            poller.register(server_fd, 1, Interest::READABLE).expect("Failed to register");
            poller.modify(server_fd, 1, Interest::WRITABLE).expect("Failed to modify");

            let events = poller.poll(Some(Duration::from_millis(100))).expect("Failed to poll");
            assert!(events.iter().any(|e| e.token == 1 && e.is_writable()));

            poller.deregister(server_fd).expect("Failed to deregister");
        }
    }
}
