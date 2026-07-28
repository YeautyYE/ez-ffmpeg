//! Process-level cache of the wgpu device stack shared by every
//! [`GpuState`](crate::wgpu_filter::gpu_state::GpuState): instance, adapter,
//! device, queue, the optional dmabuf interop, and the adapter-derived
//! direct-pack / texture-limit snapshots.
//!
//! Adapter probing plus device creation costs hundreds of milliseconds to
//! seconds per filter init, and every concurrent job used to open an
//! independent device. The cache keys on [`GpuProfile`] — a basic device and
//! a dmabuf-capable one are different stacks — and hands out one *generation*
//! per profile:
//!
//! - **Single flight**: concurrent first inits of a profile build once. The
//!   build runs OUTSIDE all locks (it takes seconds and logs freely); waiters
//!   block on a condvar and share the published `Arc`.
//! - **Generation death**: any fatal device error (staging map failure,
//!   `Device::poll` error, the readback wedge timeout, an uncaptured device
//!   error) latches the generation dead. Filters bound to it fail promptly
//!   with [`GpuGenerationLost`]; the next acquire replaces the slot with a
//!   freshly built generation. The device is never swapped under a live
//!   `GpuState` — per-filter resources are device-bound.
//! - **Retention**: the cache keeps its `Arc` across jobs on purpose —
//!   cross-job reuse is the point. At most one generation per profile is
//!   retained (a dead or non-reusable one lingers only until the next
//!   acquire replaces it), and the final generation lives until process
//!   exit, reclaimed by the OS/driver like the crate's process-lifetime
//!   hardware devices. A replaced generation's teardown — potentially the
//!   final `Arc`, hence wgpu/driver destructors — always runs OUTSIDE the
//!   slot lock, under the builder's reset guard.
//!
//! Pipelines, shader modules, bind group layouts, uniform buffers, and frame
//! resources stay per-filter; only the device stack is shared. Known residual
//! window: wgpu error scopes are device-global, so one filter's *init* scope
//! (serialized by [`SharedGpuGeneration::init_lock`]) could in principle
//! capture a *runtime* validation error raised concurrently by another
//! filter's frame — but the streaming path pre-rejects the one documented
//! validation producer (oversized textures) and library-built descriptors
//! stay within checked limits, while OOM/Internal errors bypass
//! Validation-filtered scopes entirely, so the window is library-bug-only.

use crate::wgpu_filter::hw_interop::{self, DmabufOpen, HwVulkanInterop};
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

/// Cache key: which device stack a filter needs. Derived solely from
/// `hw_zero_copy_input` — `direct_pack` depends only on the adapter and an
/// env var, and the params length only shapes per-filter bind group layouts,
/// so neither forks the device.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum GpuProfile {
    /// Plain `request_device` stack.
    Basic,
    /// Device opened with the dmabuf-import extensions where the platform
    /// and driver support them (hw zero-copy input). When support is
    /// structurally absent, the profile still owns a separate plain device
    /// and that fact is cached like any other; a TRANSIENT open failure
    /// also falls back to a plain device, but marks the generation
    /// non-reusable so the next acquisition re-probes (see
    /// [`Liveness::is_reusable`]).
    DmabufCapable,
}

impl GpuProfile {
    pub(crate) fn for_hw_input(hw_input: bool) -> Self {
        if hw_input {
            GpuProfile::DmabufCapable
        } else {
            GpuProfile::Basic
        }
    }
}

/// One shared generation of the GPU stack: everything `GpuState` used to
/// build for itself up to (and including) device creation, built exactly
/// once per profile and reused until a fatal device error retires it.
pub(crate) struct SharedGpuGeneration {
    /// Roots the backend stack and pins the sticky adapter selection,
    /// including the default-backend second-chance outcome.
    _instance: wgpu::Instance,
    /// Sticky adapter behind `device`, kept alive with the instance. A
    /// rebuild after death deliberately re-probes instead of reusing it:
    /// after a real device loss the old choice may be stale.
    _adapter: wgpu::Adapter,
    pub(crate) device: wgpu::Device,
    pub(crate) queue: wgpu::Queue,
    /// `Some` only when the [`GpuProfile::DmabufCapable`] build succeeded
    /// through `try_open_dmabuf_device`. Shared across filters: the interop
    /// is internally synchronized, and every holder of this `Arc` also
    /// holds the generation's device alive (see the ownership note in
    /// `hw_interop`).
    pub(crate) hw_interop: Option<Arc<HwVulkanInterop>>,
    /// Adapter + env snapshot (see the decision comment in
    /// [`build_generation`]); fixed for the generation's lifetime.
    pub(crate) direct_pack: bool,
    /// `device.limits().max_texture_dimension_2d` snapshot: `limits()`
    /// rebuilds the whole `Limits` struct on every call, and the per-frame
    /// size check needs only this one constant.
    pub(crate) max_texture_dim: u32,
    /// Death latch. Behind an `Arc` so the uncaptured-error handler captures
    /// the FLAG, never the generation — a generation capture would cycle
    /// generation -> device -> handler -> generation and leak the device
    /// forever. Relaxed on both sides: a monotonic latch guarding no
    /// dependent data.
    dead: Arc<AtomicBool>,
    /// Set when a dmabuf-capable build fell back to a plain device for a
    /// TRANSIENT interop-open failure (see [`Liveness::is_reusable`]): jobs
    /// on this generation run normally, but the next acquire re-probes with
    /// a fresh build instead of pinning the degraded stack for the process
    /// lifetime. Deterministic unsupport leaves this false.
    dmabuf_retry: bool,
    /// Serializes per-filter init sections on the shared device: error
    /// scopes are device-global, so two filters initializing concurrently
    /// could steal each other's validation errors. NO log macros may run
    /// while it is held (house rule: no logging under a lock).
    pub(crate) init_lock: Mutex<()>,
}

impl SharedGpuGeneration {
    /// Latches the generation dead. A plain atomic store — lock-free so it
    /// is callable from the uncaptured-error handler — and idempotent, so
    /// every fatal path may funnel through it unconditionally.
    pub(crate) fn mark_dead(&self) {
        self.dead.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }
}

/// Error identity for jobs whose shared generation was latched dead —
/// possibly by a concurrent job's fatal device error. The job that *caused*
/// the death keeps its original error; only the others observing the latch
/// fail with this. Boxed into `FrameFilterError` at the return sites; tests
/// assert identity via `downcast_ref`, stable against message edits. Never
/// surfaces from `init`, and the message still deliberately avoids the
/// words the GPU-less test skip heuristic matches on.
#[derive(Debug)]
pub(crate) struct GpuGenerationLost;

impl std::fmt::Display for GpuGenerationLost {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "shared wgpu context was lost after a fatal GPU error; this job \
             cannot continue (the next job initializes a fresh one)"
        )
    }
}

impl std::error::Error for GpuGenerationLost {}

/// The properties the cache needs from its payload, factored out so the
/// slot state machine is unit-testable without a GPU.
pub(crate) trait Liveness {
    fn is_dead(&self) -> bool;

    /// Whether the cache may keep serving this generation. Defaults to
    /// plain liveness; an implementation can retire an otherwise-healthy
    /// generation early — it is replaced at the next acquire but never
    /// killed under the jobs already running on it.
    fn is_reusable(&self) -> bool {
        !self.is_dead()
    }
}

impl Liveness for SharedGpuGeneration {
    fn is_dead(&self) -> bool {
        SharedGpuGeneration::is_dead(self)
    }

    /// A generation whose dmabuf-capable build failed its interop open for
    /// a transient reason serves its current jobs but is not re-served: the
    /// next acquire re-probes with a fresh build, restoring the per-init
    /// retry the pre-cache code performed. Deterministic unsupport (wrong
    /// platform/backend/extensions) sets no flag and stays cached.
    fn is_reusable(&self) -> bool {
        !self.is_dead() && !self.dmabuf_retry
    }
}

/// Slot state machine. `Building` marks a build in flight so concurrent
/// acquires wait on the condvar instead of racing a second multi-second
/// build.
enum Slot<T> {
    Idle,
    Building,
    Ready(Arc<T>),
}

struct SlotCell<T> {
    state: Mutex<Slot<T>>,
    cv: Condvar,
}

impl<T> SlotCell<T> {
    const fn new() -> Self {
        SlotCell {
            state: Mutex::new(Slot::Idle),
            cv: Condvar::new(),
        }
    }
}

/// Poison-tolerant lock: every guarded section is a trivial state
/// transition (no wgpu calls, no user code), so a poisoned mutex carries no
/// torn invariant worth propagating (same policy as the interop's modifier
/// cache).
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

thread_local! {
    static BUILDING_SLOTS: std::cell::RefCell<Vec<usize>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

fn slot_id<T>(cell: &SlotCell<T>) -> usize {
    cell as *const SlotCell<T> as usize
}

fn building_on_current_thread<T>(cell: &SlotCell<T>) -> bool {
    let id = slot_id(cell);
    BUILDING_SLOTS.with(|slots| slots.borrow().contains(&id))
}

struct BuilderThread {
    slot: usize,
}

impl BuilderThread {
    fn enter<T>(cell: &SlotCell<T>) -> Self {
        let slot = slot_id(cell);
        BUILDING_SLOTS.with(|slots| slots.borrow_mut().push(slot));
        Self { slot }
    }
}

impl Drop for BuilderThread {
    fn drop(&mut self) {
        BUILDING_SLOTS.with(|slots| {
            let mut slots = slots.borrow_mut();
            if let Some(index) = slots.iter().rposition(|slot| *slot == self.slot) {
                slots.remove(index);
            }
        });
    }
}

/// Resets a slot from `Building` back to `Idle` if the builder panics (wgpu
/// can panic inside device creation), waking waiters so the next one becomes
/// the builder instead of hanging forever. Disarmed on the normal completion
/// path, where the builder publishes the outcome itself.
struct BuildingGuard<'a, T> {
    cell: &'a SlotCell<T>,
    armed: bool,
}

impl<T> Drop for BuildingGuard<'_, T> {
    fn drop(&mut self) {
        if self.armed {
            *lock(&self.cell.state) = Slot::Idle;
            self.cell.cv.notify_all();
        }
    }
}

/// Per-profile single-flight slots. The key space is exactly two values
/// fixed at compile time, so two named cells (each with its own lock) beat a
/// keyed map: exhaustive dispatch, no hashing, and a basic acquisition never
/// waits behind a dmabuf build.
pub(crate) struct GenCache<T> {
    basic: SlotCell<T>,
    dmabuf: SlotCell<T>,
}

impl<T> GenCache<T> {
    pub(crate) const fn new() -> Self {
        GenCache {
            basic: SlotCell::new(),
            dmabuf: SlotCell::new(),
        }
    }

    fn cell(&self, profile: GpuProfile) -> &SlotCell<T> {
        match profile {
            GpuProfile::Basic => &self.basic,
            GpuProfile::DmabufCapable => &self.dmabuf,
        }
    }
}

impl<T: Liveness> GenCache<T> {
    /// Returns the profile's live generation, running `build` if the slot is
    /// empty or holds a dead one. `build` runs with NO lock held; concurrent
    /// acquires of the same profile wait on the condvar and share the
    /// result. A failed build resets the slot and the failing caller keeps
    /// its own error, so the next acquire retries — matching the previous
    /// per-init failure reporting on GPU-less machines, just serialized.
    pub(crate) fn acquire_with(
        &self,
        profile: GpuProfile,
        build: impl FnOnce() -> Result<Arc<T>, String>,
    ) -> Result<Arc<T>, String> {
        let cell = self.cell(profile);
        let mut state = lock(&cell.state);
        loop {
            match &*state {
                Slot::Ready(generation) if generation.is_reusable() => {
                    return Ok(Arc::clone(generation));
                }
                Slot::Building => {
                    // The builder runs without the slot mutex, but a logger or
                    // dependency callback on that same thread can re-enter the
                    // cache. Waiting here would wait on this very call to return.
                    if building_on_current_thread(cell) {
                        return Err(
                            "shared GPU generation initialization re-entered on its builder thread"
                                .to_string(),
                        );
                    }
                    // Re-dispatch on whatever state the builder left behind:
                    // Ready is shared, Idle (failed or panicked build) makes
                    // this waiter the next builder.
                    state = cell.cv.wait(state).unwrap_or_else(PoisonError::into_inner);
                }
                // Idle, or Ready holding a non-reusable generation: become
                // the builder. A retired Arc stays alive for its remaining
                // holders; it merely loses its slot to the replacement.
                _ => {
                    // Take the retired value OUT under the lock but drop it
                    // only after the lock is released and the reset guard is
                    // armed: if this was the final Arc, wgpu/driver teardown
                    // runs here, and neither a blocking nor a panicking
                    // destructor may execute under the slot mutex (a panic
                    // is converted by the guard into a clean Idle + wake).
                    let retired = std::mem::replace(&mut *state, Slot::Building);
                    drop(state);
                    let mut guard = BuildingGuard { cell, armed: true };
                    let builder_thread = BuilderThread::enter(cell);
                    drop(retired);
                    let result = build();
                    guard.armed = false;
                    *lock(&cell.state) = match &result {
                        Ok(generation) => Slot::Ready(Arc::clone(generation)),
                        Err(_) => Slot::Idle,
                    };
                    cell.cv.notify_all();
                    drop(builder_thread);
                    return result;
                }
            }
        }
    }
}

/// The process-wide cache. Const-initialized: no lazy wrapper, and no
/// destructor at exit — statics are never dropped, so the final generation
/// is reclaimed by the OS/driver (and stays reachable, so leak checkers do
/// not report it).
static CACHE: GenCache<SharedGpuGeneration> = GenCache::new();

pub(crate) struct DeferredInfoLog {
    target: &'static str,
    message: String,
}

impl DeferredInfoLog {
    pub(crate) fn new(target: &'static str, message: String) -> Self {
        Self { target, message }
    }
}

pub(crate) fn emit_deferred_info(logs: Vec<DeferredInfoLog>) {
    for record in logs {
        info!(target: record.target, "{}", record.message);
    }
}

/// Acquires the shared generation for `profile`, building one on first use
/// or after the previous generation died.
pub(crate) fn acquire(profile: GpuProfile) -> Result<Arc<SharedGpuGeneration>, String> {
    let mut deferred = Vec::new();
    let result = CACHE.acquire_with(profile, || build_generation(profile, &mut deferred));
    // `acquire_with` has published Ready (or restored Idle) before returning,
    // so a logger that initializes another filter can never wait on this build.
    emit_deferred_info(deferred);
    result
}

/// Builds one complete generation: instance, adapter (with the
/// default-backend second chance), the direct-pack decision, the device —
/// through the dmabuf path for [`GpuProfile::DmabufCapable`] — plus the
/// uncaptured-error handler and the limit snapshot. Runs outside the cache
/// mutex while its slot is logically `Building`; user-facing records are
/// collected for [`acquire`] to emit after that state is settled.
pub(crate) fn build_generation(
    profile: GpuProfile,
    deferred: &mut Vec<DeferredInfoLog>,
) -> Result<Arc<SharedGpuGeneration>, String> {
    // Prefer the primary backends: probing GL/EGL costs ~30-40ms of init
    // and spams warnings on headless boxes. Fall back to the full set so
    // GL-only machines keep working exactly as before.
    let mut instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
        backends: wgpu::Backends::PRIMARY,
        ..Default::default()
    });
    let request = wgpu::RequestAdapterOptions {
        power_preference: wgpu::PowerPreference::HighPerformance,
        force_fallback_adapter: false,
        compatible_surface: None,
    };
    let adapter = match pollster::block_on(instance.request_adapter(&request)) {
        Ok(adapter) => adapter,
        Err(_) => {
            instance = wgpu::Instance::new(&wgpu::InstanceDescriptor::default());
            pollster::block_on(instance.request_adapter(&request))
                .map_err(|e| format!("No suitable GPU adapter found: {e}"))?
        }
    };

    let adapter_info = adapter.get_info();
    deferred.push(DeferredInfoLog::new(
        module_path!(),
        format!(
            "WgpuFrameFilter adapter: {} ({:?}, {:?})",
            adapter_info.name, adapter_info.backend, adapter_info.device_type
        ),
    ));

    // On unified-memory GPUs the pack pass can write straight into the
    // mappable readback buffer, skipping a full copy of the packed frame.
    // Discrete GPUs keep the copy: mappable memory is slow to write over
    // PCIe there. EZ_WGPU_DISABLE_DIRECT_PACK=1 forces the copy path
    // (internal A/B and fallback-coverage knob). The env var is snapshotted
    // per generation: toggling it mid-process takes effect when the next
    // generation is built, not per filter init.
    let direct_pack = adapter_info.device_type == wgpu::DeviceType::IntegratedGpu
        && adapter
            .features()
            .contains(wgpu::Features::MAPPABLE_PRIMARY_BUFFERS)
        && std::env::var("EZ_WGPU_DISABLE_DIRECT_PACK").as_deref() != Ok("1");
    let device_desc = wgpu::DeviceDescriptor {
        required_features: if direct_pack {
            wgpu::Features::MAPPABLE_PRIMARY_BUFFERS
        } else {
            wgpu::Features::empty()
        },
        ..Default::default()
    };
    if direct_pack {
        deferred.push(DeferredInfoLog::new(
            module_path!(),
            "WgpuFrameFilter: direct pack readback enabled (unified memory)".to_string(),
        ));
    }

    // Zero-copy hardware input needs the device opened with dmabuf-import
    // extensions; when that is not possible the profile still works, hw
    // frames just take the download path.
    let (device, queue, hw_interop, dmabuf_retry) = match profile {
        GpuProfile::DmabufCapable => {
            match hw_interop::try_open_dmabuf_device(&adapter, &device_desc, deferred) {
                DmabufOpen::Opened(device, queue, interop) => {
                    (device, queue, Some(Arc::new(interop)), false)
                }
                outcome @ (DmabufOpen::Unsupported | DmabufOpen::TransientFailure) => {
                    // Fall back to a plain device either way; a TRANSIENT
                    // open failure additionally marks the generation
                    // non-reusable so the next acquisition re-probes
                    // (deterministic unsupport is cached for good).
                    let retry = matches!(outcome, DmabufOpen::TransientFailure);
                    let (device, queue) = pollster::block_on(adapter.request_device(&device_desc))
                        .map_err(|e| format!("Failed to create wgpu device: {e}"))?;
                    (device, queue, None, retry)
                }
            }
        }
        GpuProfile::Basic => {
            let (device, queue) = pollster::block_on(adapter.request_device(&device_desc))
                .map_err(|e| format!("Failed to create wgpu device: {e}"))?;
            (device, queue, None, false)
        }
    };

    // Make fatal device errors observable before they panic. wgpu's default
    // handler just panics the offending thread; this one latches the death
    // flag first so every other filter on the generation fails promptly
    // with GpuGenerationLost instead of waiting out its own wedge timeout,
    // then preserves the panic semantics (and message shape) of the default
    // handler. The closure captures only the flag Arc — see the `dead`
    // field docs for why it must never capture the generation.
    let dead = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&dead);
    device.on_uncaptured_error(Box::new(move |error| {
        flag.store(true, Ordering::Relaxed);
        panic!("wgpu error: {error}\n");
    }));

    Ok(Arc::new(SharedGpuGeneration {
        max_texture_dim: device.limits().max_texture_dimension_2d,
        _instance: instance,
        _adapter: adapter,
        device,
        queue,
        hw_interop,
        direct_pack,
        dead,
        dmabuf_retry,
        init_lock: Mutex::new(()),
    }))
}
