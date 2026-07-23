use crate::util::ffmpeg_utils::av_err2str;
use ffmpeg_sys_next::{
    av_buffer_unref, av_dict_free, av_dict_parse_string, av_hwdevice_ctx_create,
    av_hwdevice_ctx_create_derived, av_hwdevice_find_type_by_name, av_hwdevice_get_type_name,
    av_hwdevice_iterate_types, avcodec_get_hw_config, avfilter_get_by_name, AVBufferRef, AVCodec,
    AVDictionary, AVHWDeviceType, AVERROR, AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX, EINVAL, ENOMEM,
};
use log::{error, warn};
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex, OnceLock};

#[derive(Clone, Debug)]
pub struct HWAccelInfo {
    pub name: String,
    pub hw_device_type: AVHWDeviceType,
}

pub fn get_hwaccels() -> Vec<HWAccelInfo> {
    let mut hwaccels = Vec::new();
    let mut device_type = AVHWDeviceType::AV_HWDEVICE_TYPE_NONE;

    loop {
        device_type = unsafe { av_hwdevice_iterate_types(device_type) };
        if device_type == AVHWDeviceType::AV_HWDEVICE_TYPE_NONE {
            break;
        }

        let name = hw_device_type_name(device_type)
            .unwrap_or("unknown name")
            .to_string();

        hwaccels.push(HWAccelInfo {
            name,
            hw_device_type: device_type,
        });
    }

    hwaccels
}

static HW_DEVICES: OnceLock<Mutex<Vec<HWDevice>>> = OnceLock::new();

/// Serializes the tests that REPLACE, register into, or consume hardware
/// devices from the process-global `HW_DEVICES` registry — this module's
/// snapshot/sentinel tests and the macOS videotoolbox scheduler tests. The
/// snapshot tests swap the global table out for their whole body; a
/// hardware test running concurrently in the same test binary would
/// otherwise register into (or resolve from) the sentinel table and be
/// wiped by the snapshot restore. Tests that merely pass through a
/// `hw_device_for_filter()` call on an untouched registry (ordinary filter
/// graph tests) do not need this lock.
#[cfg(test)]
pub(crate) static HW_REGISTRY_TEST_LOCK: Mutex<()> = Mutex::new(());
// Serializes `hw_device_init_from_string` and `hw_device_init_from_type`
// end-to-end so the reuse check and the registration cannot interleave across
// threads (see the function comments).
static INIT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn init_lock() -> &'static Mutex<()> {
    INIT_LOCK.get_or_init(|| Mutex::new(()))
}
// Owning handle to the explicitly configured filter device (ffmpeg_hw.c
// filter_hw_device). Holding the HANDLE (not just a name) pins the device
// across bounded-LRU eviction: even if the registry evicts its entry, the
// explicit selection keeps its context alive and keeps resolving to the
// SAME physical device — a registry lookup by name could otherwise resolve
// a re-issued auto-name to a different device.
static FILTER_HW_DEVICE: OnceLock<Mutex<Option<HWDevice>>> = OnceLock::new();

pub(crate) fn new_hw_devices() -> Mutex<Vec<HWDevice>> {
    Mutex::new(Vec::new())
}

pub(crate) fn init_filter_hw_device(hw_device: &str) -> i32 {
    if FILTER_HW_DEVICE.get().is_some() {
        warn!("Only one filter device can be used.");
        return 0;
    }
    match hw_device_init_from_string(hw_device) {
        (0, Some(dev)) => {
            FILTER_HW_DEVICE.set(Mutex::new(Some(dev))).ok();
            0
        }
        (_, _) => {
            error!("Invalid filter device {}", hw_device);
            FILTER_HW_DEVICE.set(Mutex::new(None)).ok();
            AVERROR(EINVAL)
        }
    }
}

#[repr(i32)]
#[derive(Copy, Clone, PartialEq)]
pub enum HWAccelID {
    HwaccelNone = 0,
    HwaccelAuto,
    HwaccelGeneric,
}

#[derive(Debug)]
pub(crate) struct HWDevice {
    pub(crate) name: String,
    pub(crate) device_type: AVHWDeviceType,
    /// Shared ownership of the ONE AVBufferRef created for this device.
    /// Cloning an `HWDevice` clones the `Arc` — infallible, no FFI — so a
    /// handle can never silently hold a null reference (`av_buffer_ref` is
    /// allowed to fail, which ruled out ref-per-clone). The wrapped
    /// reference is released when the last handle (registry entry, pinned
    /// filter slot, or consumer-held clone) drops; consumers attach their
    /// own `av_buffer_ref(dev.device_ref())` on top for FFmpeg contexts.
    device: Arc<OwnedDeviceRef>,
    /// The reuse key this device was registered under, matched by
    /// `reuse_move_to_back` so an identical request hands back the existing
    /// context instead of creating a new one: the exact spec string for
    /// `hw_device_init_from_string`, or the canonical `":{type}[:{device}]"`
    /// key for `hw_device_init_from_type` (see `type_init_arg` for why the two
    /// schemes cannot collide). `None` never matches the reuse lookup.
    pub(crate) init_arg: Option<String>,
}

/// The single owning reference behind every handle to one device context.
/// Dropped (and the context's refcount released) only when the last `Arc`
/// clone goes.
#[derive(Debug)]
struct OwnedDeviceRef(*mut AVBufferRef);

impl Drop for OwnedDeviceRef {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: this is the creation reference, owned exclusively by
            // this wrapper; the last Arc clone dropping is the only caller.
            unsafe {
                av_buffer_unref(&mut self.0);
            }
        }
    }
}

// SAFETY: the wrapper only carries the pointer; releasing it from whichever
// thread drops last is safe because the AVBufferRef refcount is atomic.
unsafe impl Send for OwnedDeviceRef {}
// SAFETY: shared access is read-only (the pointer value); the only mutation
// is in Drop, which the Arc guarantees runs exactly once with no other
// reader left.
unsafe impl Sync for OwnedDeviceRef {}

impl HWDevice {
    fn new(
        name: String,
        device_type: AVHWDeviceType,
        device_ref: *mut AVBufferRef,
        init_arg: Option<String>,
    ) -> Self {
        HWDevice {
            name,
            device_type,
            device: Arc::new(OwnedDeviceRef(device_ref)),
            init_arg,
        }
    }

    /// Raw device-context reference for FFI. Valid as long as this handle
    /// (or any clone sharing its `Arc`) is alive; callers wanting to keep
    /// the context beyond that take their own `av_buffer_ref` on it.
    pub(crate) fn device_ref(&self) -> *mut AVBufferRef {
        self.device.0
    }
}

impl Clone for HWDevice {
    fn clone(&self) -> Self {
        HWDevice {
            name: self.name.clone(),
            device_type: self.device_type,
            device: Arc::clone(&self.device),
            init_arg: self.init_arg.clone(),
        }
    }
}

// SAFETY: the AVBufferRef refcount is atomic, so the shared owning
// reference may be moved across threads and released from whichever thread
// drops last. (`HWDevice` is also `Sync` via its auto impl — every field
// is `Sync`, including `Arc<OwnedDeviceRef>` through the wrapper's
// explicit read-only `Sync`.)
unsafe impl Send for HWDevice {}

pub(crate) unsafe fn hw_device_free_all() {
    // Release the pinned filter-device handle (its context survives if a
    // consumer still holds a reference).
    if let Some(slot) = FILTER_HW_DEVICE.get() {
        if let Ok(mut slot) = slot.lock() {
            slot.take();
        }
    }

    // Drop every registry entry: each HWDevice's Drop releases the
    // registry's reference (contexts still held by live consumer refs stay
    // alive until those release).
    if let Some(hw_devices) = HW_DEVICES.get() {
        match hw_devices.lock() {
            Ok(mut devices_guard) => {
                devices_guard.clear();
            }
            Err(e) => {
                error!("Failed to lock hardware device list: {}", e);
            }
        }
    }
}

/// Name of a hardware device type, `None` when the linked FFmpeg has no
/// name for it — `av_hwdevice_get_type_name` returns NULL then, notably
/// for `AV_HWDEVICE_TYPE_NONE` — so log paths never hand a NULL to
/// `CStr::from_ptr`.
pub(crate) fn hw_device_type_name(device_type: AVHWDeviceType) -> Option<&'static str> {
    // SAFETY: av_hwdevice_get_type_name returns NULL or a pointer into a
    // static name table in libavutil, so a non-NULL result is a valid C
    // string with static lifetime.
    unsafe {
        let name = av_hwdevice_get_type_name(device_type);
        if name.is_null() {
            None
        } else {
            CStr::from_ptr(name).to_str().ok()
        }
    }
}

pub(crate) fn hw_device_for_filter() -> Option<HWDevice> {
    if let Some(slot) = FILTER_HW_DEVICE.get() {
        let slot = slot.lock().unwrap();
        if let Some(dev) = slot.as_ref() {
            // An explicitly configured filter device wins. The slot owns its
            // handle, so the selection survives registry eviction unchanged.
            return Some(dev.clone());
        }
    }
    let devices = HW_DEVICES.get_or_init(new_hw_devices);

    let devices = devices.lock().unwrap();
    // Only a device with a concrete type can back a filter, so the default
    // skips AV_HWDEVICE_TYPE_NONE entries — the by-type and by-codec lookups
    // match on a real type and never see them, and a NONE entry's type has
    // no name, which would send NULL through the advisory warn below. The
    // newest concretely-typed registration wins, matching the previous
    // last() pick.
    if let Some(dev) = devices
        .iter()
        .rev()
        .find(|dev| dev.device_type != AVHWDeviceType::AV_HWDEVICE_TYPE_NONE)
    {
        if devices.len() > 1 {
            let type_name = hw_device_type_name(dev.device_type).unwrap_or("unknown");
            warn!("There are {} hardware devices. device {} of type {type_name} is picked for filters by default. Set hardware device explicitly with the filter_hw_device option if device {} is not usable for filters.",
            devices.len(),dev.name,
            dev.name,);
        }

        return Some(dev.clone());
    }

    None
}

pub(crate) fn hw_device_match_by_codec(codec: *const AVCodec) -> Option<HWDevice> {
    let mut i = 0;

    loop {
        let config = unsafe { avcodec_get_hw_config(codec, i) };
        if config.is_null() {
            return None;
        }

        unsafe {
            if (*config).methods as u32 & AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX as u32 == 0 {
                i += 1;
                continue;
            }

            if let Some(dev) = hw_device_get_by_type((*config).device_type) {
                return Some(dev.clone());
            }
        }

        i += 1;
    }
}

pub(crate) fn hw_device_get_by_type(device_type: AVHWDeviceType) -> Option<HWDevice> {
    let mut found = None;

    let devices = HW_DEVICES.get_or_init(new_hw_devices);
    let devices = devices.lock().unwrap();
    for device in devices.iter() {
        if device.device_type == device_type {
            if found.is_some() {
                return None;
            }
            found = Some(device.clone());
        }
    }
    found
}

/// Split a device specification into the device type name and the remainder
/// starting at the first ':', '=' or '@' separator (e.g. "cuda:0" -> ("cuda", ":0")).
fn split_device_type(arg: &str) -> (&str, &str) {
    // k is a byte offset of an ASCII separator (or arg.len()), so both
    // slices split at a char boundary.
    let k = arg.find([':', '=', '@']).unwrap_or(arg.len());
    // The type is the prefix BEFORE the separator (ffmpeg_hw.c
    // hw_device_init_from_string: av_strndup(arg, k)).
    (&arg[..k], &arg[k..])
}

/// Parse the ":device[,key=value...]" tail of a device specification into
/// the device name and the options string. The leading ':' is a separator,
/// not part of the device name (ffmpeg_hw.c skips it with `++p` first).
fn split_device_and_options(p: &str) -> (Option<&str>, Option<&str>) {
    let rest = p.strip_prefix(':').unwrap_or(p);
    match rest.find(',') {
        Some(comma_pos) => (
            (comma_pos > 0).then(|| &rest[..comma_pos]),
            Some(&rest[comma_pos + 1..]),
        ),
        None => (if rest.is_empty() { None } else { Some(rest) }, None),
    }
}

/// Frees an `AVDictionary` on drop. `av_hwdevice_ctx_create` reads (and may
/// partially consume) the options dict but never takes ownership, so every caller
/// must free it -- on success AND on every error path. The parsed options
/// previously leaked on all paths; wrapping the pointer here closes that leak.
struct DictGuard(*mut AVDictionary);

impl Drop for DictGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { av_dict_free(&mut self.0) };
        }
    }
}

pub(crate) fn hw_device_init_from_string(arg: &str) -> (i32, Option<HWDevice>) {
    // Serialize the whole reuse-check -> create -> register sequence: two
    // concurrent calls with the same spec must not both miss the reuse check and
    // each create (and permanently register) a device, which would leak a context
    // and mint a duplicate auto name. Hardware-device setup is per-job, not
    // per-frame, so this coarse lock is off every hot path.
    let _init_guard = init_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    let mut device_ref = null_mut();

    let (type_str, mut p) = split_device_type(arg);

    let Ok(type_name) = CString::new(type_str) else {
        error!("Device creation failed: type:{type_str} can't convert to CString");
        return (AVERROR(ENOMEM), None);
    };
    let device_type = unsafe { av_hwdevice_find_type_by_name(type_name.as_ptr()) };
    if device_type == AVHWDeviceType::AV_HWDEVICE_TYPE_NONE {
        error!("Invalid device specification \"{arg}\": unknown device type");
        return (AVERROR(EINVAL), None);
    }

    // A long-running service re-runs the same hwaccel spec on every job. Auto
    // device names (vaapi0, vaapi1, ...) never match on lookup, so each call used
    // to create and permanently retain a new device context; after enough jobs
    // av_hwdevice_default_name exhausts its names and hardware init returns ENOMEM.
    // Reuse the device created for an identical spec (the context is a refcounted
    // AVBufferRef shared safely across jobs; the list is still freed once at
    // process cleanup, so no per-job free can dangle a shared ref). Reuse moves the
    // entry to the BACK of the list so it stays the "last-initialized" default
    // filter device (hw_device_for_filter picks the newest concretely-typed
    // entry).
    //
    // The check deliberately sits AFTER the type-name validation above: a spec
    // that registers always starts with a valid device type name, so a malformed
    // spec beginning with a separator (":vaapi:...") errors out here and can never
    // alias a `hw_device_init_from_type` reuse key, which deliberately starts
    // with ':' (see `type_init_arg`).
    if let Some(existing) = reuse_by_init_arg_move_to_back(arg) {
        return (0, Some(existing));
    }

    let name = if p.starts_with('=') {
        let name_end = p[1..].find([':', '@', ',']).unwrap_or(p.len() - 1);
        let name = Some(p[1..=name_end].to_string());

        if hw_device_get_by_name(&name.clone().unwrap()).is_some() {
            error!("Invalid device specification \"{arg}\": named device already exists");
            return (AVERROR(EINVAL), None);
        }

        let new_p_index = 1 + name_end;
        p = &p[new_p_index..];
        name
    } else {
        hw_device_default_name(device_type)
    };

    if p.is_empty() {
        // New device with no parameters.
        let err =
            unsafe { av_hwdevice_ctx_create(&mut device_ref, device_type, null(), null_mut(), 0) };
        if err < 0 {
            error!("Device creation failed: {err}.");
            unsafe {
                av_buffer_unref(&mut device_ref);
            }
            return (err, None);
        }
    } else if p.starts_with(':') {
        // New device with some parameters.
        let (device_name, options_str) = split_device_and_options(p);
        // Freed on every path below (parse error, create error, success).
        let mut options = DictGuard(null_mut());

        if let Some(v) = options_str {
            unsafe {
                let Ok(v_cstr) = CString::new(v) else {
                    error!("Device creation failed: option:{v} can't convert to CString");
                    av_buffer_unref(&mut device_ref);
                    return (AVERROR(EINVAL), None);
                };
                let eq_cstr = CString::new("=").unwrap();
                let comma_cstr = CString::new(",").unwrap();
                let err = av_dict_parse_string(
                    &mut options.0,
                    v_cstr.as_ptr(),
                    eq_cstr.as_ptr(),
                    comma_cstr.as_ptr(),
                    0,
                );
                if err < 0 {
                    error!("Invalid device specification \"{arg}\": failed to parse options");
                    av_buffer_unref(&mut device_ref);
                    return (AVERROR(EINVAL), None);
                }
            }
        }

        let err = unsafe {
            match device_name {
                None => av_hwdevice_ctx_create(&mut device_ref, device_type, null(), options.0, 0),
                Some(device_name) => {
                    let Ok(device_name_cstr) = CString::new(device_name) else {
                        error!("Device creation failed: device_name:{device_name} can't convert to CString");
                        av_buffer_unref(&mut device_ref);
                        return (AVERROR(EINVAL), None);
                    };
                    av_hwdevice_ctx_create(
                        &mut device_ref,
                        device_type,
                        device_name_cstr.as_ptr(),
                        options.0,
                        0,
                    )
                }
            }
        };
        if err < 0 {
            error!("Device creation failed: {err}.");
            unsafe {
                av_buffer_unref(&mut device_ref);
            }
            return (err, None);
        }
    } else if let Some(src_name) = p.strip_prefix('@') {
        // Derive from existing device.
        let Some(src_device) = hw_device_get_by_name(src_name) else {
            error!("Invalid device specification \"{arg}\": invalid source device name");
            unsafe {
                av_buffer_unref(&mut device_ref);
            }
            return (AVERROR(EINVAL), None);
        };
        let err = unsafe {
            av_hwdevice_ctx_create_derived(&mut device_ref, device_type, src_device.device_ref(), 0)
        };
        if err < 0 {
            error!("Device creation failed: {err}.");
            unsafe {
                av_buffer_unref(&mut device_ref);
            }
            return (err, None);
        }
    } else if let Some(v) = p.strip_prefix(',') {
        unsafe {
            // Freed on every path below (parse error, create error, success).
            let mut options = DictGuard(null_mut());
            let Ok(v_cstr) = CString::new(v) else {
                error!("Device creation failed: option:{v} can't convert to CString");
                av_buffer_unref(&mut device_ref);
                return (AVERROR(EINVAL), None);
            };
            let eq_cstr = CString::new("=").unwrap();
            let comma_cstr = CString::new(",").unwrap();
            let mut err = av_dict_parse_string(
                &mut options.0,
                v_cstr.as_ptr(),
                eq_cstr.as_ptr(),
                comma_cstr.as_ptr(),
                0,
            );
            if err < 0 {
                error!("Invalid device specification \"{arg}\": failed to parse options");
                av_buffer_unref(&mut device_ref);
                return (AVERROR(EINVAL), None);
            }
            err = av_hwdevice_ctx_create(&mut device_ref, device_type, null(), options.0, 0);
            if err < 0 {
                error!("Device creation failed: {err}.");
                av_buffer_unref(&mut device_ref);
                return (err, None);
            }
        }
    } else {
        error!("Invalid device specification \"{arg}\": parse error");
        return (AVERROR(EINVAL), None);
    }

    let dev = HWDevice::new(
        name.unwrap(),
        device_type,
        device_ref,
        Some(arg.to_string()),
    );
    add_hw_device(dev.clone());

    (0, Some(dev))
}

pub(crate) fn hw_device_init_from_type(
    device_type: AVHWDeviceType,
    device: Option<String>,
) -> (i32, Option<HWDevice>) {
    // Same serialization as `hw_device_init_from_string`: the whole reuse-check
    // -> create -> register sequence must not interleave across threads, or two
    // concurrent identical requests would each create (and permanently register)
    // a device. Hardware-device setup is per-job, not per-frame, so this coarse
    // lock is off every hot path.
    let _init_guard = init_lock()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    // The decode path funnels every job through here: devices register under
    // auto-generated names ("vaapi0", ...), so the caller's hw_device_get_by_name
    // lookup never matches and, without reuse, each job would create and
    // permanently retain a new context until hw_device_default_name exhausts its
    // names and returns ENOMEM. Key the registration on the requested
    // (type, device) pair and reuse the existing device, with the same
    // move-to-back semantics as the from_string reuse (the reused device stays
    // the "last-initialized" default filter device). A key is only unavailable
    // for a type with no name, which cannot be created (or registered) anyway.
    let init_arg = type_init_arg(device_type, device.as_deref());
    if let Some(init_arg) = init_arg.as_deref() {
        if let Some(existing) = reuse_by_init_arg_move_to_back(init_arg) {
            return (0, Some(existing));
        }
    }

    let name = hw_device_default_name(device_type);
    if name.is_none() {
        return (AVERROR(ENOMEM), None);
    }

    let mut device_ref = null_mut();

    let err = match device.as_deref() {
        None => unsafe {
            av_hwdevice_ctx_create(&mut device_ref, device_type, null(), null_mut(), 0)
        },
        Some(device) => {
            let Ok(device_cstr) = CString::new(device) else {
                return (AVERROR(EINVAL), None);
            };

            unsafe {
                av_hwdevice_ctx_create(
                    &mut device_ref,
                    device_type,
                    device_cstr.as_ptr(),
                    null_mut(),
                    0,
                )
            }
        }
    };

    if err < 0 {
        error!("Device creation failed: {}.", err);
        unsafe {
            av_buffer_unref(&mut device_ref);
        }
        return (err, None);
    }

    let dev = register_from_type_device(name.unwrap(), device_type, device_ref, device.as_deref());

    (0, Some(dev))
}

/// Registers a device created by `hw_device_init_from_type`, recording the
/// canonical `type_init_arg` reuse key so the NEXT identical (type, device)
/// request finds it. Split out so the key-recording contract is unit-testable
/// without hardware: production and test drive the same registration path.
fn register_from_type_device(
    name: String,
    device_type: AVHWDeviceType,
    device_ref: *mut AVBufferRef,
    device: Option<&str>,
) -> HWDevice {
    let dev = HWDevice::new(
        name,
        device_type,
        device_ref,
        type_init_arg(device_type, device),
    );
    add_hw_device(dev.clone());
    dev
}

/// Canonical reuse key recorded as `init_arg` for devices registered by
/// `hw_device_init_from_type`, so a later identical (type, device) request can
/// reuse the registered context instead of creating a new one.
///
/// Key scheme: `":{type_name}"` for a type-only request, `":{type_name}:{device}"`
/// when a device string was requested. Consequences, both deliberate:
/// - No collision with `hw_device_init_from_string` keys in either direction:
///   from_string records the raw spec string and only ever registers (or looks
///   up) specs whose type prefix passed av_hwdevice_find_type_by_name, so its
///   keys never start with ':'; keys built here always do.
/// - A type-only request only reuses a type-only registration, never a
///   device-specific one (they may be different physical devices), and vice
///   versa (`":vaapi"` vs `":vaapi:/dev/dri/renderD128"`).
///
/// Returns `None` when the type has no name (unknown to the linked FFmpeg):
/// such a device cannot be created, so nothing is ever registered for it.
fn type_init_arg(device_type: AVHWDeviceType, device: Option<&str>) -> Option<String> {
    let type_name = unsafe { av_hwdevice_get_type_name(device_type) };
    if type_name.is_null() {
        return None;
    }
    let type_name = unsafe { CStr::from_ptr(type_name) }.to_str().ok()?;
    Some(match device {
        None => format!(":{type_name}"),
        Some(device) => format!(":{type_name}:{device}"),
    })
}

pub(crate) fn hw_device_default_name(device_type: AVHWDeviceType) -> Option<String> {
    // Get the name of the hardware device type
    let type_name = unsafe { av_hwdevice_get_type_name(device_type) };
    if type_name.is_null() {
        return None;
    }

    let type_name = unsafe { CStr::from_ptr(type_name) }.to_str().ok()?;
    let index_limit = 1000;

    for index in 0..index_limit {
        let name = format!("{}{}", type_name, index);

        // Check if the name is available
        if hw_device_get_by_name(&name).is_none() {
            return Some(name);
        }
    }

    None
}

pub(crate) fn hw_device_get_by_name(name: &str) -> Option<HWDevice> {
    let devices = HW_DEVICES.get_or_init(new_hw_devices);

    let devices = devices.lock().unwrap();
    for device in devices.iter() {
        if device.name == name {
            return Some(device.clone());
        }
    }

    None
}

/// Reuses the device already registered under the same reuse key (a from_string
/// spec string or a from_type canonical key), if any, so an identical request
/// reuses it instead of leaking a fresh context — moving the matched entry to
/// the back of the process-global list so it stays the last-initialized default
/// filter device.
fn reuse_by_init_arg_move_to_back(arg: &str) -> Option<HWDevice> {
    let devices = HW_DEVICES.get_or_init(new_hw_devices);
    let mut devices = devices.lock().unwrap();
    reuse_move_to_back(&mut devices, arg)
}

/// Pure reuse over a device list: a device is reusable for `arg` only if its
/// recorded reuse key equals `arg` exactly — a from_string spec string or a
/// from_type canonical key; the two schemes cannot collide (see `type_init_arg`)
/// and devices with `init_arg == None` never match. On a hit the entry is moved
/// to the BACK of the list — `hw_device_for_filter` picks the newest
/// concretely-typed entry, so reuse must preserve the "last-initialized
/// wins" default-filter semantics.
/// Split out so the reuse behavior is unit-testable without the process-global
/// list.
fn reuse_move_to_back(devices: &mut Vec<HWDevice>, arg: &str) -> Option<HWDevice> {
    let idx = devices
        .iter()
        .position(|device| device.init_arg.as_deref() == Some(arg))?;
    let device = devices.remove(idx);
    devices.push(device.clone());
    Some(device)
}

/// Registry size bound. Reuse (both key schemes) moves an entry to the back,
/// so the FRONT is the least-recently-used and is what eviction drops.
/// Without a cap a long-lived service cycling through DISTINCT specs (or
/// alias spellings of one device) grows the registry forever and eventually
/// exhausts the auto-generated name space. 32 comfortably covers every
/// simultaneous-distinct-device workload (a machine has a handful of
/// accelerators; the cap only bounds distinct SPEC strings).
const HW_DEVICES_CAP: usize = 32;

fn add_hw_device(device: HWDevice) {
    let devices = HW_DEVICES.get_or_init(new_hw_devices);
    let mut devices = devices.lock().unwrap();
    devices.push(device);
    // Bounded LRU under the registry lock. Evicting drops only the
    // REGISTRY's reference (HWDevice owns its ref; see Clone/Drop): every
    // consumer path clones its handle while the lock is held, so an evicted
    // device's context is freed by the last consumer handle, never under a
    // live job. An evicted-but-alive device is simply no longer findable —
    // get_by_name/get_by_type/for_filter and name generation all probe the
    // registry only, so a re-issued name can never resolve to the evicted
    // device.
    while devices.len() > HW_DEVICES_CAP {
        drop(devices.remove(0));
    }
}

/// Runtime availability of one GPU filter backend: the hardware device type
/// plus the FFmpeg filters that run on it.
///
/// Produced by [`get_gpu_filter_backends`]. `device_available` reflects this
/// machine (driver present, device usable); `filters[i].present_in_build`
/// reflects the linked FFmpeg build (compiled with that filter or not).
/// A filter chain such as `scale_cuda=1280:720` is usable only when both are true.
#[derive(Clone, Debug)]
pub struct GpuFilterBackend {
    /// FFmpeg device type name, e.g. "cuda", "vaapi", "qsv", "vulkan", "opencl".
    pub name: String,
    pub device_type: AVHWDeviceType,
    /// Whether a device of this type could actually be created on this machine.
    pub device_available: bool,
    /// FFmpeg error text when device creation failed — surface this to users,
    /// it usually names the missing piece (driver, permission, library).
    pub device_error: Option<String>,
    /// Known GPU filters of this backend and whether the linked FFmpeg build has them.
    pub filters: Vec<GpuFilterAvailability>,
}

/// Presence of a single named filter in the linked FFmpeg build.
#[derive(Clone, Debug)]
pub struct GpuFilterAvailability {
    pub name: &'static str,
    pub present_in_build: bool,
}

/// Well-known GPU filters per backend, used to pre-fill
/// [`GpuFilterBackend::filters`]. Presence is still checked at runtime.
fn known_filters_for(device_type: AVHWDeviceType) -> &'static [&'static str] {
    match device_type {
        AVHWDeviceType::AV_HWDEVICE_TYPE_CUDA => &[
            "scale_cuda",
            "overlay_cuda",
            "yadif_cuda",
            "bwdif_cuda",
            "chromakey_cuda",
            "colorspace_cuda",
            "bilateral_cuda",
            "thumbnail_cuda",
            "hwupload_cuda",
        ],
        AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI => &[
            "scale_vaapi",
            "deinterlace_vaapi",
            "denoise_vaapi",
            "procamp_vaapi",
            "sharpness_vaapi",
            "tonemap_vaapi",
            "overlay_vaapi",
            "transpose_vaapi",
        ],
        AVHWDeviceType::AV_HWDEVICE_TYPE_QSV => {
            &["scale_qsv", "vpp_qsv", "overlay_qsv", "deinterlace_qsv"]
        }
        AVHWDeviceType::AV_HWDEVICE_TYPE_VULKAN => &[
            "scale_vulkan",
            "gblur_vulkan",
            "avgblur_vulkan",
            "chromaber_vulkan",
            "overlay_vulkan",
            "flip_vulkan",
            "hflip_vulkan",
            "vflip_vulkan",
            "transpose_vulkan",
            "nlmeans_vulkan",
            "bwdif_vulkan",
            "blend_vulkan",
            "xfade_vulkan",
            "libplacebo",
        ],
        AVHWDeviceType::AV_HWDEVICE_TYPE_OPENCL => &[
            "program_opencl",
            "avgblur_opencl",
            "boxblur_opencl",
            "overlay_opencl",
            "tonemap_opencl",
            "unsharp_opencl",
            "nlmeans_opencl",
            "xfade_opencl",
        ],
        _ => &[],
    }
}

/// Returns whether the linked FFmpeg build contains a filter with this name.
///
/// ```rust,ignore
/// assert!(ez_ffmpeg::hwaccel::is_filter_available("scale"));
/// let has_cuda_scale = ez_ffmpeg::hwaccel::is_filter_available("scale_cuda");
/// ```
pub fn is_filter_available(name: &str) -> bool {
    let Ok(name_cstr) = CString::new(name) else {
        return false;
    };
    !unsafe { avfilter_get_by_name(name_cstr.as_ptr()) }.is_null()
}

/// Probes every hardware device type known to the linked FFmpeg build and
/// reports, per backend, whether a device can be created on this machine and
/// which of its GPU filters are compiled into the build.
///
/// Use this before constructing a GPU `filter_desc` chain to pick a working
/// backend and to give users actionable errors instead of a mid-pipeline failure:
///
/// ```rust,ignore
/// let usable: Vec<_> = ez_ffmpeg::hwaccel::get_gpu_filter_backends()
///     .into_iter()
///     .filter(|b| b.device_available)
///     .collect();
/// ```
///
/// Note: probing creates (and immediately frees) one device per type, which can
/// load vendor libraries; call it once at startup, not per job. Probe devices
/// are never registered for pipeline use, so this has no effect on
/// `filter_hw_device` selection.
pub fn get_gpu_filter_backends() -> Vec<GpuFilterBackend> {
    let mut backends = Vec::new();
    let mut device_type = AVHWDeviceType::AV_HWDEVICE_TYPE_NONE;

    loop {
        device_type = unsafe { av_hwdevice_iterate_types(device_type) };
        if device_type == AVHWDeviceType::AV_HWDEVICE_TYPE_NONE {
            break;
        }

        let name = unsafe {
            let name = av_hwdevice_get_type_name(device_type);
            if name.is_null() {
                continue;
            }
            match CStr::from_ptr(name).to_str() {
                Ok(name) => name.to_string(),
                Err(_) => "unknown name".to_string(),
            }
        };

        let (device_available, device_error) = probe_hw_device(device_type);

        let filters = known_filters_for(device_type)
            .iter()
            .map(|filter_name| GpuFilterAvailability {
                name: filter_name,
                present_in_build: is_filter_available(filter_name),
            })
            .collect();

        backends.push(GpuFilterBackend {
            name,
            device_type,
            device_available,
            device_error,
            filters,
        });
    }

    backends
}

/// Tries to create a device of the given type and frees it immediately.
/// Deliberately does NOT register the device in `HW_DEVICES`: probing must not
/// change which device `hw_device_for_filter()` later picks for real pipelines.
fn probe_hw_device(device_type: AVHWDeviceType) -> (bool, Option<String>) {
    let mut device_ref: *mut AVBufferRef = null_mut();
    let err =
        unsafe { av_hwdevice_ctx_create(&mut device_ref, device_type, null(), null_mut(), 0) };
    if err < 0 {
        unsafe { av_buffer_unref(&mut device_ref) };
        (false, Some(av_err2str(err)))
    } else {
        unsafe { av_buffer_unref(&mut device_ref) };
        (true, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This module's registry tests serialize on the crate-visible
    /// [`HW_REGISTRY_TEST_LOCK`] (shared with the macOS hardware tests in
    /// the scheduler — see its doc comment); the snapshot guard below bounds
    /// any sentinel residue to the guard's lifetime even on an assertion
    /// panic.

    /// Panic-safe snapshot/restore of the global registry: takes the current
    /// entries at construction and writes them back on Drop — which runs on
    /// BOTH the success path and an assertion unwind, so a failing test can
    /// never leak its sentinel entries into other tests.
    struct RegistrySnapshot(Vec<HWDevice>);

    impl RegistrySnapshot {
        fn take() -> Self {
            let registry = HW_DEVICES.get_or_init(new_hw_devices);
            RegistrySnapshot(std::mem::take(
                &mut *registry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            ))
        }
    }

    impl Drop for RegistrySnapshot {
        fn drop(&mut self) {
            // Poison-tolerant on BOTH ends: an assertion that panics while
            // holding the registry lock poisons the mutex, and a second
            // panic here would abort the process instead of restoring.
            let registry = HW_DEVICES.get_or_init(new_hw_devices);
            *registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = std::mem::take(&mut self.0);
        }
    }

    #[test]
    fn test_get_hwaccels() {
        let hwaccels = get_hwaccels();
        println!("{:?}", hwaccels);
    }

    #[test]
    fn test_is_filter_available() {
        // "scale" exists in every FFmpeg build; garbage names never do.
        assert!(is_filter_available("scale"));
        assert!(!is_filter_available("definitely_not_a_filter_xyz"));
        assert!(!is_filter_available("bad\0name"));
    }

    fn dev(name: &str, init_arg: Option<&str>) -> HWDevice {
        dev_with_ref(name, init_arg, null_mut())
    }

    /// Device with a real (av_buffer_alloc) refcounted sentinel buffer, so
    /// tests can assert that reuse hands back the SAME underlying context:
    /// under the `Arc<OwnedDeviceRef>` model every `HWDevice` clone shares
    /// the ONE creation reference, so identity is checked through the shared
    /// `(*ref).data` payload pointer, and the sentinel is released exactly
    /// once when the last handle drops.
    fn dev_with_ref(name: &str, init_arg: Option<&str>, device_ref: *mut AVBufferRef) -> HWDevice {
        HWDevice::new(
            name.to_string(),
            AVHWDeviceType::AV_HWDEVICE_TYPE_NONE,
            device_ref,
            init_arg.map(str::to_string),
        )
    }

    /// `dev_with_ref` with a concrete device type, for pinning selection
    /// rules that must distinguish typed entries from TYPE_NONE sentinels.
    fn typed_dev_with_ref(
        name: &str,
        device_type: AVHWDeviceType,
        device_ref: *mut AVBufferRef,
    ) -> HWDevice {
        HWDevice::new(name.to_string(), device_type, device_ref, None)
    }

    /// Allocates a real 1-byte refcounted buffer as a context stand-in.
    fn sentinel_buffer() -> *mut AVBufferRef {
        // SAFETY: av_buffer_alloc returns an owned refcounted buffer.
        let buf = unsafe { ffmpeg_sys_next::av_buffer_alloc(1) };
        assert!(!buf.is_null(), "av_buffer_alloc failed");
        buf
    }

    /// Shared payload pointer: identical for every reference to one buffer.
    fn payload(buf: *mut AVBufferRef) -> *mut u8 {
        // SAFETY: buf is a live AVBufferRef from sentinel_buffer/Clone.
        unsafe { (*buf).data }
    }

    // A repeated hwaccel spec must reuse the device created for that exact spec
    // (otherwise a long-running service leaks a context per job and eventually
    // exhausts device names -> ENOMEM). Devices registered without a reuse key
    // (`init_arg == None`) must never be reused by spec.
    #[test]
    fn reuse_only_matches_the_exact_init_spec() {
        let mut devices = vec![
            dev("vaapi0", Some("vaapi:/dev/dri/renderD128")),
            dev("vaapi1", Some("vaapi:/dev/dri/renderD129")),
            dev("cuda0", None),
        ];

        assert_eq!(
            reuse_move_to_back(&mut devices, "vaapi:/dev/dri/renderD128")
                .map(|d| d.name.clone())
                .as_deref(),
            Some("vaapi0"),
            "an identical spec must reuse its device"
        );
        assert!(
            reuse_move_to_back(&mut devices, "vaapi:/dev/dri/renderD130").is_none(),
            "a different spec must not reuse an unrelated device"
        );
        assert!(
            reuse_move_to_back(&mut devices, "cuda0").is_none(),
            "a device with no init spec (init_arg None) must never be reused by spec"
        );
        assert!(
            reuse_move_to_back(&mut Vec::new(), "vaapi:/dev/dri/renderD128").is_none(),
            "an empty list reuses nothing"
        );
    }

    // Reuse must keep the reused device as the LAST entry, because
    // hw_device_for_filter picks the newest concretely-typed entry as the
    // default filter device.
    // Sequence A -> B -> A: after reusing A it must be last again (not B).
    #[test]
    fn reuse_moves_the_device_to_the_back_for_default_filter_selection() {
        let mut devices = vec![dev("A", Some("spec-a")), dev("B", Some("spec-b"))];
        assert_eq!(
            devices.last().map(|d| d.name.as_str()),
            Some("B"),
            "precondition: B was initialized last"
        );

        let reused = reuse_move_to_back(&mut devices, "spec-a").expect("A must be reused");
        assert_eq!(reused.name, "A");
        assert_eq!(
            devices.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
            vec!["B", "A"],
            "reusing A must move it to the back so it is the default filter device"
        );
        assert_eq!(
            devices.len(),
            2,
            "reuse must not add a duplicate entry (no leaked context)"
        );
    }

    // hw_device_init_from_type keys registrations on the requested
    // (type, device) pair. The key shape (":{type}[:{device}]") must stay
    // disjoint from from_string spec keys in BOTH directions, or a decode-path
    // registration could alias a filter_hw_device spec (and vice versa).
    // av_hwdevice_get_type_name reads a static table in libavutil, so no real
    // device (or GPU) is needed here.
    #[test]
    fn from_type_keys_cannot_collide_with_from_string_specs() {
        let vaapi = AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI;
        let device_key = type_init_arg(vaapi, Some("/dev/dri/renderD128"))
            .expect("vaapi always has a type name");
        assert_eq!(device_key, ":vaapi:/dev/dri/renderD128");
        assert_eq!(type_init_arg(vaapi, None).as_deref(), Some(":vaapi"));
        assert_eq!(
            type_init_arg(AVHWDeviceType::AV_HWDEVICE_TYPE_NONE, None),
            None,
            "a type with no name gets no key (it cannot be created anyway)"
        );

        // A device registered from the equivalent from_string spec must NOT
        // satisfy a from_type request...
        let mut devices = vec![dev("vaapi0", Some("vaapi:/dev/dri/renderD128"))];
        assert!(reuse_move_to_back(&mut devices, &device_key).is_none());
        // ...and a from_type registration must not satisfy a from_string spec
        // lookup (from_string rejects ':'-prefixed specs before its reuse check).
        let mut devices = vec![dev("vaapi0", Some(&device_key))];
        assert!(reuse_move_to_back(&mut devices, "vaapi:/dev/dri/renderD128").is_none());
    }

    // Two from_type requests for the same (type, device) must resolve to the
    // SAME registered context (callers each take their own av_buffer_ref on it;
    // dec_task's per-job hw_device_get_by_name lookup never matches the
    // auto-generated names, so without keyed reuse every job would register a
    // fresh context until hw_device_default_name returns ENOMEM).
    #[test]
    fn from_type_reuse_matches_the_exact_type_and_device_request() {
        let vaapi = AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI;
        let d128 = type_init_arg(vaapi, Some("/dev/dri/renderD128")).unwrap();
        let d129 = type_init_arg(vaapi, Some("/dev/dri/renderD129")).unwrap();
        let type_only = type_init_arg(vaapi, None).unwrap();

        let ref_a = sentinel_buffer();
        let ref_b = sentinel_buffer();
        let payload_a = payload(ref_a);
        let payload_b = payload(ref_b);
        let mut devices = vec![
            dev_with_ref("vaapi0", Some(&d128), ref_a),
            dev_with_ref("vaapi1", Some(&type_only), ref_b),
        ];

        // Same (type, device) request -> same underlying context, nothing added.
        let reused = reuse_move_to_back(&mut devices, &d128).expect("same request must reuse");
        assert_eq!(reused.name, "vaapi0");
        assert_eq!(
            payload(reused.device_ref()),
            payload_a,
            "reuse must hand back the SAME registered context"
        );
        assert_eq!(devices.len(), 2, "reuse must not register a new entry");

        // A different device string of the same type is a different request.
        assert!(
            reuse_move_to_back(&mut devices, &d129).is_none(),
            "a different device must not reuse another device's context"
        );

        // A type-only request reuses only the type-only registration, never a
        // device-specific one (documented scheme in type_init_arg).
        let reused = reuse_move_to_back(&mut devices, &type_only)
            .expect("type-only must reuse the type-only entry");
        assert_eq!(reused.name, "vaapi1");
        assert_eq!(payload(reused.device_ref()), payload_b);
        assert_eq!(
            devices.last().map(|d| d.name.as_str()),
            Some("vaapi1"),
            "from_type reuse must mirror from_string's move-to-back so the reused \
             device stays the default filter device"
        );
    }

    // The auto-detect decode path probes several device types via
    // hw_device_init_from_type; a failed creation must return an error and
    // register nothing (no list pollution, no leaked entry). A VAAPI device
    // with a nonexistent path fails on every platform: either the backend is
    // not compiled in (ENOSYS) or opening the node fails.
    #[test]
    fn from_type_failed_creation_registers_nothing() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let before = HW_DEVICES
            .get()
            .map(|m| m.lock().unwrap().len())
            .unwrap_or(0);

        let (err, dev) = hw_device_init_from_type(
            AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI,
            Some("/definitely/not/a/device/node".to_string()),
        );
        assert!(err < 0, "creating a device on a bogus node must fail");
        assert!(dev.is_none());

        let after = HW_DEVICES
            .get()
            .map(|m| m.lock().unwrap().len())
            .unwrap_or(0);
        assert_eq!(
            before, after,
            "a failed from_type creation must not register a device"
        );
    }

    #[test]
    fn test_get_gpu_filter_backends_does_not_register_devices() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let devices_before = HW_DEVICES
            .get()
            .map(|m| m.lock().unwrap().len())
            .unwrap_or(0);

        let backends = get_gpu_filter_backends();
        for backend in &backends {
            println!(
                "{}: device_available={} error={:?} filters_in_build={}/{}",
                backend.name,
                backend.device_available,
                backend.device_error,
                backend
                    .filters
                    .iter()
                    .filter(|f| f.present_in_build)
                    .count(),
                backend.filters.len(),
            );
        }

        let devices_after = HW_DEVICES
            .get()
            .map(|m| m.lock().unwrap().len())
            .unwrap_or(0);
        assert_eq!(
            devices_before, devices_after,
            "probing must not register devices in HW_DEVICES"
        );
    }

    // Device specifications follow ffmpeg's -init_hw_device syntax:
    // type[=name][:device[,key=value...]] or type@source. The TYPE is the
    // part BEFORE the first separator (ffmpeg_hw.c: av_strndup(arg, k)).
    #[test]
    fn split_plain_type() {
        assert_eq!(split_device_type("cuda"), ("cuda", ""));
    }

    #[test]
    fn split_type_with_device_ordinal() {
        assert_eq!(split_device_type("cuda:0"), ("cuda", ":0"));
    }

    #[test]
    fn split_type_with_name_and_source() {
        assert_eq!(split_device_type("vaapi=va@src"), ("vaapi", "=va@src"));
    }

    // The ":device[,key=value...]" tail: the leading ':' is a separator,
    // not part of the device name (ffmpeg_hw.c: `++p` before parsing) —
    // "cuda:1" must select device "1", not a device named ":1" (which CUDA's
    // strtol would silently read as 0).
    #[test]
    fn device_tail_plain_ordinal() {
        assert_eq!(split_device_and_options(":0"), (Some("0"), None));
    }

    #[test]
    fn device_tail_with_options() {
        assert_eq!(
            split_device_and_options(":/dev/dri/renderD128,k=v"),
            (Some("/dev/dri/renderD128"), Some("k=v"))
        );
    }

    #[test]
    fn device_tail_options_only() {
        assert_eq!(split_device_and_options(":,k=v"), (None, Some("k=v")));
    }

    #[test]
    fn device_tail_empty() {
        assert_eq!(split_device_and_options(":"), (None, None));
    }

    // LOOKUP-wiring pin (no hardware needed): a sentinel entry
    // pre-registered under the exact canonical key makes a wired from_type
    // return it WITHOUT touching device creation; deleting the production
    // reuse lookup sends the call into av_hwdevice_ctx_create for a
    // nonexistent node -> red. The other half of the wiring — from_type's
    // own registration recording the canonical key — is pinned by
    // `from_type_registration_records_the_canonical_reuse_key`, which
    // drives the shared `register_from_type_device` helper. The RAII
    // snapshot guard keeps the non-AVHWDeviceContext sentinel out of
    // get_by_type/default-filter tests.
    #[test]
    fn from_type_production_wiring_reuses_across_calls() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let _restore = RegistrySnapshot::take();
        let registry = HW_DEVICES.get_or_init(new_hw_devices);

        let vaapi = AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI;
        // Unique path that never exists as a real device node.
        let dev_path = "/ez-ffmpeg-tests/wiring-pin-sentinel";
        let key = type_init_arg(vaapi, Some(dev_path)).unwrap();

        let sentinel = sentinel_buffer();
        let sentinel_payload = payload(sentinel);
        add_hw_device(HWDevice::new(
            "wiring-pin".to_string(),
            vaapi,
            sentinel,
            Some(key),
        ));

        for round in 1..=2 {
            let (err, dev) = hw_device_init_from_type(vaapi, Some(dev_path.to_string()));
            assert_eq!(
                err, 0,
                "round {round}: the registered (type, device) request must \
                 succeed via the production reuse lookup, not attempt \
                 creation of the nonexistent node"
            );
            let dev = dev.expect("reuse returns the registered device");
            assert_eq!(
                payload(dev.device_ref()),
                sentinel_payload,
                "round {round}: from_type must hand back the SAME registered \
                 context"
            );
            assert_eq!(dev.name, "wiring-pin");
        }
        let len = registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        assert_eq!(len, 1, "reuse must never grow the registry");
    }

    // R4 registration-key pin: `register_from_type_device` is the exact
    // registration path `hw_device_init_from_type` runs after a successful
    // creation; it must record the canonical `type_init_arg` key, or the
    // decode path re-creates a device per job (the original leak). Reverting
    // the helper to `init_arg: None` turns this red. Hardware-free: the
    // "created" context is a sentinel buffer. Snapshots/restores the global
    // registry via the RAII guard.
    #[test]
    fn from_type_registration_records_the_canonical_reuse_key() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _restore = RegistrySnapshot::take();

        let vaapi = AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI;
        let dev_path = "/ez-ffmpeg-tests/registration-key-sentinel";

        let registered = register_from_type_device(
            "regkey-pin".to_string(),
            vaapi,
            sentinel_buffer(),
            Some(dev_path),
        );
        let expected_key = type_init_arg(vaapi, Some(dev_path)).unwrap();
        assert_eq!(
            registered.init_arg.as_deref(),
            Some(expected_key.as_str()),
            "from_type's registration must record the canonical reuse key"
        );

        // And the recorded key must actually resolve through the production
        // lookup: the next identical request reuses instead of creating.
        let reused =
            reuse_by_init_arg_move_to_back(&expected_key).expect("the registered key must match");
        assert_eq!(reused.name, "regkey-pin");
        assert_eq!(
            payload(reused.device_ref()),
            payload(registered.device_ref())
        );
    }

    // Bounded-LRU pin against the PRODUCTION registry: add_hw_device itself
    // must evict the FRONT (least-recently-used) entry past HW_DEVICES_CAP,
    // a reused (moved-to-back) entry must survive, and the length must stay
    // at the cap. Deleting the eviction loop in add_hw_device turns this
    // red. Runs under HW_REGISTRY_TEST_LOCK and snapshots/restores the
    // global list so the sentinel entries never leak into other tests.
    #[test]
    fn registry_evicts_least_recently_used_past_the_cap() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        let _restore = RegistrySnapshot::take();
        let registry = HW_DEVICES.get_or_init(new_hw_devices);

        for i in 0..HW_DEVICES_CAP {
            add_hw_device(dev_with_ref(
                &format!("d{i}"),
                Some(&format!("spec-{i}")),
                sentinel_buffer(),
            ));
        }
        // Touch d0 through the production reuse path: moves it to the back.
        assert!(reuse_by_init_arg_move_to_back("spec-0").is_some());

        // One past the cap through the production registration path.
        add_hw_device(dev_with_ref("fresh", Some("spec-fresh"), sentinel_buffer()));

        // Snapshot names OUTSIDE the assertions: panicking while holding the
        // registry MutexGuard would poison the lock under the restore guard.
        let (len, names): (usize, Vec<String>) = {
            let devices = registry
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                devices.len(),
                devices.iter().map(|d| d.name.clone()).collect(),
            )
        };
        assert_eq!(len, HW_DEVICES_CAP, "length pinned at the cap");
        assert!(
            names.iter().any(|n| n == "d0"),
            "the just-reused entry must survive eviction (LRU, not FIFO)"
        );
        assert!(
            !names.iter().any(|n| n == "d1"),
            "the least-recently-used entry must be the one evicted"
        );
        assert!(
            names.iter().any(|n| n == "fresh"),
            "the new entry must be registered"
        );
    }

    // hw_device_for_filter must never pick a TYPE_NONE entry: no filter can
    // bind such a device, and its type has no name — selecting one sent
    // av_hwdevice_get_type_name's NULL straight into CStr::from_ptr inside
    // the multi-device advisory warn, taking the whole process down whenever
    // a concurrently running job configured a filtergraph while this
    // registry held sentinel entries.
    #[test]
    fn filter_default_skips_typeless_devices() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _restore = RegistrySnapshot::take();

        add_hw_device(dev_with_ref("s0", None, sentinel_buffer()));
        add_hw_device(dev_with_ref("s1", None, sentinel_buffer()));

        assert!(
            hw_device_for_filter().is_none(),
            "a TYPE_NONE device must never be the filter default"
        );
    }

    // With typed and typeless entries mixed, the newest concretely-typed one
    // wins — the previous blind last() pick would have handed the sentinel
    // to the filtergraph — and the multi-device warn resolves a real type
    // name on the way out.
    #[test]
    fn filter_default_picks_the_newest_concretely_typed_device() {
        let _registry = HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let _restore = RegistrySnapshot::take();

        add_hw_device(typed_dev_with_ref(
            "older-cuda",
            AVHWDeviceType::AV_HWDEVICE_TYPE_CUDA,
            sentinel_buffer(),
        ));
        add_hw_device(typed_dev_with_ref(
            "newer-vaapi",
            AVHWDeviceType::AV_HWDEVICE_TYPE_VAAPI,
            sentinel_buffer(),
        ));
        add_hw_device(dev_with_ref("sentinel", None, sentinel_buffer()));

        let picked = hw_device_for_filter()
            .expect("a concretely typed device must be picked over sentinels");
        assert_eq!(picked.name, "newer-vaapi");
    }
}
