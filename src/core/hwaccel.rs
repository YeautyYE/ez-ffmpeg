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
use std::sync::{Mutex, OnceLock};

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

        let name = unsafe {
            let name = av_hwdevice_get_type_name(device_type);
            match CStr::from_ptr(name).to_str() {
                Ok(name) => name.to_string(),
                Err(_) => "unknown name".to_string(),
            }
        };

        hwaccels.push(HWAccelInfo {
            name,
            hw_device_type: device_type,
        });
    }

    hwaccels
}

static HW_DEVICES: OnceLock<Mutex<Vec<HWDevice>>> = OnceLock::new();
// Serializes `hw_device_init_from_string` end-to-end so the reuse check and the
// registration cannot interleave across threads (see the function comment).
static INIT_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn init_lock() -> &'static Mutex<()> {
    INIT_LOCK.get_or_init(|| Mutex::new(()))
}
// Stores only the device NAME: the device itself lives in HW_DEVICES,
// mirroring ffmpeg_hw.c where filter_hw_device is a borrowed pointer into
// hw_devices — one owner, freed exactly once.
static FILTER_HW_DEVICE: OnceLock<Mutex<Option<String>>> = OnceLock::new();

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
            FILTER_HW_DEVICE
                .set(Mutex::new(Some(dev.name.clone())))
                .ok();
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

#[derive(Clone, Debug)]
pub(crate) struct HWDevice {
    pub(crate) name: String,
    pub(crate) device_type: AVHWDeviceType,
    pub(crate) device_ref: *mut AVBufferRef,
    /// The exact spec string this device was created from (`hw_device_init_from_string`),
    /// used to reuse it for an identical spec instead of creating a new context.
    /// `None` for devices created by other paths (e.g. `hw_device_init_from_type`).
    pub(crate) init_arg: Option<String>,
}

// SAFETY: device_ref is an owned AVBufferRef handed between threads as a
// whole; clones share the same ref and every copy is released exactly once
// through hw_device_free_all. Sync is intentionally NOT implemented.
unsafe impl Send for HWDevice {}

pub(crate) unsafe fn hw_device_free_all() {
    // The filter device slot holds only a name; the device it refers to is
    // owned by HW_DEVICES and freed exactly once below.
    if let Some(slot) = FILTER_HW_DEVICE.get() {
        if let Ok(mut slot) = slot.lock() {
            slot.take();
        }
    }

    // Free all devices in the hardware device list
    if let Some(hw_devices) = HW_DEVICES.get() {
        match hw_devices.lock() {
            Ok(mut devices_guard) => {
                // Iterate through and free each device reference
                for device in devices_guard.iter_mut() {
                    if !device.device_ref.is_null() {
                        av_buffer_unref(&mut device.device_ref);
                        // av_buffer_unref automatically sets pointer to null to prevent dangling pointers
                    }
                }
                // Optional: Clear the device list to free Vec memory
                devices_guard.clear();
            }
            Err(e) => {
                error!("Failed to lock hardware device list: {}", e);
            }
        }
    }
}

pub(crate) fn hw_device_for_filter() -> Option<HWDevice> {
    if let Some(slot) = FILTER_HW_DEVICE.get() {
        let slot = slot.lock().unwrap();
        if let Some(name) = slot.as_ref() {
            // An explicitly configured filter device wins; resolve it from
            // the single owning list.
            return hw_device_get_by_name(name);
        }
    }
    let devices = HW_DEVICES.get_or_init(new_hw_devices);

    let devices = devices.lock().unwrap();
    if !devices.is_empty() {
        let dev = devices.last();

        match dev {
            None => {}
            Some(dev) => {
                if devices.len() > 1 {
                    unsafe {
                        let type_name = av_hwdevice_get_type_name(dev.device_type);
                        let type_name = CStr::from_ptr(type_name).to_str();
                        if let Ok(type_name) = type_name {
                            warn!("There are {} hardware devices. device {} of type {type_name} is picked for filters by default. Set hardware device explicitly with the filter_hw_device option if device {} is not usable for filters.",
                            devices.len(),dev.name,
                            dev.name,);
                        }
                    }
                }

                return Some(dev.clone());
            }
        }
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

    // A long-running service re-runs the same hwaccel spec on every job. Auto
    // device names (vaapi0, vaapi1, ...) never match on lookup, so each call used
    // to create and permanently retain a new device context; after enough jobs
    // av_hwdevice_default_name exhausts its names and hardware init returns ENOMEM.
    // Reuse the device created for an identical spec (the context is a refcounted
    // AVBufferRef shared safely across jobs; the list is still freed once at
    // process cleanup, so no per-job free can dangle a shared ref). Reuse moves the
    // entry to the BACK of the list so it stays the "last-initialized" default
    // filter device (hw_device_for_filter picks `devices.last()`).
    if let Some(existing) = reuse_by_init_arg_move_to_back(arg) {
        return (0, Some(existing));
    }

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
            av_hwdevice_ctx_create_derived(&mut device_ref, device_type, src_device.device_ref, 0)
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

    let dev = HWDevice {
        name: name.unwrap(),
        device_type,
        device_ref,
        init_arg: Some(arg.to_string()),
    };
    add_hw_device(dev.clone());

    (0, Some(dev))
}

pub(crate) fn hw_device_init_from_type(
    device_type: AVHWDeviceType,
    device: Option<String>,
) -> (i32, Option<HWDevice>) {
    let name = hw_device_default_name(device_type);
    if name.is_none() {
        return (AVERROR(ENOMEM), None);
    }

    let mut device_ref = null_mut();

    let err = match device {
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

    let dev = HWDevice {
        name: name.unwrap(),
        device_type,
        device_ref,
        init_arg: None,
    };

    add_hw_device(dev.clone());

    (0, Some(dev))
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

/// Reuses the device already created from the exact same spec string, if any, so
/// an identical `hw_device_init_from_string` request reuses it instead of leaking
/// a fresh context — moving the matched entry to the back of the process-global
/// list so it stays the last-initialized default filter device.
fn reuse_by_init_arg_move_to_back(arg: &str) -> Option<HWDevice> {
    let devices = HW_DEVICES.get_or_init(new_hw_devices);
    let mut devices = devices.lock().unwrap();
    reuse_move_to_back(&mut devices, arg)
}

/// Pure reuse over a device list: a device is reusable for `arg` only if it was
/// created from that exact spec string (devices with `init_arg == None`, e.g.
/// `hw_device_init_from_type`, never match). On a hit the entry is moved to the
/// BACK of the list — `hw_device_for_filter` picks `devices.last()`, so reuse must
/// preserve the "last-initialized wins" default-filter semantics. Split out so the
/// reuse behavior is unit-testable without the process-global list.
fn reuse_move_to_back(devices: &mut Vec<HWDevice>, arg: &str) -> Option<HWDevice> {
    let idx = devices
        .iter()
        .position(|device| device.init_arg.as_deref() == Some(arg))?;
    let device = devices.remove(idx);
    devices.push(device.clone());
    Some(device)
}

fn add_hw_device(device: HWDevice) {
    let devices = HW_DEVICES.get_or_init(new_hw_devices);
    let mut devices = devices.lock().unwrap();
    devices.push(device);
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
        HWDevice {
            name: name.to_string(),
            device_type: AVHWDeviceType::AV_HWDEVICE_TYPE_NONE,
            device_ref: null_mut(),
            init_arg: init_arg.map(str::to_string),
        }
    }

    // A repeated hwaccel spec must reuse the device created for that exact spec
    // (otherwise a long-running service leaks a context per job and eventually
    // exhausts device names -> ENOMEM). Devices created by other paths
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
                .map(|d| d.name)
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
    // hw_device_for_filter picks devices.last() as the default filter device.
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

    #[test]
    fn test_get_gpu_filter_backends_does_not_register_devices() {
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
}
