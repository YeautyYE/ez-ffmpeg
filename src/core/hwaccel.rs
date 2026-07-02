use ffmpeg_sys_next::{
    av_buffer_unref, av_dict_parse_string, av_hwdevice_ctx_create, av_hwdevice_ctx_create_derived,
    av_hwdevice_find_type_by_name, av_hwdevice_get_type_name, av_hwdevice_iterate_types,
    avcodec_get_hw_config, AVBufferRef, AVCodec, AVHWDeviceType, AVERROR,
    AV_CODEC_HW_CONFIG_METHOD_HW_DEVICE_CTX, EINVAL, ENOMEM,
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
            FILTER_HW_DEVICE.set(Mutex::new(Some(dev.name.clone()))).ok();
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
}

unsafe impl Send for HWDevice {}
unsafe impl Sync for HWDevice {}

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

pub(crate) fn hw_device_init_from_string(arg: &str) -> (i32, Option<HWDevice>) {
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
        let name_end = p[1..]
            .find([':', '@', ','])
            .unwrap_or(p.len() - 1);
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
        let mut options = null_mut();

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
                    &mut options,
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
                None => av_hwdevice_ctx_create(&mut device_ref, device_type, null(), options, 0),
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
                        options,
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
            let mut options = null_mut();
            let Ok(v_cstr) = CString::new(v) else {
                error!("Device creation failed: option:{v} can't convert to CString");
                av_buffer_unref(&mut device_ref);
                return (AVERROR(EINVAL), None);
            };
            let eq_cstr = CString::new("=").unwrap();
            let comma_cstr = CString::new(",").unwrap();
            let mut err = av_dict_parse_string(
                &mut options,
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
            err = av_hwdevice_ctx_create(&mut device_ref, device_type, null(), options, 0);
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

fn add_hw_device(device: HWDevice) {
    let devices = HW_DEVICES.get_or_init(new_hw_devices);
    let mut devices = devices.lock().unwrap();
    devices.push(device);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_hwaccels() {
        let hwaccels = get_hwaccels();
        println!("{:?}", hwaccels);
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
