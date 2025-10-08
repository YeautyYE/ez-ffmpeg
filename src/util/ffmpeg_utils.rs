use ffmpeg_sys_next::{AV_ERROR_MAX_STRING_SIZE, AVDictionary, av_dict_set, av_strerror};
use std::collections::HashMap;
use std::ffi::{CStr, CString};

pub(crate) fn hashmap_to_avdictionary(
    opts: &Option<HashMap<CString, CString>>,
) -> *mut AVDictionary {
    let mut av_dict: *mut AVDictionary = std::ptr::null_mut();

    if let Some(map) = opts {
        for (key, value) in map {
            unsafe {
                av_dict_set(&mut av_dict, key.as_ptr(), value.as_ptr(), 0);
            }
        }
    }

    av_dict
}

pub fn av_err2str(err: i32) -> String {
    unsafe {
        let mut buffer = [0i8; AV_ERROR_MAX_STRING_SIZE];
        av_strerror(err, buffer.as_mut_ptr(), AV_ERROR_MAX_STRING_SIZE);
        let c_str = CStr::from_ptr(buffer.as_ptr());
        match c_str.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => format!("Unknown error: {}", err),
        }
    }
}
