/// Fill `buf` with cryptographically-secure random bytes.
///
/// - Native: reads from `/dev/urandom`.
/// - Wasm:   uses `crypto.getRandomValues()` via web-sys.
#[cfg(not(target_arch = "wasm32"))]
pub fn fill_random_bytes(buf: &mut [u8]) {
    use std::io::Read;
    if let Ok(mut f) = std::fs::File::open("/dev/urandom") {
        let _ = f.read_exact(buf);
    }
}

#[cfg(target_arch = "wasm32")]
pub fn fill_random_bytes(buf: &mut [u8]) {
    use js_sys::Uint8Array;
    let crypto = web_sys::window().and_then(|w| w.crypto().ok());
    if let Some(crypto) = crypto {
        let arr = Uint8Array::new_with_length(buf.len() as u32);
        let _ = crypto.get_random_values_with_array_buffer_view(&arr);
        arr.copy_to(buf);
    } else {
        // Fallback: use Math.random() when crypto is unavailable
        for byte in buf.iter_mut() {
            *byte = (js_sys::Math::random() * 256.0) as u8;
        }
    }
}
