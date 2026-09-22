//! Native image clipboard ownership. No watcher, text forwarding, or payload persistence.
use arboard::{Clipboard, Error as ClipboardError, ImageData};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use machine_fabric_protocol::RpcError;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

pub const DEFAULT_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub struct ClipboardService {
    // On X11 the owner must stay alive after a write for consumers such as Codex.
    clipboard: Mutex<Option<Clipboard>>,
}

impl ClipboardService {
    fn with<T>(
        &self,
        operation: impl FnOnce(&mut Clipboard) -> Result<T, RpcError>,
    ) -> Result<T, RpcError> {
        #[cfg(windows)]
        {
            // A Windows service's Session 0 clipboard is not the user's desktop.
            // Until an interactive-session broker exists, keep that target disabled.
            let mut session = 0;
            let ok = unsafe {
                windows_sys::Win32::System::RemoteDesktop::ProcessIdToSessionId(
                    std::process::id(),
                    &mut session,
                )
            };
            if ok == 0 || session == 0 {
                return Err(RpcError::new(
                    "CLIPBOARD_UNAVAILABLE",
                    "clipboard requires an interactive Windows session",
                ));
            }
        }
        let mut guard = self.clipboard.lock().expect("clipboard lock");
        if guard.is_none() {
            *guard = Some(Clipboard::new().map_err(unavailable)?);
        }
        let result = operation(guard.as_mut().expect("clipboard initialized"));
        // Re-open after a display disconnect instead of retaining a broken connection.
        if result
            .as_ref()
            .is_err_and(|e| e.code == "CLIPBOARD_UNAVAILABLE")
        {
            *guard = None;
        }
        result
    }

    /// Probe the native backend without reading or changing clipboard contents.
    pub fn status(&self) -> Result<Value, RpcError> {
        self.with(|_| {
            Ok(json!({"ready": true, "imageOnly": true,
            "maxBytes": DEFAULT_MAX_BYTES, "display": std::env::var("DISPLAY").ok()}))
        })
    }

    pub fn read(&self, max_bytes: usize) -> Result<Value, RpcError> {
        self.with(|clipboard| {
            let image = clipboard.get_image().map_err(|error| match error {
                ClipboardError::ContentNotAvailable => {
                    RpcError::new("NO_IMAGE", "clipboard contains no image")
                }
                error => unavailable(error),
            })?;
            check_size(image.bytes.len(), max_bytes)?;
            let digest = image_digest(image.width, image.height, &image.bytes);
            Ok(json!({"semanticDigest": digest, "size": image.bytes.len(),
                "content": {"kind": "image", "width": image.width, "height": image.height,
                    "rgbaBase64": BASE64.encode(&image.bytes)}}))
        })
    }

    pub fn write(&self, params: &Value, max_bytes: usize) -> Result<Value, RpcError> {
        // Validate before touching the destination. Invalid input never clears its clipboard.
        let (width, height, bytes, digest) = decode_image(params, max_bytes)?;
        self.with(|clipboard| {
            if params
                .get("expiresAtMs")
                .and_then(Value::as_u64)
                .is_some_and(|deadline| now_ms() >= deadline)
            {
                return Err(RpcError::new(
                    "CLIPBOARD_EXPIRED",
                    "image transfer expired before it could be applied",
                ));
            }
            clipboard
                .set_image(ImageData {
                    width,
                    height,
                    bytes: Cow::Owned(bytes),
                })
                .map_err(unavailable)?;
            Ok(json!({"applied": true, "semanticDigest": digest, "width": width, "height": height}))
        })
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImageContent {
    kind: String,
    width: usize,
    height: usize,
    rgba_base64: String,
}

fn decode_image(
    params: &Value,
    maximum: usize,
) -> Result<(usize, usize, Vec<u8>, String), RpcError> {
    let raw = params
        .get("content")
        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "content is required"))?;
    if raw.get("kind").and_then(Value::as_str) != Some("image") {
        return Err(RpcError::new("NO_IMAGE", "only images can be synchronized"));
    }
    let image: ImageContent = serde_json::from_value(raw.clone())
        .map_err(|_| RpcError::new("INVALID_PARAMS", "invalid image content"))?;
    debug_assert_eq!(image.kind, "image");
    let size = image
        .width
        .checked_mul(image.height)
        .and_then(|n| n.checked_mul(4))
        .filter(|&n| n > 0)
        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "invalid image dimensions"))?;
    check_size(size, maximum)?;
    // Reject oversized encoded input before allocating a decoded pixel buffer.
    if image.rgba_base64.len() > size.div_ceil(3) * 4 {
        return Err(RpcError::new(
            "INVALID_PARAMS",
            "encoded data exceeds image dimensions",
        ));
    }
    let bytes = BASE64
        .decode(&image.rgba_base64)
        .map_err(|_| RpcError::new("INVALID_PARAMS", "invalid image encoding"))?;
    if bytes.len() != size {
        return Err(RpcError::new(
            "INVALID_PARAMS",
            "RGBA size does not match image dimensions",
        ));
    }
    let digest = image_digest(image.width, image.height, &bytes);
    if params.get("semanticDigest").and_then(Value::as_str) != Some(&digest) {
        return Err(RpcError::new(
            "CLIPBOARD_DIGEST_MISMATCH",
            "image digest mismatch",
        ));
    }
    Ok((image.width, image.height, bytes, digest))
}

fn check_size(size: usize, maximum: usize) -> Result<(), RpcError> {
    if size == 0 || size > maximum.min(DEFAULT_MAX_BYTES) {
        Err(RpcError::new(
            "CLIPBOARD_TOO_LARGE",
            "image exceeds the clipboard pixel limit (16 MiB maximum)",
        ))
    } else {
        Ok(())
    }
}
fn unavailable(error: ClipboardError) -> RpcError {
    let mut error = RpcError::new(
        "CLIPBOARD_UNAVAILABLE",
        format!("native clipboard unavailable: {error}"),
    );
    error.retryable = true;
    error
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn image_digest(width: usize, height: usize, bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(format!("image/rgba\0{width}x{height}\0"));
    hash.update(bytes);
    hex::encode(hash.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> Value {
        json!({"semanticDigest":image_digest(1,1,&[1,2,3,255]),
        "content":{"kind":"image","width":1,"height":1,"rgbaBase64":BASE64.encode([1,2,3,255])}})
    }
    #[test]
    fn image_roundtrip_and_corruption() {
        let mut value = fixture();
        assert_eq!(decode_image(&value, 4).unwrap().2, [1, 2, 3, 255]);
        value["semanticDigest"] = json!("wrong");
        assert_eq!(
            decode_image(&value, 4).unwrap_err().code,
            "CLIPBOARD_DIGEST_MISMATCH"
        );
    }
    #[test]
    fn rejects_text_empty_overflow_and_size_before_native_access() {
        let service = ClipboardService::default();
        for kind in ["text", "empty"] {
            assert_eq!(
                service
                    .write(&json!({"content":{"kind":kind}}), 4)
                    .unwrap_err()
                    .code,
                "NO_IMAGE"
            );
        }
        for width in [0, usize::MAX, 100_000] {
            let mut value = fixture();
            value["content"]["width"] = json!(width);
            assert!(decode_image(&value, DEFAULT_MAX_BYTES).is_err());
        }
        assert_eq!(
            decode_image(&fixture(), 3).unwrap_err().code,
            "CLIPBOARD_TOO_LARGE"
        );
        let mut value = fixture();
        value["content"]["rgbaBase64"] = json!("bad");
        assert!(decode_image(&value, 4).is_err());
        assert!(service.clipboard.lock().unwrap().is_none());
    }
}

#[cfg(all(test, target_os = "linux"))]
mod native_x11_test {
    use super::*;
    #[test]
    #[ignore = "requires an isolated Xvfb display; scripts/test-clipboard-x11.py supplies it"]
    fn image_survives_request_and_is_readable_by_codex_backend() {
        let owner = ClipboardService::default();
        assert_eq!(owner.status().unwrap()["ready"], true);
        let rgba = [12, 34, 56, 255];
        let image = json!({"semanticDigest":image_digest(1,1,&rgba),"content":{"kind":"image","width":1,"height":1,"rgbaBase64":BASE64.encode(rgba)}});
        assert_eq!(owner.write(&image, 4).unwrap()["applied"], true);
        // Codex uses another arboard connection, then encodes the received pixels.
        let reader = ClipboardService::default();
        assert_eq!(reader.read(4).unwrap()["content"], image["content"]);
        assert!(
            owner
                .write(
                    &json!({"content":{"kind":"text","text":"do not replace"}}),
                    4
                )
                .is_err()
        );
        let mut expired = image.clone();
        expired["expiresAtMs"] = json!(1);
        assert_eq!(
            owner.write(&expired, 4).unwrap_err().code,
            "CLIPBOARD_EXPIRED"
        );
        assert_eq!(reader.read(4).unwrap()["content"], image["content"]);
        let png = std::process::Command::new("xclip")
            .args(["-selection", "clipboard", "-target", "image/png", "-o"])
            .output()
            .unwrap();
        assert!(png.status.success());
        assert!(png.stdout.starts_with(b"\x89PNG\r\n\x1a\n"));
    }
}
