//! Platforms without a supported native clipboard backend.
use machine_fabric_protocol::RpcError;
use serde_json::Value;
pub const DEFAULT_MAX_BYTES: usize = 16 * 1024 * 1024;
#[derive(Default)]
pub struct ClipboardService;
impl ClipboardService {
    pub fn status(&self) -> Result<Value, RpcError> {
        Err(RpcError::new(
            "CLIPBOARD_UNSUPPORTED",
            "native image clipboard is unavailable on this platform",
        ))
    }
    pub fn read(&self, _: usize) -> Result<Value, RpcError> {
        self.status()
    }
    pub fn write(&self, _: &Value, _: usize) -> Result<Value, RpcError> {
        self.status()
    }
}
