mod lease;
mod store;
mod task;

pub use lease::{LeaseError, LeaseTable};
pub use store::{FabricState, JsonStore, StoreError, atomic_replace};
pub use task::{TaskError, TaskTable};

use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after Unix epoch")
        .as_millis() as u64
}

pub fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}
