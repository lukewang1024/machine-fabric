use machine_fabric_schema::{
    ActivationTransaction, AgentInstance, Approval, Artifact, ControllerPeer, Executor, Generation,
    Handoff, Lease, Task, WorkspaceSession,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FabricState {
    #[serde(default)]
    pub controller_id: Option<String>,
    #[serde(default)]
    pub sessions: Vec<WorkspaceSession>,
    #[serde(default)]
    pub executors: Vec<Executor>,
    #[serde(default)]
    pub controllers: Vec<ControllerPeer>,
    #[serde(default)]
    pub leases: Vec<Lease>,
    #[serde(default)]
    pub lease_fences: BTreeMap<String, u64>,
    #[serde(default)]
    pub tasks: Vec<Task>,
    #[serde(default)]
    pub artifacts: Vec<Artifact>,
    #[serde(default)]
    pub generations: Vec<Generation>,
    #[serde(default)]
    pub transactions: Vec<ActivationTransaction>,
    #[serde(default)]
    pub agents: Vec<AgentInstance>,
    #[serde(default)]
    pub handoffs: Vec<Handoff>,
    #[serde(default)]
    pub approvals: Vec<Approval>,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("state IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid state: {0}")]
    Json(#[from] serde_json::Error),
    #[error("controller identity mismatch: state belongs to {stored}, configured as {configured}")]
    ControllerIdentityMismatch { stored: String, configured: String },
}

#[derive(Debug, Clone)]
pub struct JsonStore {
    path: PathBuf,
}

impl JsonStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<FabricState, StoreError> {
        if !self.path.exists() {
            return Ok(FabricState::default());
        }
        Ok(serde_json::from_slice(&fs::read(&self.path)?)?)
    }

    pub fn save(&self, state: &FabricState) -> Result<(), StoreError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = temporary_path(&self.path);
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(&serde_json::to_vec_pretty(state)?)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        atomic_replace(&temporary, &self.path)?;
        Ok(())
    }
}

#[cfg(unix)]
pub fn atomic_replace(source: &Path, target: &Path) -> std::io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
pub fn atomic_replace(source: &Path, target: &Path) -> std::io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result != 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    path.with_file_name(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_state_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let store = JsonStore::new(directory.path().join("state.json"));
        store.save(&FabricState::default()).unwrap();
        assert!(store.load().unwrap().tasks.is_empty());
    }
}
