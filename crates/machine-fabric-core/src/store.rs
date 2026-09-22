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

#[cfg(test)]
#[derive(Clone, Copy)]
enum SaveFailure {
    Write,
    Sync,
    Rename,
    Enospc,
}

impl JsonStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<FabricState, StoreError> {
        if !self.path.exists() {
            return Ok(FabricState::default());
        }
        Ok(serde_json::from_slice(&fs::read(&self.path)?)?)
    }

    pub fn save(&self, state: &FabricState) -> Result<(), StoreError> {
        self.save_inner(state, None)
    }

    #[cfg(test)]
    fn save_with_failure(
        &self,
        state: &FabricState,
        failure: SaveFailure,
    ) -> Result<(), StoreError> {
        self.save_inner(state, Some(failure))
    }

    fn save_inner(
        &self,
        state: &FabricState,
        #[cfg(test)] failure: Option<SaveFailure>,
        #[cfg(not(test))] _failure: Option<()>,
    ) -> Result<(), StoreError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = temporary_path(&self.path);
        let result = (|| -> Result<(), StoreError> {
            let mut options = fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            let bytes = serde_json::to_vec_pretty(state)?;
            file.write_all(&bytes).map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("write state temp {}: {error}", temporary.display()),
                )
            })?;
            #[cfg(test)]
            if matches!(failure, Some(SaveFailure::Write | SaveFailure::Enospc)) {
                return Err(StoreError::Io(std::io::Error::from_raw_os_error(28)));
            }
            file.write_all(b"\n").map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("write state temp {}: {error}", temporary.display()),
                )
            })?;
            file.sync_all().map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("sync state temp {}: {error}", temporary.display()),
                )
            })?;
            #[cfg(test)]
            if matches!(failure, Some(SaveFailure::Sync)) {
                return Err(StoreError::Io(std::io::Error::other(
                    "injected sync failure",
                )));
            }
            #[cfg(test)]
            if matches!(failure, Some(SaveFailure::Rename)) {
                return Err(StoreError::Io(std::io::Error::other(
                    "injected rename failure",
                )));
            }
            atomic_replace(&temporary, &self.path).map_err(|error| {
                std::io::Error::new(
                    error.kind(),
                    format!("rename state temp {}: {error}", temporary.display()),
                )
            })?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result
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

    #[test]
    fn failed_atomic_replace_cleans_new_temp_and_preserves_old_path() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        std::fs::create_dir(&path).unwrap();
        let store = JsonStore::new(&path);
        assert!(store.save(&FabricState::default()).is_err());
        assert!(path.is_dir());
        let leftovers = std::fs::read_dir(directory.path())
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with(".state.json.") && name.ends_with(".tmp"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "leftover temp files: {leftovers:?}");
    }

    #[test]
    fn injected_write_sync_rename_and_enospc_failures_preserve_old_state() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        let store = JsonStore::new(&path);
        store.save(&FabricState::default()).unwrap();
        for failure in [
            SaveFailure::Write,
            SaveFailure::Sync,
            SaveFailure::Rename,
            SaveFailure::Enospc,
        ] {
            assert!(
                store
                    .save_with_failure(&FabricState::default(), failure)
                    .is_err()
            );
            assert!(store.load().unwrap().tasks.is_empty());
            let leftovers = std::fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"));
            assert!(!leftovers);
        }
    }
}
