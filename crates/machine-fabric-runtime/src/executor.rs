use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use machine_fabric_core::{atomic_replace, now_ms, sha256_bytes};
use machine_fabric_protocol::{Request, Response, RpcError};
use machine_fabric_schema::{
    CapabilityDescriptor, Effect, HealthCheck, IdempotencyContract, RetryPolicy, RollbackStrategy,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::datapack::{DataPackResourceTree, pack_chromium_datapack};
use crate::generation::{Overlay, activate, apply_overlays, materialize, record_state};
use crate::process::ProcessTable;
use crate::telemetry::{event_fields, request_event};

pub struct ExecutorRuntime {
    id: String,
    allowed_roots: Vec<PathBuf>,
    processes: ProcessTable,
    fences: Option<Mutex<ExecutorFences>>,
    execution: ExecutionCapacity,
    relay_root: PathBuf,
    computer_use: crate::computer_use::ComputerUseService,
    desktop: Mutex<crate::desktop::DesktopQueue>,
    desktop_execution: Mutex<()>,
}

#[derive(Debug)]
struct ExecutionCapacity {
    maximum: usize,
    maximum_queue: usize,
    wait_timeout: Duration,
    state: Mutex<ExecutionState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct ExecutionState {
    active: usize,
    held_resources: std::collections::BTreeSet<String>,
    waiting: Vec<WaitingExecution>,
    next_ticket: u64,
}

#[derive(Debug)]
struct WaitingExecution {
    ticket: u64,
    resources: Vec<String>,
}

struct ExecutionPermit<'a> {
    capacity: &'a ExecutionCapacity,
    resources: Vec<String>,
}

impl Drop for ExecutionPermit<'_> {
    fn drop(&mut self) {
        let mut state = self.capacity.state.lock().expect("execution capacity lock");
        state.active = state.active.saturating_sub(1);
        for resource in &self.resources {
            state.held_resources.remove(resource);
        }
        self.capacity.changed.notify_all();
    }
}

impl ExecutionCapacity {
    #[cfg(test)]
    fn new(maximum: usize) -> Self {
        Self::configured(maximum, 16, Duration::from_secs(2))
    }

    fn configured(maximum: usize, maximum_queue: usize, wait_timeout: Duration) -> Self {
        Self {
            maximum,
            maximum_queue,
            wait_timeout,
            state: Mutex::new(ExecutionState::default()),
            changed: Condvar::new(),
        }
    }

    fn from_environment() -> Self {
        let fallback = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4)
            .clamp(2, 32);
        let maximum = std::env::var("MACHINE_FABRIC_EXECUTOR_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|value: &usize| *value > 0)
            .unwrap_or(fallback);
        let maximum_queue = std::env::var("MACHINE_FABRIC_EXECUTOR_MAX_QUEUE")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(128);
        let wait_timeout = std::env::var("MACHINE_FABRIC_EXECUTOR_QUEUE_WAIT_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(30_000);
        Self::configured(maximum, maximum_queue, Duration::from_millis(wait_timeout))
    }

    fn acquire(&self, mut resources: Vec<String>) -> Result<ExecutionPermit<'_>, RpcError> {
        resources.sort();
        resources.dedup();
        let deadline = Instant::now() + self.wait_timeout;
        let mut state = self.state.lock().expect("execution capacity lock");
        let ticket = state.next_ticket;
        state.next_ticket = state.next_ticket.saturating_add(1);

        if !self.can_grant(&state, &resources, None) && state.waiting.len() >= self.maximum_queue {
            let mut error = RpcError::new(
                "EXECUTOR_QUEUE_FULL",
                format!("executor queue limit {} reached", self.maximum_queue),
            );
            error.retryable = true;
            return Err(error);
        }

        state.waiting.push(WaitingExecution {
            ticket,
            resources: resources.clone(),
        });
        loop {
            if self.can_grant(&state, &resources, Some(ticket)) {
                let position = state
                    .waiting
                    .iter()
                    .position(|waiting| waiting.ticket == ticket)
                    .expect("queued execution ticket");
                state.waiting.remove(position);
                state.active += 1;
                state.held_resources.extend(resources.iter().cloned());
                self.changed.notify_all();
                return Ok(ExecutionPermit {
                    capacity: self,
                    resources,
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.waiting.retain(|waiting| waiting.ticket != ticket);
                self.changed.notify_all();
                let mut error = RpcError::new(
                    "EXECUTOR_QUEUE_TIMEOUT",
                    format!(
                        "executor admission wait exceeded {} ms",
                        self.wait_timeout.as_millis()
                    ),
                );
                error.retryable = true;
                return Err(error);
            }
            let (next_state, timeout) = self
                .changed
                .wait_timeout(state, remaining)
                .expect("execution capacity wait");
            state = next_state;
            if timeout.timed_out() && !self.can_grant(&state, &resources, Some(ticket)) {
                state.waiting.retain(|waiting| waiting.ticket != ticket);
                self.changed.notify_all();
                let mut error = RpcError::new(
                    "EXECUTOR_QUEUE_TIMEOUT",
                    format!(
                        "executor admission wait exceeded {} ms",
                        self.wait_timeout.as_millis()
                    ),
                );
                error.retryable = true;
                return Err(error);
            }
        }
    }

    fn can_grant(&self, state: &ExecutionState, resources: &[String], ticket: Option<u64>) -> bool {
        if state.active >= self.maximum
            || resources
                .iter()
                .any(|resource| state.held_resources.contains(resource))
        {
            return false;
        }
        let Some(ticket) = ticket else {
            return state.waiting.is_empty();
        };
        let Some(position) = state
            .waiting
            .iter()
            .position(|waiting| waiting.ticket == ticket)
        else {
            return false;
        };
        !state.waiting[..position].iter().any(|earlier| {
            earlier
                .resources
                .iter()
                .any(|resource| resources.contains(resource))
        })
    }

    fn snapshot(&self) -> (usize, usize, usize, usize, usize) {
        let state = self.state.lock().expect("execution capacity lock");
        (
            state.active,
            self.maximum,
            state.waiting.len(),
            self.maximum_queue,
            state.held_resources.len(),
        )
    }

    fn is_available(&self, resource: &str) -> bool {
        !self
            .state
            .lock()
            .expect("execution capacity lock")
            .held_resources
            .contains(resource)
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExecutorFences {
    #[serde(skip)]
    path: PathBuf,
    #[serde(default)]
    resources: BTreeMap<String, FenceRecord>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct FenceRecord {
    controller_id: String,
    fence: u64,
}

impl ExecutorRuntime {
    fn base(id: impl Into<String>, allowed_roots: Vec<PathBuf>) -> Result<Self, RpcError> {
        let mut roots = Vec::with_capacity(allowed_roots.len());
        for root in allowed_roots {
            roots.push(root.canonicalize().map_err(|error| {
                RpcError::new("INVALID_ROOT", format!("{}: {error}", root.display()))
            })?);
        }
        let relay_root = roots
            .first()
            .cloned()
            .ok_or_else(|| RpcError::new("INVALID_ROOT", "at least one allowed root is required"))?
            .join(".machine-fabric-relay");
        Ok(Self {
            id: id.into(),
            allowed_roots: roots,
            processes: ProcessTable::default(),
            fences: None,
            execution: ExecutionCapacity::from_environment(),
            relay_root,
            computer_use: crate::computer_use::ComputerUseService::default(),
            desktop: Mutex::new(crate::desktop::DesktopQueue::default()),
            desktop_execution: Mutex::new(()),
        })
    }

    #[cfg(test)]
    pub(crate) fn new(
        id: impl Into<String>,
        allowed_roots: Vec<PathBuf>,
    ) -> Result<Self, RpcError> {
        Self::base(id, allowed_roots)
    }

    pub fn open(
        id: impl Into<String>,
        allowed_roots: Vec<PathBuf>,
        state_path: PathBuf,
    ) -> Result<Self, RpcError> {
        let mut runtime = Self::base(id, allowed_roots)?;
        let mut fences: ExecutorFences = if state_path.exists() {
            serde_json::from_slice(&fs::read(&state_path).map_err(|error| {
                RpcError::new(
                    "FENCE_STATE_FAILED",
                    format!("{}: {error}", state_path.display()),
                )
            })?)
            .map_err(|error| RpcError::new("FENCE_STATE_FAILED", error.to_string()))?
        } else {
            ExecutorFences::default()
        };
        let process_state_path = state_path.with_file_name("executor-processes.json");
        runtime.relay_root = state_path.with_file_name("artifact-relay");
        fences.path = state_path;
        runtime.processes = ProcessTable::open(process_state_path)?;
        runtime.desktop = Mutex::new(crate::desktop::DesktopQueue::open(
            runtime.relay_root.with_file_name("desktop-queue.json"),
        )?);
        runtime.fences = Some(Mutex::new(fences));
        Ok(runtime)
    }

    pub fn handle(&self, request: Request) -> Response {
        let started = std::time::Instant::now();
        request_event(
            "info",
            "request.started",
            &request,
            json!({"executorId": self.id}),
        );
        let finish_fields = json!({
            "requestId": request.request_id.clone(),
            "correlationId": request.correlation_id.as_deref().unwrap_or(&request.request_id),
            "parentRequestId": request.parent_request_id.clone(),
            "action": request.action.clone(),
        });
        let mut params = request.params;
        let permit = if matches!(
            request.action.as_str(),
            "ping" | "status" | "availability" | "capability.list"
        ) {
            Ok(None)
        } else {
            execution_resources(&request.action, &params, &self.id)
                .and_then(|resources| self.execution.acquire(resources))
                .map(Some)
        };
        let result = match permit {
            Ok(_permit) => self
                .reconcile_desktop()
                .and_then(|()| self.issue_authority(&request.action, &mut params))
                .and_then(|()| self.desktop_dispatch(&request.action, params)),
            Err(error) => Err(error),
        };
        let response = match result {
            Ok(value) => Response::success(request.request_id.clone(), value),
            Err(error) => Response::failure(request.request_id.clone(), error),
        };
        event_fields(
            if response.ok { "info" } else { "error" },
            "request.finished",
            json!({"request": finish_fields, "executorId": self.id, "ok": response.ok, "durationMs": started.elapsed().as_millis()}),
        );
        response
    }

    fn issue_authority(&self, action: &str, params: &mut Value) -> Result<(), RpcError> {
        let Some(fences) = &self.fences else {
            return Ok(());
        };
        let canonical = match action {
            "fs.write" => "filesystem.write",
            "fs.patch" => "filesystem.patch",
            "fs.remove" => "filesystem.remove",
            "fs.restore" => "filesystem.restore",
            other => other,
        };
        let contract = capability_catalog()
            .iter()
            .find(|capability| capability.name == canonical)
            .cloned();
        let Some(contract) = contract else {
            return Ok(());
        };
        if matches!(contract.effect, Effect::ReadOnly) {
            return Ok(());
        }
        let resources = execution_resources(action, params, &self.id)?;
        let mut table = fences.lock().expect("executor fence lock");
        let controller_id = format!("executor:{}", self.id);
        let mut authorities = Vec::with_capacity(resources.len());
        for resource in resources {
            let fence = table
                .resources
                .get(&resource)
                .map_or(1, |current| current.fence.saturating_add(1));
            table.resources.insert(
                resource.clone(),
                FenceRecord {
                    controller_id: controller_id.clone(),
                    fence,
                },
            );
            authorities.push(json!({
                "controllerId": controller_id,
                "resource": resource,
                "fence": fence,
            }));
        }
        persist_fences(&table)?;
        params["_authority"] = Value::Array(authorities);
        Ok(())
    }

    fn execution_summary(&self) -> Value {
        let (active, maximum, queued, queue_limit, held_resources) = self.execution.snapshot();
        json!({
            "active": active,
            "maximum": maximum,
            "queued": queued,
            "queueLimit": queue_limit,
            "heldResourceCount": held_resources,
        })
    }

    fn desktop_dispatch(&self, action: &str, mut params: Value) -> Result<Value, RpcError> {
        if action.starts_with("desktop.") {
            let result = self
                .desktop
                .lock()
                .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?
                .command(action, &params)?;
            self.reconcile_desktop()?;
            if matches!(action, "desktop.finish" | "desktop.cancel") {
                return self
                    .desktop
                    .lock()
                    .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?
                    .command("desktop.get", &params);
            }
            return Ok(result);
        }
        if !crate::desktop::protected(action) {
            return self.dispatch(action, params);
        }
        self.reconcile_desktop()?;
        let gate = self
            .desktop_execution
            .lock()
            .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?;
        self.desktop
            .lock()
            .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?
            .begin(action, &mut params)?;
        let result = self.dispatch(action, params);
        let uncertain = result.as_ref().is_err_and(|error| {
            let message = error.message.to_ascii_lowercase();
            error.code == "COMPUTER_USE_UNAVAILABLE"
                || error.code.contains("TIMEOUT")
                || (error.code == "COMPUTER_USE_TOOL_FAILED"
                    && (message.contains("timeout")
                        || message.contains("timed out")
                        || message.contains("abort")))
        });
        self.desktop
            .lock()
            .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?
            .end(uncertain)?;
        drop(gate);
        self.reconcile_desktop()?;
        result
    }

    fn reconcile_desktop(&self) -> Result<(), RpcError> {
        let gate = match self.desktop_execution.try_lock() {
            Ok(gate) => gate,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(()),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                return Err(RpcError::new("DESKTOP_STATE_FAILED", error.to_string()));
            }
        };
        let cleanup = self
            .desktop
            .lock()
            .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?
            .needs_cleanup();
        if !cleanup {
            return Ok(());
        }
        let closed = self
            .computer_use
            .close_existing(&self.relay_root.with_file_name("computer-use"));
        self.desktop
            .lock()
            .map_err(|error| RpcError::new("DESKTOP_STATE_FAILED", error.to_string()))?
            .cleanup_done(closed.is_ok())?;
        drop(gate);
        closed
    }

    fn dispatch(&self, action: &str, params: Value) -> Result<Value, RpcError> {
        match action {
            "ping" | "status" => Ok(json!({
                "executorId": self.id,
                "status": "ready",
                "allowedRoots": self.allowed_roots,
                "capabilities": capability_catalog(),
                "execution": self.execution_summary(),
                "computerUse": self.computer_use.status(&self.relay_root.with_file_name("computer-use")),
                "computerUseStateRoot": self.relay_root.with_file_name("computer-use"),
            })),
            "computer-use.tools" | "computer-use.call" => self.computer_use.call(
                &self.relay_root.with_file_name("computer-use"),
                &params,
                action == "computer-use.tools",
            ),
            "availability" => {
                let (active, maximum, queued, queue_limit, held_resources) =
                    self.execution.snapshot();
                let requested: Vec<String> = params
                    .get("resources")
                    .and_then(Value::as_array)
                    .map(|items| {
                        items
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
                    .unwrap_or_default();
                Ok(json!({
                    "executorId": self.id,
                    "available": active < maximum || queued < queue_limit,
                    "active": active,
                    "maximum": maximum,
                    "queued": queued,
                    "queueLimit": queue_limit,
                    "heldResourceCount": held_resources,
                    "resources": requested.iter().map(|resource| json!({
                        "resource": resource,
                        "available": self.execution.is_available(resource),
                    })).collect::<Vec<_>>(),
                }))
            }
            "capability.list" => Ok(serde_json::to_value(capability_catalog()).unwrap()),
            "capability.negotiate" => {
                let name = required_str(&params, "name")?;
                let requested = required_str(&params, "version")?;
                let descriptor = capability_catalog()
                    .into_iter()
                    .find(|capability| capability.name == name)
                    .ok_or_else(|| {
                        RpcError::new(
                            "CAPABILITY_NOT_FOUND",
                            format!("unknown capability: {name}"),
                        )
                    })?;
                let requested_major = requested.split('.').next();
                let available_major = descriptor.version.split('.').next();
                if requested_major != available_major {
                    return Err(RpcError::new(
                        "CAPABILITY_VERSION_UNSUPPORTED",
                        format!(
                            "{name} requested {requested}, executor provides {}",
                            descriptor.version
                        ),
                    ));
                }
                Ok(json!({
                    "name": name,
                    "requestedVersion": requested,
                    "selectedVersion": descriptor.version,
                    "contract": descriptor,
                }))
            }
            "fs.stat" | "filesystem.stat" => {
                let path = self.path(&params, "path", false)?;
                let metadata = fs::symlink_metadata(&path)
                    .map_err(|error| io_error("FS_STAT_FAILED", &path, error))?;
                Ok(json!({
                    "path": path,
                    "exists": true,
                    "kind": if metadata.is_dir() { "directory" } else if metadata.is_file() { "file" } else { "other" },
                    "size": metadata.len(),
                    "digest": if metadata.is_file() { Some(digest_file(&path)?) } else { None },
                }))
            }
            "fs.resolve" | "filesystem.resolve" => {
                let path = self.path(&params, "path", false)?;
                Ok(json!({"path": path, "exists": path.exists()}))
            }
            "fs.read" | "filesystem.read" => {
                let path = self.path(&params, "path", true)?;
                let bytes =
                    fs::read(&path).map_err(|error| io_error("FS_READ_FAILED", &path, error))?;
                Ok(json!({
                    "path": path,
                    "content": String::from_utf8_lossy(&bytes),
                    "digest": sha256_bytes(&bytes),
                    "size": bytes.len(),
                }))
            }
            "fs.list" | "filesystem.list" => {
                let path = self.path(&params, "path", true)?;
                let mut entries = Vec::new();
                for entry in
                    fs::read_dir(&path).map_err(|error| io_error("FS_LIST_FAILED", &path, error))?
                {
                    let entry = entry.map_err(|error| io_error("FS_LIST_FAILED", &path, error))?;
                    let metadata = entry
                        .metadata()
                        .map_err(|error| io_error("FS_LIST_FAILED", &entry.path(), error))?;
                    entries.push(json!({
                        "name": entry.file_name(),
                        "path": entry.path(),
                        "kind": if metadata.is_dir() { "directory" } else if metadata.is_file() { "file" } else { "other" },
                        "size": metadata.len(),
                    }));
                }
                entries.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
                Ok(json!({"path": path, "entries": entries}))
            }
            "fs.search" | "filesystem.search" => {
                let path = self.path(&params, "path", true)?;
                let query = required_str(&params, "query")?;
                let max_results = params
                    .get("maxResults")
                    .and_then(Value::as_u64)
                    .unwrap_or(200) as usize;
                let mut matches = Vec::new();
                search_tree(&path, query, max_results, &mut matches)?;
                Ok(
                    json!({"path": path, "query": query, "matches": matches, "truncated": matches.len() >= max_results}),
                )
            }
            "fs.write" | "filesystem.write" => {
                let path = self.path(&params, "path", false)?;
                let content = params
                    .get("content")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "content is required"))?;
                if let Some(expected) = params.get("expectedDigest").and_then(Value::as_str) {
                    let actual = if path.exists() {
                        Some(digest_file(&path)?)
                    } else {
                        None
                    };
                    if actual.as_deref() != Some(expected) {
                        return Err(RpcError::new(
                            "DIGEST_CONFLICT",
                            format!(
                                "expected {expected}, got {}",
                                actual.as_deref().unwrap_or("<missing>")
                            ),
                        ));
                    }
                }
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)
                        .map_err(|error| io_error("FS_WRITE_FAILED", parent, error))?;
                }
                let temporary = path.with_extension(format!("fabric.{}.tmp", std::process::id()));
                fs::write(&temporary, content)
                    .map_err(|error| io_error("FS_WRITE_FAILED", &temporary, error))?;
                atomic_replace(&temporary, &path)
                    .map_err(|error| io_error("FS_WRITE_FAILED", &path, error))?;
                Ok(json!({"path": path, "digest": digest_file(&path)?}))
            }
            "fs.patch" | "filesystem.patch" => {
                let path = self.path(&params, "path", true)?;
                let expected_digest = required_str(&params, "expectedDigest")?;
                let actual_digest = digest_file(&path)?;
                if actual_digest != expected_digest {
                    return Err(RpcError::new(
                        "DIGEST_CONFLICT",
                        format!("expected {expected_digest}, got {actual_digest}"),
                    ));
                }
                let before = required_str(&params, "before")?;
                let after = required_str(&params, "after")?;
                let content = fs::read_to_string(&path)
                    .map_err(|error| io_error("FS_PATCH_FAILED", &path, error))?;
                let occurrences = content.matches(before).count();
                if occurrences != 1 {
                    return Err(RpcError::new(
                        "PATCH_CONTEXT_MISMATCH",
                        format!("patch context matched {occurrences} times"),
                    ));
                }
                let patched = content.replacen(before, after, 1);
                let temporary = path.with_extension(format!("fabric.{}.tmp", std::process::id()));
                fs::write(&temporary, patched)
                    .map_err(|error| io_error("FS_PATCH_FAILED", &temporary, error))?;
                atomic_replace(&temporary, &path)
                    .map_err(|error| io_error("FS_PATCH_FAILED", &path, error))?;
                Ok(json!({"path": path, "digest": digest_file(&path)?}))
            }
            "fs.remove" | "filesystem.remove" => {
                let path = self.path(&params, "path", true)?;
                let expected = required_str(&params, "expectedDigest")?;
                let actual = if path.is_dir() {
                    digest_tree(&path)?.0
                } else {
                    digest_file(&path)?
                };
                if actual != expected {
                    return Err(RpcError::new(
                        "DIGEST_CONFLICT",
                        format!("expected {expected}, got {actual}"),
                    ));
                }
                let parent = path
                    .parent()
                    .ok_or_else(|| RpcError::new("PATH_INVALID", "path has no parent"))?;
                let trash = parent.join(".machine-fabric-trash");
                fs::create_dir_all(&trash)
                    .map_err(|error| io_error("FS_REMOVE_FAILED", &trash, error))?;
                let token = format!("{}_{}", now_ms(), uuid::Uuid::new_v4().simple());
                let destination = trash.join(&token);
                fs::rename(&path, &destination)
                    .map_err(|error| io_error("FS_REMOVE_FAILED", &path, error))?;
                Ok(json!({"path": path, "removed": true, "restoreToken": destination}))
            }
            "fs.restore" | "filesystem.restore" => {
                let destination = self.path(&params, "path", false)?;
                let token = self.path(&params, "restoreToken", true)?;
                if token
                    .parent()
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str())
                    != Some(".machine-fabric-trash")
                {
                    return Err(RpcError::new(
                        "INVALID_RESTORE_TOKEN",
                        "restore token is not managed by fabric",
                    ));
                }
                if destination.exists() {
                    return Err(RpcError::new(
                        "RESTORE_CONFLICT",
                        "restore destination already exists",
                    ));
                }
                fs::rename(&token, &destination)
                    .map_err(|error| io_error("FS_RESTORE_FAILED", &destination, error))?;
                Ok(json!({"path": destination, "restored": true}))
            }
            "filesystem.mkdir" => {
                let path = self.path(&params, "path", false)?;
                fs::create_dir_all(&path)
                    .map_err(|error| io_error("FS_MKDIR_FAILED", &path, error))?;
                Ok(json!({"path": path, "created": true}))
            }
            "command.run" => self.run_command(&params),
            "artifact.build" => {
                let result = self.run_command(&params)?;
                let artifact_path = self.path(&params, "artifactPath", true)?;
                let metadata = fs::metadata(&artifact_path)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &artifact_path, error))?;
                let (digest, size, files, kind) = if metadata.is_dir() {
                    let (digest, size, files) = digest_tree(&artifact_path)?;
                    (digest, size, files, "directory")
                } else {
                    (digest_file(&artifact_path)?, metadata.len(), 1, "file")
                };
                Ok(json!({
                    "command": result,
                    "artifact": {
                        "path": artifact_path, "digest": digest, "size": size,
                        "files": files, "kind": kind, "executorId": self.id
                    }
                }))
            }
            "artifact.describe" => {
                let path = self.path(&params, "path", true)?;
                let metadata = fs::metadata(&path)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
                let (digest, size, files, kind) = if metadata.is_file() {
                    (digest_file(&path)?, metadata.len(), 1, "file")
                } else if metadata.is_dir() {
                    let (digest, size, files) = digest_tree(&path)?;
                    (digest, size, files, "directory")
                } else {
                    return Err(RpcError::new(
                        "INVALID_ARTIFACT",
                        "artifact must be a file or directory",
                    ));
                };
                Ok(json!({
                    "digest": digest,
                    "size": size,
                    "files": files,
                    "kind": kind,
                    "path": path,
                    "executorId": self.id,
                }))
            }
            "artifact.relay.archive.create" => {
                let source = self.path(&params, "path", true)?;
                let metadata = fs::metadata(&source)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &source, error))?;
                if !metadata.is_dir() {
                    return Err(RpcError::new(
                        "INVALID_ARTIFACT",
                        "archive relay currently requires a directory artifact",
                    ));
                }
                let mut entries = Vec::new();
                relay_manifest(&source, &source, &mut entries)?;
                let (digest, size, files) = digest_tree(&source)?;
                fs::create_dir_all(&self.relay_root).map_err(|error| {
                    io_error("ARTIFACT_TRANSFER_FAILED", &self.relay_root, error)
                })?;
                let token = uuid::Uuid::new_v4().simple().to_string();
                let archive_path = relay_archive_path(&self.relay_root, &token)?;
                let archive_file = fs::File::create(&archive_path)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                let encoder = GzEncoder::new(archive_file, Compression::fast());
                let mut builder = tar::Builder::new(encoder);
                builder.follow_symlinks(false);
                for entry in &entries {
                    let relative_text = required_str(entry, "path")?;
                    let relative = safe_relative(relative_text)?;
                    let path = source.join(&relative);
                    match required_str(entry, "kind")? {
                        "directory" => builder.append_dir(&relative, &path),
                        "file" => builder.append_path_with_name(&path, &relative),
                        _ => unreachable!("relay_manifest emits only files and directories"),
                    }
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &path, error))?;
                }
                let encoder = builder
                    .into_inner()
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                encoder
                    .finish()
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                let archive_size = fs::metadata(&archive_path)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?
                    .len();
                Ok(json!({
                    "token": token, "compression": "gzip", "archiveSize": archive_size,
                    "digest": digest, "size": size, "files": files
                }))
            }
            "artifact.relay.archive.read" => {
                let token = required_str(&params, "token")?;
                let archive_path = relay_archive_path(&self.relay_root, token)?;
                let offset = params.get("offset").and_then(Value::as_u64).unwrap_or(0);
                let limit = params
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(8 * 1024 * 1024)
                    .min(16 * 1024 * 1024);
                let mut file = fs::File::open(&archive_path)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &archive_path, error))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &archive_path, error))?;
                let mut bytes = vec![0; limit as usize];
                let read = file
                    .read(&mut bytes)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &archive_path, error))?;
                bytes.truncate(read);
                Ok(
                    json!({"token": token, "offset": offset, "data": BASE64.encode(&bytes), "bytes": read, "eof": read < limit as usize}),
                )
            }
            "artifact.relay.archive.remove" => {
                let token = required_str(&params, "token")?;
                let archive_path = relay_archive_path(&self.relay_root, token)?;
                match fs::remove_file(&archive_path) {
                    Ok(()) => Ok(json!({"token": token, "removed": true})),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        Ok(json!({"token": token, "removed": false}))
                    }
                    Err(error) => Err(io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error)),
                }
            }
            "artifact.relay.archive.prepare" => {
                let staging = self.path(&params, "staging", false)?;
                if staging.exists() {
                    fs::remove_dir_all(&staging)
                        .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &staging, error))?;
                }
                fs::create_dir_all(&staging)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &staging, error))?;
                let archive_path = staging.join("payload.tar.gz");
                fs::File::create(&archive_path)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                Ok(json!({"staging": staging}))
            }
            "artifact.relay.archive.write" => {
                let staging = self.path(&params, "staging", true)?;
                let archive_path = staging.join("payload.tar.gz");
                let offset = params
                    .get("offset")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "offset is required"))?;
                let bytes = BASE64
                    .decode(required_str(&params, "data")?)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .open(&archive_path)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                file.write_all(&bytes)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?;
                Ok(json!({"offset": offset, "bytes": bytes.len()}))
            }
            "artifact.relay.archive.commit" => {
                let destination = self.path(&params, "destination", false)?;
                let staging = self.path(&params, "staging", true)?;
                let expected = required_str(&params, "expectedDigest")?;
                let expected_archive_size = params
                    .get("archiveSize")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "archiveSize is required"))?;
                let expected_size = params
                    .get("size")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "size is required"))?;
                let expected_files = params
                    .get("files")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "files is required"))?;
                let archive_path = staging.join("payload.tar.gz");
                let actual_archive_size = fs::metadata(&archive_path)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &archive_path, error))?
                    .len();
                if actual_archive_size != expected_archive_size {
                    return Err(RpcError::new(
                        "ARTIFACT_ARCHIVE_SIZE_MISMATCH",
                        format!(
                            "expected {expected_archive_size} archive bytes, got {actual_archive_size}"
                        ),
                    ));
                }
                let extracted = staging.join("extracted");
                fs::create_dir(&extracted)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &extracted, error))?;
                extract_relay_archive(&archive_path, &extracted, expected_size, expected_files)?;
                let (digest, size, files) = digest_tree(&extracted)?;
                if digest != expected {
                    return Err(RpcError::new(
                        "ARTIFACT_DIGEST_MISMATCH",
                        format!("expected {expected}, got {digest}"),
                    ));
                }
                commit_relay_staging(&destination, &extracted)?;
                let _ = fs::remove_file(&archive_path);
                let _ = fs::remove_dir(&staging);
                Ok(
                    json!({"destination": destination, "digest": digest, "size": size, "files": files}),
                )
            }
            "artifact.relay.manifest" => {
                let source = self.path(&params, "path", true)?;
                let mut entries = Vec::new();
                relay_manifest(&source, &source, &mut entries)?;
                let (digest, size, files) = digest_tree(&source)?;
                Ok(
                    json!({"path": source, "entries": entries, "digest": digest, "size": size, "files": files}),
                )
            }
            "artifact.relay.read" => {
                let source = self.path(&params, "path", true)?;
                let relative = safe_relative(required_str(&params, "relativePath")?)?;
                let path = source.join(relative);
                let offset = params.get("offset").and_then(Value::as_u64).unwrap_or(0);
                let limit = params
                    .get("limit")
                    .and_then(Value::as_u64)
                    .unwrap_or(1024 * 1024)
                    .min(4 * 1024 * 1024);
                let mut file = fs::File::open(&path)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
                let mut bytes = vec![0; limit as usize];
                let read = file
                    .read(&mut bytes)
                    .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
                bytes.truncate(read);
                Ok(
                    json!({"relativePath": required_str(&params, "relativePath")?, "offset": offset, "data": BASE64.encode(&bytes), "bytes": read, "eof": read < limit as usize}),
                )
            }
            "artifact.relay.prepare" => {
                let destination = self.path(&params, "destination", false)?;
                let staging = self.path(&params, "staging", false)?;
                if staging.exists() {
                    fs::remove_dir_all(&staging)
                        .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &staging, error))?;
                }
                fs::create_dir_all(&staging)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &staging, error))?;
                for entry in params
                    .get("entries")
                    .and_then(Value::as_array)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "entries are required"))?
                {
                    let relative = safe_relative(required_str(entry, "path")?)?;
                    let path = staging.join(relative);
                    match required_str(entry, "kind")? {
                        "directory" => fs::create_dir_all(&path),
                        "file" => {
                            if let Some(parent) = path.parent() {
                                fs::create_dir_all(parent).map_err(|error| {
                                    io_error("ARTIFACT_TRANSFER_FAILED", parent, error)
                                })?;
                            }
                            fs::File::create(&path).map(|_| ())
                        }
                        _ => {
                            return Err(RpcError::new(
                                "INVALID_ARTIFACT",
                                "relay supports files and directories only",
                            ));
                        }
                    }
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &path, error))?;
                }
                Ok(json!({"destination": destination, "staging": staging}))
            }
            "artifact.relay.write" => {
                let staging = self.path(&params, "staging", true)?;
                let relative = safe_relative(required_str(&params, "relativePath")?)?;
                let path = staging.join(relative);
                let offset = params
                    .get("offset")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "offset is required"))?;
                let bytes = BASE64
                    .decode(required_str(&params, "data")?)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &path, error))?;
                file.seek(SeekFrom::Start(offset))
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &path, error))?;
                file.write_all(&bytes)
                    .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &path, error))?;
                Ok(
                    json!({"relativePath": required_str(&params, "relativePath")?, "offset": offset, "bytes": bytes.len()}),
                )
            }
            "artifact.relay.commit" => {
                let destination = self.path(&params, "destination", false)?;
                let staging = self.path(&params, "staging", true)?;
                let expected = required_str(&params, "expectedDigest")?;
                let (digest, size, files) = digest_tree(&staging)?;
                if digest != expected {
                    return Err(RpcError::new(
                        "ARTIFACT_DIGEST_MISMATCH",
                        format!("expected {expected}, got {digest}"),
                    ));
                }
                let backup = destination.with_extension(format!(
                    "machine-fabric-backup-{}",
                    uuid::Uuid::new_v4().simple()
                ));
                if destination.exists() {
                    fs::rename(&destination, &backup).map_err(|error| {
                        io_error("ARTIFACT_TRANSFER_FAILED", &destination, error)
                    })?;
                }
                if let Err(error) = fs::rename(&staging, &destination) {
                    if backup.exists() {
                        let _ = fs::rename(&backup, &destination);
                    }
                    return Err(io_error("ARTIFACT_TRANSFER_FAILED", &destination, error));
                }
                if backup.exists() {
                    fs::remove_dir_all(&backup)
                        .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &backup, error))?;
                }
                Ok(
                    json!({"destination": destination, "digest": digest, "size": size, "files": files}),
                )
            }
            "process.start" | "agent.start" => {
                let cwd_input = required_str(&params, "cwd")?;
                let cwd = self.path(&params, "cwd", true)?;
                let argv = string_array(&params, "argv")?;
                validate_command(
                    &argv,
                    cwd_input,
                    params.get("approvalDigest").and_then(Value::as_str),
                )?;
                let process_id = if action == "agent.start" {
                    required_str(&params, "agentId")?.to_owned()
                } else {
                    required_str(&params, "processId")?.to_owned()
                };
                let log_path = if let Some(path) = params.get("logPath").and_then(Value::as_str) {
                    self.path(&json!({"path": path}), "path", false)?
                } else {
                    cwd.join(format!(".fabric-{process_id}.log"))
                };
                let env = params
                    .get("env")
                    .cloned()
                    .map(serde_json::from_value::<BTreeMap<String, String>>)
                    .transpose()
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?
                    .unwrap_or_default();
                Ok(serde_json::to_value(self.processes.start(
                    process_id,
                    cwd,
                    argv,
                    env,
                    log_path,
                    params.get("readiness").cloned(),
                )?)
                .expect("process serializes"))
            }
            "process.get" => Ok(serde_json::to_value(
                self.processes.get(required_str(&params, "processId")?)?,
            )
            .expect("process serializes")),
            "process.list" => Ok(json!({"processes": self.processes.list()})),
            "process.stop" | "agent.stop" => Ok(serde_json::to_value(self.processes.stop(
                if action == "agent.stop" {
                    required_str(&params, "agentId")?
                } else {
                    required_str(&params, "processId")?
                },
            )?)
            .expect("process serializes")),
            "process.events" => {
                let record = self.processes.get(required_str(&params, "processId")?)?;
                Ok(json!({
                    "processId": record.id,
                    "events": [{
                        "type": "process.snapshot", "timestamp": record.updated_at,
                        "state": record.state, "readiness": record.readiness
                    }]
                }))
            }
            "logs.read" => self.processes.logs(
                required_str(&params, "processId")?,
                params.get("tail").and_then(Value::as_u64).unwrap_or(200) as usize,
            ),
            "port.check" => {
                let port = params
                    .get("port")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| RpcError::new("INVALID_PARAMS", "port is required"))?;
                let available = std::net::TcpListener::bind(("127.0.0.1", port as u16)).is_ok();
                Ok(json!({"port": port, "available": available}))
            }
            "application.materialize" => {
                let generation_root = self.path(&params, "generationRoot", false)?;
                let baseline = self.application_path(&params, "baselinePath")?;
                Ok(serde_json::to_value(materialize(
                    &generation_root,
                    required_str(&params, "generationId")?,
                    &baseline,
                )?)
                .expect("generation serializes"))
            }
            "application.apply-artifacts" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let mut overlays: Vec<Overlay> = serde_json::from_value(
                    params
                        .get("overlays")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "overlays are required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                for overlay in &mut overlays {
                    overlay.source = self.path(&json!({"path": overlay.source}), "path", true)?;
                }
                apply_overlays(&application_path, &overlays)
            }
            "artifact.pack-chromium-datapack" => {
                let root_path = self.path(&params, "rootPath", true)?;
                let resource_trees: Vec<DataPackResourceTree> =
                    serde_json::from_value(params.get("resourceTrees").cloned().ok_or_else(
                        || RpcError::new("INVALID_PARAMS", "resourceTrees are required"),
                    )?)
                    .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                let base_pack_path = params
                    .get("basePackPath")
                    .and_then(Value::as_str)
                    .map(|path| self.path(&json!({"path": path}), "path", true))
                    .transpose()?;
                self.path(
                    &json!({"path": root_path.join(required_str(&params, "outputRelative")?)}),
                    "path",
                    false,
                )?;
                for tree in &resource_trees {
                    self.path(
                        &json!({"path": root_path.join(&tree.root_relative)}),
                        "path",
                        true,
                    )?;
                }
                pack_chromium_datapack(
                    &root_path,
                    &resource_trees,
                    Path::new(required_str(&params, "outputRelative")?),
                    params
                        .get("platform")
                        .and_then(Value::as_str)
                        .unwrap_or("win"),
                    params.get("arch").and_then(Value::as_str).unwrap_or("x64"),
                    params
                        .get("bundleName")
                        .and_then(Value::as_str)
                        .unwrap_or("resources"),
                    base_pack_path.as_deref(),
                    params.get("basePackDigest").and_then(Value::as_str),
                    &params
                        .get("changedPrefixes")
                        .and_then(Value::as_array)
                        .map(|items| {
                            items
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default(),
                )
            }
            "application.generation.record" | "application.runtime.record" => {
                let generation_root = self.path(&params, "generationRoot", true)?;
                let mut evidence = params.get("evidence").cloned().unwrap_or_else(|| json!({}));
                if let Some(marker) = params.get("runtimeMarker") {
                    if !evidence.is_object() {
                        return Err(RpcError::new(
                            "INVALID_PARAMS",
                            "evidence must be an object when runtimeMarker is supplied",
                        ));
                    }
                    evidence["runtimeMarker"] = marker.clone();
                }
                record_state(
                    &generation_root,
                    required_str(&params, "generationId")?,
                    required_str(&params, "state")?,
                    evidence,
                )
            }
            "application.activate" => {
                let generation_root = self.path(&params, "generationRoot", true)?;
                activate(&generation_root, required_str(&params, "generationId")?)
            }
            #[cfg(target_os = "macos")]
            "application.inspect" => {
                let application_path = self.application_path(&params, "applicationPath")?;
                crate::macos::inspect(&application_path)
            }
            #[cfg(target_os = "macos")]
            "application.finalize" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let signing_keychain = params
                    .get("signingKeychain")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "signingKeychain", true))
                    .transpose()?;
                let signing_keychain_password_file = params
                    .get("signingKeychainPasswordFile")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "signingKeychainPasswordFile", false))
                    .transpose()?;
                let mut units: Vec<crate::macos::SigningUnit> = serde_json::from_value(
                    params
                        .get("units")
                        .cloned()
                        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "units are required"))?,
                )
                .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?;
                for unit in &mut units {
                    unit.path = self.path(&json!({"path": unit.path}), "path", true)?;
                    if let Some(entitlements) = unit.entitlements.take() {
                        unit.entitlements =
                            Some(self.path(&json!({"path": entitlements}), "path", true)?);
                    }
                }
                crate::macos::finalize(
                    &application_path,
                    units,
                    params
                        .get("identity")
                        .and_then(Value::as_str)
                        .unwrap_or("-"),
                    signing_keychain.as_deref(),
                    signing_keychain_password_file.as_deref(),
                )
            }
            #[cfg(target_os = "macos")]
            "application.launch" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let user_data_dir = params
                    .get("userDataDir")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "userDataDir", false))
                    .transpose()?;
                crate::macos::launch(
                    &application_path,
                    &string_array(&params, "args")?,
                    required_str(&params, "bundleIdentifier")?,
                    params
                        .get("terminateConflictingInstances")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    params
                        .get("remoteDebuggingPort")
                        .and_then(Value::as_u64)
                        .unwrap_or(9222) as u16,
                    crate::macos::LaunchOptions {
                        env: &crate::process::application_environment(params.get("env"))?,
                        user_data_dir: user_data_dir.as_deref(),
                        chromium_local_state_patch: params.get("chromiumLocalStatePatch"),
                        browser_executable_relative: params
                            .get("browserExecutableRelative")
                            .and_then(Value::as_str),
                    },
                )
            }
            #[cfg(target_os = "macos")]
            "application.open-file" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let file = self.path(&params, "file", true)?;
                let handler = params
                    .get("handlerPath")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "handlerPath", true))
                    .transpose()?;
                crate::macos::open_file(&application_path, &file, handler.as_deref())
            }
            #[cfg(target_os = "macos")]
            "application.stop" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                crate::macos::stop(&application_path)
            }
            #[cfg(target_os = "macos")]
            "ui.inspect" => Ok(json!({
                "targets": crate::macos::cdp_pages(
                    params
                        .get("remoteDebuggingPort")
                        .and_then(Value::as_u64)
                        .unwrap_or(9222) as u16,
                )?
            })),
            #[cfg(target_os = "macos")]
            "ui.evaluate" => crate::macos::cdp_evaluate(
                params
                    .get("remoteDebuggingPort")
                    .and_then(Value::as_u64)
                    .unwrap_or(9222) as u16,
                params.get("targetUrlPrefix").and_then(Value::as_str),
                required_str(&params, "expression")?,
            ),
            #[cfg(target_os = "macos")]
            "ui.automate" => crate::macos::cdp_automate(
                params
                    .get("remoteDebuggingPort")
                    .and_then(Value::as_u64)
                    .unwrap_or(9222) as u16,
                params.get("targetUrlPrefix").and_then(Value::as_str),
                required_str(&params, "method")?,
                params.get("params").cloned().unwrap_or_else(|| json!({})),
            ),
            #[cfg(target_os = "macos")]
            "ui.capture" => {
                let output = self.path(&params, "output", false)?;
                crate::macos::cdp_capture(
                    params
                        .get("remoteDebuggingPort")
                        .and_then(Value::as_u64)
                        .unwrap_or(9222) as u16,
                    params.get("targetUrlPrefix").and_then(Value::as_str),
                    &output,
                )
            }
            #[cfg(target_os = "macos")]
            "ui.native-inspect" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                crate::macos::native_inspect(
                    &application_path,
                    params
                        .get("requestPermission")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                )
            }
            #[cfg(windows)]
            "application.inspect" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                crate::windows::inspect(&application_path)
            }
            #[cfg(windows)]
            "application.launch" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let user_data_dir = params
                    .get("userDataDir")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "userDataDir", false))
                    .transpose()?;
                let runtime_shadow_dir = params
                    .get("runtimeShadowDir")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "runtimeShadowDir", false))
                    .transpose()?;
                let chromium_local_state_path = params
                    .get("chromiumLocalStatePath")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "chromiumLocalStatePath", false))
                    .transpose()?;
                let file = params
                    .get("file")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "file", true))
                    .transpose()?;
                crate::windows::launch(
                    &application_path,
                    &string_array(&params, "args")?,
                    crate::windows::LaunchOptions {
                        env: &crate::process::application_environment(params.get("env"))?,
                        user_data_dir: user_data_dir.as_deref(),
                        runtime_shadow_dir: runtime_shadow_dir.as_deref(),
                        chromium_local_state_path: chromium_local_state_path.as_deref(),
                        chromium_local_state_patch: params.get("chromiumLocalStatePatch"),
                        chromium_local_state_settle_ms: params
                            .get("chromiumLocalStateSettleMs")
                            .and_then(Value::as_u64)
                            .unwrap_or(0),
                        file: file.as_deref(),
                        remote_debugging_port: params
                            .get("remoteDebuggingPort")
                            .and_then(Value::as_u64)
                            .map(|port| port as u16),
                        terminate_conflicting_instances: params
                            .get("terminateConflictingInstances")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                    },
                )
            }
            #[cfg(windows)]
            "application.open-file" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let file = self.path(&params, "file", true)?;
                let handler = params
                    .get("handlerPath")
                    .and_then(Value::as_str)
                    .map(|_| self.path(&params, "handlerPath", true))
                    .transpose()?;
                crate::windows::open_file(&application_path, &file, handler.as_deref())
            }
            #[cfg(windows)]
            "ui.evaluate" => crate::windows::cdp_evaluate(
                params
                    .get("remoteDebuggingPort")
                    .and_then(Value::as_u64)
                    .unwrap_or(9222) as u16,
                params.get("targetUrlPrefix").and_then(Value::as_str),
                required_str(&params, "expression")?,
            ),
            #[cfg(windows)]
            "ui.native-inspect" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                crate::windows::native_inspect(
                    &application_path,
                    params.get("expectedWindowTitle").and_then(Value::as_str),
                )
            }
            #[cfg(windows)]
            "ui.capture" => {
                let application_path = self.path(&params, "applicationPath", true)?;
                let output = self.path(&params, "output", false)?;
                crate::windows::capture_window(
                    &application_path,
                    params.get("expectedWindowTitle").and_then(Value::as_str),
                    &output,
                )
            }
            _ => Err(RpcError::new(
                "UNKNOWN_ACTION",
                format!("unknown action: {action}"),
            )),
        }
    }

    fn path(&self, params: &Value, key: &str, must_exist: bool) -> Result<PathBuf, RpcError> {
        let raw = params
            .get(key)
            .and_then(Value::as_str)
            .ok_or_else(|| RpcError::new("INVALID_PARAMS", format!("{key} is required")))?;
        let path = PathBuf::from(raw);
        let checked = if must_exist || path.exists() {
            path.canonicalize()
                .map_err(|error| io_error("PATH_INVALID", &path, error))?
        } else {
            let mut ancestor = path.as_path();
            let mut missing = Vec::new();
            while !ancestor.exists() {
                let name = ancestor.file_name().ok_or_else(|| {
                    RpcError::new("PATH_INVALID", "path has no existing ancestor")
                })?;
                missing.push(name.to_owned());
                ancestor = ancestor
                    .parent()
                    .ok_or_else(|| RpcError::new("PATH_INVALID", "path has no parent"))?;
            }
            let mut resolved = ancestor
                .canonicalize()
                .map_err(|error| io_error("PATH_INVALID", &path, error))?;
            for name in missing.into_iter().rev() {
                resolved.push(name);
            }
            resolved
        };
        if !self
            .allowed_roots
            .iter()
            .any(|root| checked.starts_with(root))
        {
            return Err(RpcError::new(
                "PATH_OUTSIDE_ALLOWED_ROOTS",
                format!("{} is outside executor roots", checked.display()),
            ));
        }
        Ok(checked)
    }

    fn application_path(&self, params: &Value, key: &str) -> Result<PathBuf, RpcError> {
        let raw = required_str(params, key)?;
        let checked = PathBuf::from(raw)
            .canonicalize()
            .map_err(|error| io_error("PATH_INVALID", Path::new(raw), error))?;
        let managed = self
            .allowed_roots
            .iter()
            .any(|root| checked.starts_with(root));
        let system_application = cfg!(target_os = "macos")
            && checked.starts_with("/Applications")
            && checked.extension().and_then(|value| value.to_str()) == Some("app");
        if !managed && !system_application {
            return Err(RpcError::new(
                "PATH_OUTSIDE_APPLICATION_ROOTS",
                format!(
                    "{} is outside managed roots and /Applications",
                    checked.display()
                ),
            ));
        }
        Ok(checked)
    }

    fn run_command(&self, params: &Value) -> Result<Value, RpcError> {
        let cwd_input = required_str(params, "cwd")?;
        let cwd = self.path(params, "cwd", true)?;
        let argv = string_array(params, "argv")?;
        validate_command(
            &argv,
            cwd_input,
            params.get("approvalDigest").and_then(Value::as_str),
        )?;
        let env = params
            .get("env")
            .cloned()
            .map(serde_json::from_value::<BTreeMap<String, String>>)
            .transpose()
            .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?
            .unwrap_or_default();
        let output = Command::new(&argv[0])
            .args(&argv[1..])
            .current_dir(&cwd)
            .envs(env)
            .stdin(Stdio::null())
            .output()
            .map_err(|error| RpcError::new("COMMAND_FAILED", error.to_string()))?;
        let stdout = bounded_output(&output.stdout);
        let stderr = bounded_output(&output.stderr);
        if !output.status.success() {
            return Err(RpcError::new(
                "COMMAND_FAILED",
                format!("command exited with {}: {stderr}", output.status),
            ));
        }
        Ok(json!({
            "cwd": cwd, "argv": argv, "exitCode": output.status.code(),
            "stdout": stdout, "stderr": stderr
        }))
    }
}

pub fn capability_catalog() -> Vec<CapabilityDescriptor> {
    #[allow(unused_mut)] // macOS appends platform-only application/UI providers.
    let mut capabilities = vec![
        ("filesystem.resolve", Effect::ReadOnly),
        ("filesystem.stat", Effect::ReadOnly),
        ("filesystem.read", Effect::ReadOnly),
        ("filesystem.list", Effect::ReadOnly),
        ("filesystem.search", Effect::ReadOnly),
        ("filesystem.write", Effect::Mutating),
        ("filesystem.patch", Effect::Mutating),
        ("filesystem.remove", Effect::Mutating),
        ("filesystem.restore", Effect::Mutating),
        ("filesystem.mkdir", Effect::Mutating),
        ("command.run", Effect::Mutating),
        ("artifact.build", Effect::Mutating),
        ("artifact.describe", Effect::ReadOnly),
        ("artifact.pack-chromium-datapack", Effect::Mutating),
        ("artifact.relay.archive.create", Effect::ReadOnly),
        ("artifact.relay.archive.read", Effect::ReadOnly),
        ("artifact.relay.archive.remove", Effect::ReadOnly),
        ("artifact.relay.archive.prepare", Effect::Mutating),
        ("artifact.relay.archive.write", Effect::Mutating),
        ("artifact.relay.archive.commit", Effect::Mutating),
        ("artifact.relay.manifest", Effect::ReadOnly),
        ("artifact.relay.read", Effect::ReadOnly),
        ("artifact.relay.prepare", Effect::Mutating),
        ("artifact.relay.write", Effect::Mutating),
        ("artifact.relay.commit", Effect::Mutating),
        ("process.start", Effect::Mutating),
        ("process.get", Effect::ReadOnly),
        ("process.list", Effect::ReadOnly),
        ("process.stop", Effect::Mutating),
        ("process.events", Effect::ReadOnly),
        ("logs.read", Effect::ReadOnly),
        ("port.check", Effect::ReadOnly),
        ("agent.start", Effect::Mutating),
        ("agent.stop", Effect::Mutating),
        ("application.materialize", Effect::Mutating),
        ("application.apply-artifacts", Effect::Mutating),
        ("application.generation.record", Effect::Mutating),
        ("application.runtime.record", Effect::Mutating),
        ("application.activate", Effect::Mutating),
    ];
    #[cfg(target_os = "macos")]
    capabilities.extend([
        ("application.inspect", Effect::ReadOnly),
        ("application.finalize", Effect::Mutating),
        ("application.launch", Effect::Mutating),
        ("application.open-file", Effect::Mutating),
        ("application.stop", Effect::Mutating),
        ("ui.inspect", Effect::ReadOnly),
        ("ui.evaluate", Effect::ReadOnly),
        ("ui.automate", Effect::Mutating),
        ("ui.capture", Effect::ReadOnly),
        ("ui.native-inspect", Effect::ReadOnly),
    ]);
    #[cfg(windows)]
    capabilities.extend([
        ("application.inspect", Effect::ReadOnly),
        ("application.launch", Effect::Mutating),
        ("application.open-file", Effect::Mutating),
        ("ui.evaluate", Effect::ReadOnly),
        ("ui.capture", Effect::ReadOnly),
        ("ui.native-inspect", Effect::ReadOnly),
    ]);
    capabilities
        .into_iter()
        .map(|(name, effect)| contract(name, effect))
        .collect()
}

fn contract(name: &str, effect: Effect) -> CapabilityDescriptor {
    let (required, properties, locks, key_fields, timeout_ms, rollback, evidence) = match name {
        "filesystem.resolve" | "filesystem.stat" | "filesystem.read" | "filesystem.list" => (
            vec!["filesystem"],
            json!({"path": {"type": "string"}}),
            Vec::new(),
            vec!["path"],
            30_000,
            RollbackStrategy::None,
            vec!["filesystem-result"],
        ),
        "filesystem.search" => (
            vec!["filesystem"],
            json!({
                "path": {"type": "string"},
                "query": {"type": "string"},
                "maxResults": {"type": "integer", "minimum": 1}
            }),
            Vec::new(),
            vec!["path", "query", "maxResults"],
            60_000,
            RollbackStrategy::None,
            vec!["search-results"],
        ),
        "filesystem.write" => (
            vec!["filesystem"],
            json!({
                "path": {"type": "string"},
                "content": {"type": "string"},
                "expectedDigest": {"type": "string"}
            }),
            vec!["filesystem:${path}"],
            vec!["path", "content", "expectedDigest"],
            30_000,
            RollbackStrategy::None,
            vec!["file-digest"],
        ),
        "filesystem.patch" => (
            vec!["filesystem"],
            json!({
                "path": {"type": "string"},
                "before": {"type": "string"},
                "after": {"type": "string"},
                "expectedDigest": {"type": "string"}
            }),
            vec!["filesystem:${path}"],
            vec!["path", "before", "after", "expectedDigest"],
            30_000,
            RollbackStrategy::None,
            vec!["file-digest"],
        ),
        "filesystem.remove" => (
            vec!["filesystem"],
            json!({
                "path": {"type": "string"},
                "expectedDigest": {"type": "string"}
            }),
            vec!["filesystem:${path}"],
            vec!["path", "expectedDigest"],
            30_000,
            RollbackStrategy::Compensate {
                capability: "filesystem.restore".to_owned(),
            },
            vec!["restore-token"],
        ),
        "filesystem.restore" => (
            vec!["filesystem"],
            json!({
                "path": {"type": "string"},
                "restoreToken": {"type": "string"}
            }),
            vec!["filesystem:${path}"],
            vec!["path", "restoreToken"],
            30_000,
            RollbackStrategy::None,
            vec!["restore-result"],
        ),
        "filesystem.mkdir" => (
            vec!["filesystem"],
            json!({"path": {"type": "string"}}),
            vec!["filesystem:${path}"],
            vec!["path"],
            30_000,
            RollbackStrategy::None,
            vec!["directory-result"],
        ),
        "command.run" => (
            vec!["process"],
            json!({
                "cwd": {"type": "string"},
                "argv": {"type": "array", "items": {"type": "string"}},
                "env": {"type": "object", "additionalProperties": {"type": "string"}},
                "approvalDigest": {"type": "string"}
            }),
            vec!["command:${cwd}"],
            vec!["cwd", "argv", "env"],
            3_600_000,
            RollbackStrategy::None,
            vec!["command-result"],
        ),
        "artifact.build" => (
            vec!["process", "filesystem"],
            json!({
                "cwd": {"type": "string"},
                "argv": {"type": "array", "items": {"type": "string"}},
                "env": {"type": "object", "additionalProperties": {"type": "string"}},
                "artifactPath": {"type": "string"},
                "artifactType": {"type": "string"},
                "approvalDigest": {"type": "string"}
            }),
            vec!["build:${artifactPath}"],
            vec!["cwd", "argv", "env", "artifactPath", "artifactType"],
            3_600_000,
            RollbackStrategy::None,
            vec!["build-log", "artifact-digest", "provenance"],
        ),
        "artifact.describe" => (
            vec!["filesystem"],
            json!({"path": {"type": "string"}}),
            Vec::new(),
            vec!["path"],
            600_000,
            RollbackStrategy::None,
            vec!["artifact-digest", "provenance"],
        ),
        "artifact.relay.archive.create"
        | "artifact.relay.archive.read"
        | "artifact.relay.archive.remove"
        | "artifact.relay.manifest"
        | "artifact.relay.read" => (
            vec!["filesystem"],
            json!({"path": {"type": "string"}, "token": {"type": "string"}, "relativePath": {"type": "string"}, "offset": {"type": "integer"}, "limit": {"type": "integer"}}),
            Vec::new(),
            vec!["path", "relativePath", "offset", "limit"],
            600_000,
            RollbackStrategy::None,
            vec!["artifact-relay-source"],
        ),
        "artifact.relay.archive.prepare"
        | "artifact.relay.archive.write"
        | "artifact.relay.archive.commit"
        | "artifact.relay.prepare"
        | "artifact.relay.write"
        | "artifact.relay.commit" => (
            vec!["filesystem"],
            json!({"destination": {"type": "string"}, "staging": {"type": "string"}, "entries": {"type": "array"}, "relativePath": {"type": "string"}, "offset": {"type": "integer"}, "data": {"type": "string"}, "expectedDigest": {"type": "string"}, "archiveSize": {"type": "integer"}, "size": {"type": "integer"}, "files": {"type": "integer"}}),
            vec!["artifact-relay:${destination}"],
            vec!["destination", "staging"],
            3_600_000,
            RollbackStrategy::None,
            vec!["artifact-relay-destination"],
        ),
        "artifact.pack-chromium-datapack" => (
            vec!["chromium-datapack"],
            json!({
                "rootPath": {"type": "string"},
                "resourceTrees": {"type": "array"},
                "outputRelative": {"type": "string"},
                "platform": {"type": "string"},
                "arch": {"type": "string"},
                "bundleName": {"type": "string"},
                "basePackPath": {"type": "string"},
                "basePackDigest": {"type": "string"},
                "changedPrefixes": {"type": "array", "items": {"type": "string"}}
            }),
            vec!["datapack:${rootPath}/${outputRelative}"],
            vec!["rootPath", "resourceTrees", "outputRelative"],
            3_600_000,
            RollbackStrategy::RetainPreviousGeneration,
            vec!["packed-resource-digest", "packed-resource-manifest"],
        ),
        "application.materialize" => (
            vec!["filesystem-copy"],
            json!({
                "generationRoot": {"type": "string"},
                "generationId": {"type": "string"},
                "baselinePath": {"type": "string"}
            }),
            vec!["generation:${generationRoot}/${generationId}"],
            vec!["generationRoot", "generationId", "baselinePath"],
            3_600_000,
            RollbackStrategy::RetainPreviousGeneration,
            vec!["generation-marker", "baseline-identity"],
        ),
        "application.apply-artifacts" => (
            vec!["filesystem-copy"],
            json!({
                "applicationPath": {"type": "string"},
                "overlays": {"type": "array"}
            }),
            vec!["application:${applicationPath}"],
            vec!["applicationPath", "overlays"],
            3_600_000,
            RollbackStrategy::RetainPreviousGeneration,
            vec!["applied-artifacts", "generation-marker"],
        ),
        "application.finalize" => (
            vec!["macos-codesign"],
            json!({
                "applicationPath": {"type": "string"},
                "identity": {"type": "string"},
                "signingKeychain": {"type": "string"},
                "signingKeychainPasswordFile": {"type": "string"},
                "units": {"type": "array"}
            }),
            vec!["application:${applicationPath}"],
            vec!["applicationPath", "units", "identity"],
            600_000,
            RollbackStrategy::RetainPreviousGeneration,
            vec!["signature-verification"],
        ),
        "application.activate" => (
            vec!["atomic-symlink"],
            json!({
                "generationRoot": {"type": "string"},
                "generationId": {"type": "string"},
                "fence": {"type": "integer"}
            }),
            vec!["activation:${generationRoot}/current"],
            vec!["generationRoot", "generationId"],
            30_000,
            RollbackStrategy::RetainPreviousGeneration,
            vec!["activation-record"],
        ),
        "application.generation.record" | "application.runtime.record" => (
            vec!["filesystem"],
            json!({
                "generationRoot": {"type": "string"},
                "generationId": {"type": "string"},
                "state": {"type": "string"},
                "evidence": {"type": "object"},
                "runtimeMarker": {}
            }),
            vec!["generation:${generationRoot}/${generationId}"],
            vec!["generationRoot", "generationId", "state", "evidence"],
            30_000,
            RollbackStrategy::RetainPreviousGeneration,
            vec!["generation-record"],
        ),
        "application.inspect" => (
            vec![if cfg!(windows) {
                "windows-application"
            } else {
                "macos-application"
            }],
            json!({"applicationPath": {"type": "string"}}),
            Vec::new(),
            vec!["applicationPath"],
            30_000,
            RollbackStrategy::None,
            vec!["application-identity"],
        ),
        "application.launch" => (
            vec![if cfg!(windows) {
                "windows-application"
            } else {
                "macos-application"
            }],
            json!({
                "applicationPath": {"type": "string"},
                "bundleIdentifier": {"type": "string"},
                "args": {"type": "array", "items": {"type": "string"}},
                "env": {"type": "object", "additionalProperties": {"type": "string"}},
                "userDataDir": {"type": "string"},
                "chromiumLocalStatePatch": {"type": "object"},
                "browserExecutableRelative": {"type": "string"},
                "runtimeShadowDir": {"type": "string"},
                "chromiumLocalStatePath": {"type": "string"},
                "chromiumLocalStateSettleMs": {"type": "integer", "minimum": 0, "maximum": 30000},
                "file": {"type": "string"},
                "remoteDebuggingPort": {"type": "integer"},
                "terminateConflictingInstances": {"type": "boolean"}
            }),
            vec![
                "application-instance:${bundleIdentifier}",
                "debug-port:${remoteDebuggingPort}",
            ],
            vec![
                "applicationPath",
                "bundleIdentifier",
                "args",
                "env",
                "terminateConflictingInstances",
            ],
            120_000,
            RollbackStrategy::Compensate {
                capability: "process.stop".to_owned(),
            },
            vec!["process", "readiness"],
        ),
        "application.open-file" => (
            vec![if cfg!(windows) {
                "windows-shell"
            } else {
                "launch-services"
            }],
            json!({
                "applicationPath": {"type": "string"},
                "file": {"type": "string"},
                "handlerPath": {"type": "string"}
            }),
            vec!["application-instance:${applicationPath}"],
            vec!["applicationPath", "file"],
            30_000,
            RollbackStrategy::None,
            vec!["open-result"],
        ),
        "application.stop" => (
            vec![if cfg!(windows) {
                "windows-application"
            } else {
                "macos-application"
            }],
            json!({"applicationPath": {"type": "string"}}),
            vec!["application-instance:${applicationPath}"],
            vec!["applicationPath"],
            30_000,
            RollbackStrategy::None,
            vec!["stop-result"],
        ),
        "ui.inspect" => (
            vec!["cdp"],
            json!({"remoteDebuggingPort": {"type": "integer"}}),
            Vec::new(),
            Vec::new(),
            30_000,
            RollbackStrategy::None,
            vec!["ui-snapshot"],
        ),
        "ui.evaluate" => (
            vec!["cdp"],
            json!({
                "remoteDebuggingPort": {"type": "integer"},
                "targetUrlPrefix": {"type": "string"},
                "expression": {"type": "string"}
            }),
            Vec::new(),
            vec!["remoteDebuggingPort", "targetUrlPrefix", "expression"],
            30_000,
            RollbackStrategy::None,
            vec!["evaluation-result"],
        ),
        "ui.automate" => (
            vec!["cdp"],
            json!({
                "remoteDebuggingPort": {"type": "integer"},
                "targetUrlPrefix": {"type": "string"},
                "method": {"type": "string"},
                "params": {"type": "object"}
            }),
            vec!["ui-target:${remoteDebuggingPort}"],
            vec!["remoteDebuggingPort", "targetUrlPrefix", "method", "params"],
            30_000,
            RollbackStrategy::None,
            vec!["automation-result"],
        ),
        #[cfg(not(windows))]
        "ui.capture" => (
            vec!["cdp"],
            json!({
                "remoteDebuggingPort": {"type": "integer"},
                "targetUrlPrefix": {"type": "string"},
                "output": {"type": "string"}
            }),
            Vec::new(),
            vec!["remoteDebuggingPort", "targetUrlPrefix", "output"],
            30_000,
            RollbackStrategy::None,
            vec!["screenshot"],
        ),
        #[cfg(windows)]
        "ui.capture" => (
            vec!["windows-desktop", "printwindow"],
            json!({
                "applicationPath": {"type": "string"},
                "expectedWindowTitle": {"type": "string"},
                "output": {"type": "string"}
            }),
            Vec::new(),
            vec!["applicationPath", "expectedWindowTitle", "output"],
            30_000,
            RollbackStrategy::None,
            vec!["screenshot", "capture-backend", "window-bounds"],
        ),
        "ui.native-inspect" => (
            vec![if cfg!(windows) {
                "windows-accessibility"
            } else {
                "macos-accessibility"
            }],
            json!({
                "applicationPath": {"type": "string"},
                "requestPermission": {"type": "boolean"},
                "expectedWindowTitle": {"type": "string"}
            }),
            Vec::new(),
            vec!["applicationPath"],
            30_000,
            RollbackStrategy::None,
            vec!["native-window-tree", "accessibility-trust"],
        ),
        "process.start" => (
            vec!["process"],
            json!({
                "processId": {"type": "string"},
                "cwd": {"type": "string"},
                "argv": {"type": "array", "items": {"type": "string"}},
                "env": {"type": "object", "additionalProperties": {"type": "string"}},
                "logPath": {"type": "string"},
                "readiness": {"type": "object"}
            }),
            vec!["process:${processId}"],
            vec!["processId", "argv", "cwd"],
            30_000,
            RollbackStrategy::Compensate {
                capability: "process.stop".to_owned(),
            },
            vec!["process-record", "readiness"],
        ),
        "process.get" | "process.stop" | "process.events" | "logs.read" => (
            vec!["process"],
            json!({
                "processId": {"type": "string"},
                "tail": {"type": "integer"}
            }),
            if name == "process.stop" {
                vec!["process:${processId}"]
            } else {
                Vec::new()
            },
            vec!["processId"],
            30_000,
            RollbackStrategy::None,
            vec!["process-record"],
        ),
        "process.list" => (
            vec!["process"],
            json!({}),
            Vec::new(),
            Vec::new(),
            30_000,
            RollbackStrategy::None,
            vec!["process-list"],
        ),
        "port.check" => (
            vec!["network"],
            json!({"port": {"type": "integer", "minimum": 1, "maximum": 65535}}),
            Vec::new(),
            vec!["port"],
            30_000,
            RollbackStrategy::None,
            vec!["port-status"],
        ),
        "agent.start" => (
            vec!["process", "agent-runtime"],
            json!({
                "agentId": {"type": "string"},
                "role": {"type": "string"},
                "cwd": {"type": "string"},
                "argv": {"type": "array", "items": {"type": "string"}},
                "env": {"type": "object", "additionalProperties": {"type": "string"}},
                "logPath": {"type": "string"},
                "readiness": {"type": "object"}
            }),
            vec!["agent:${agentId}"],
            vec!["agentId", "role", "cwd", "argv"],
            60_000,
            RollbackStrategy::Compensate {
                capability: "agent.stop".to_owned(),
            },
            vec!["agent-process", "readiness"],
        ),
        "agent.stop" => (
            vec!["process", "agent-runtime"],
            json!({"agentId": {"type": "string"}}),
            vec!["agent:${agentId}"],
            vec!["agentId"],
            30_000,
            RollbackStrategy::None,
            vec!["agent-process"],
        ),
        _ => (
            Vec::new(),
            json!({}),
            Vec::new(),
            Vec::new(),
            30_000,
            RollbackStrategy::None,
            Vec::new(),
        ),
    };
    let optional_fields = [
        "mode",
        "identity",
        "fence",
        "remoteDebuggingPort",
        "targetUrlPrefix",
        "requestPermission",
        "maxResults",
        "env",
        "logPath",
        "readiness",
        "tail",
        "fence",
        "approvalDigest",
    ];
    // Schema presence is distinct from idempotency identity. Several native
    // handlers provide defaults or only use these inputs for an optional mode.
    let capability_optional_fields: &[&str] = match name {
        "artifact.pack-chromium-datapack" => &[
            "platform",
            "arch",
            "bundleName",
            "basePackPath",
            "basePackDigest",
            "changedPrefixes",
        ],
        "application.generation.record" | "application.runtime.record" => {
            &["evidence", "runtimeMarker"]
        }
        "application.finalize" => &["signingKeychain", "signingKeychainPasswordFile"],
        "application.launch" => &[
            "env",
            "userDataDir",
            "chromiumLocalStatePatch",
            "browserExecutableRelative",
            "runtimeShadowDir",
            "chromiumLocalStatePath",
            "chromiumLocalStateSettleMs",
            "file",
        ],
        "application.open-file" => &["handlerPath"],
        _ => &[],
    };
    let schema_required: Vec<String> = properties
        .as_object()
        .map(|object| {
            object
                .keys()
                .filter(|key| {
                    !(optional_fields.contains(&key.as_str())
                        || capability_optional_fields.contains(&key.as_str())
                        || name == "filesystem.write" && key.as_str() == "expectedDigest")
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let retryable_idempotent = !key_fields.is_empty();
    CapabilityDescriptor {
        name: name.to_owned(),
        version: "1.0.0".to_owned(),
        input_schema: json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": properties,
            "required": schema_required,
            "additionalProperties": true
        }),
        output_schema: output_schema(name),
        required_executor_features: required.into_iter().map(str::to_owned).collect(),
        locks: locks
            .into_iter()
            .map(|key| machine_fabric_schema::LockTemplate {
                key: key.to_owned(),
                mode: machine_fabric_schema::LockMode::Exclusive,
            })
            .collect(),
        idempotency: IdempotencyContract {
            key_fields: key_fields.into_iter().map(str::to_owned).collect(),
            retention_ms: 86_400_000,
        },
        timeout_ms,
        retry: RetryPolicy {
            max_attempts: if matches!(effect, Effect::ReadOnly) || retryable_idempotent {
                3
            } else {
                1
            },
            retryable_errors: vec!["EXECUTOR_UNAVAILABLE".to_owned()],
            backoff_ms: 500,
        },
        rollback,
        health_check: HealthCheck::Process,
        emitted_evidence: evidence.into_iter().map(str::to_owned).collect(),
        effect,
    }
}

fn output_schema(name: &str) -> Value {
    let properties = match name {
        "filesystem.resolve" => json!({
            "path": {"type": "string"}, "exists": {"type": "boolean"}
        }),
        "filesystem.stat" => json!({
            "path": {"type": "string"}, "exists": {"type": "boolean"},
            "kind": {"type": "string"}, "size": {"type": "integer"},
            "digest": {"type": ["string", "null"]}
        }),
        "filesystem.read" => json!({
            "path": {"type": "string"}, "content": {"type": "string"},
            "digest": {"type": "string"}, "size": {"type": "integer"}
        }),
        "filesystem.list" => json!({"path": {"type": "string"}, "entries": {"type": "array"}}),
        "filesystem.search" => json!({
            "path": {"type": "string"}, "query": {"type": "string"},
            "matches": {"type": "array"}, "truncated": {"type": "boolean"}
        }),
        "filesystem.write" | "filesystem.patch" => {
            json!({"path": {"type": "string"}, "digest": {"type": "string"}})
        }
        "filesystem.remove" => json!({
            "path": {"type": "string"}, "removed": {"type": "boolean"},
            "restoreToken": {"type": "string"}
        }),
        "filesystem.restore" => {
            json!({"path": {"type": "string"}, "restored": {"type": "boolean"}})
        }
        "filesystem.mkdir" => json!({
            "path": {"type": "string"}, "created": {"type": "boolean"}
        }),
        "artifact.describe" => json!({
            "digest": {"type": "string"}, "size": {"type": "integer"},
            "files": {"type": "integer"}, "kind": {"type": "string"},
            "path": {"type": "string"}, "executorId": {"type": "string"}
        }),
        "command.run" => json!({
            "cwd": {"type": "string"}, "argv": {"type": "array"},
            "exitCode": {"type": ["integer", "null"]},
            "stdout": {"type": "string"}, "stderr": {"type": "string"}
        }),
        "artifact.build" => json!({
            "command": {"type": "object"}, "artifact": {"type": "object"}
        }),
        "process.start" | "process.get" | "process.stop" | "agent.start" | "agent.stop" => json!({
            "id": {"type": "string"}, "state": {"type": "string"},
            "pid": {"type": ["integer", "null"]}
        }),
        "application.materialize" => json!({
            "generationId": {"type": "string"}, "root": {"type": "string"},
            "applicationPath": {"type": "string"}, "markerPath": {"type": "string"},
            "createdAt": {"type": "integer"}
        }),
        "application.activate" => json!({
            "current": {"type": "string"}, "previous": {"type": ["string", "null"]},
            "generationId": {"type": "string"}, "activatedAt": {"type": "integer"}
        }),
        "application.generation.record" | "application.runtime.record" => json!({
            "generationId": {"type": "string"}, "state": {"type": "string"}
        }),
        "application.inspect" => json!({
            "path": {"type": "string"},
            "bundleIdentifier": {"type": ["string", "null"]},
            "version": {}, "build": {}, "documentTypes": {}
        }),
        "application.finalize" => json!({
            "applicationPath": {"type": "string"}, "signed": {"type": "array"},
            "verified": {"type": "boolean"}
        }),
        "application.launch" => json!({
            "applicationPath": {"type": "string"}, "pid": {"type": "integer"},
            "args": {"type": "array"}, "cdp": {"type": "object"}
        }),
        "artifact.pack-chromium-datapack" => json!({
            "output": {"type": "string"}, "resources": {"type": "integer"},
            "entries": {"type": "integer"}, "size": {"type": "integer"},
            "sha256": {"type": "string"}, "contentHash": {"type": "string"}
        }),
        "application.apply-artifacts" => json!({
            "applied": {"type": "array"}
        }),
        "application.open-file" => json!({
            "applicationPath": {"type": "string"}, "handlerPath": {"type": "string"},
            "file": {"type": "string"},
            "openedAt": {"type": "integer"}
        }),
        "application.stop" => json!({
            "applicationPath": {"type": "string"}, "stopped": {"type": "array"}
        }),
        "ui.inspect" => json!({"targets": {"type": "array"}}),
        "ui.automate" => json!({"target": {"type": "object"}, "result": {}}),
        "ui.capture" => json!({
            "path": {"type": "string"}, "target": {"type": "object"},
            "bytes": {"type": "integer"}, "capturedAt": {"type": "integer"}
        }),
        "logs.read" => json!({
            "processId": {"type": "string"}, "path": {"type": "string"},
            "lines": {"type": "array"}
        }),
        "process.list" => json!({"processes": {"type": "array"}}),
        "port.check" => json!({
            "port": {"type": "integer"}, "available": {"type": "boolean"}
        }),
        "ui.evaluate" => json!({
            "target": {"type": "object"}, "value": {}, "evaluatedAt": {"type": "integer"}
        }),
        "ui.native-inspect" => json!({
            "applicationPath": {"type": "string"}, "pids": {"type": "array"},
            "inspection": {"type": "object"}, "inspectedAt": {"type": "integer"}
        }),
        "process.events" => json!({
            "processId": {"type": "string"}, "events": {"type": "array"}
        }),
        _ => json!({}),
    };
    let required = properties
        .as_object()
        .map(|object| object.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    json!({
        "type": "object", "properties": properties,
        "required": required, "additionalProperties": true
    })
}

fn persist_fences(fences: &ExecutorFences) -> Result<(), RpcError> {
    if let Some(parent) = fences.path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            RpcError::new(
                "FENCE_STATE_FAILED",
                format!("{}: {error}", parent.display()),
            )
        })?;
    }
    let temporary = fences
        .path
        .with_extension(format!("json.{}.tmp", std::process::id()));
    fs::write(
        &temporary,
        serde_json::to_vec_pretty(fences).expect("fences serialize"),
    )
    .map_err(|error| RpcError::new("FENCE_STATE_FAILED", error.to_string()))?;
    atomic_replace(&temporary, &fences.path)
        .map_err(|error| RpcError::new("FENCE_STATE_FAILED", error.to_string()))
}

fn render_authority_resource(template: &str, params: &Value) -> Result<String, RpcError> {
    let mut rendered = template.to_owned();
    while let Some(start) = rendered.find("${") {
        let relative_end = rendered[start + 2..]
            .find('}')
            .ok_or_else(|| RpcError::new("INVALID_LOCK_TEMPLATE", template))?;
        let end = start + 2 + relative_end;
        let key = &rendered[start + 2..end];
        let value = params.get(key).ok_or_else(|| {
            RpcError::new(
                "EXECUTOR_AUTHORITY_MISMATCH",
                format!("capability lock requires {key}"),
            )
        })?;
        let value = value
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| value.to_string());
        rendered.replace_range(start..=end, &value);
    }
    Ok(rendered)
}

fn execution_resources(
    action: &str,
    params: &Value,
    executor_id: &str,
) -> Result<Vec<String>, RpcError> {
    let canonical = match action {
        "fs.write" => "filesystem.write",
        "fs.patch" => "filesystem.patch",
        "fs.remove" => "filesystem.remove",
        "fs.restore" => "filesystem.restore",
        other => other,
    };
    let Some(contract) = capability_catalog()
        .into_iter()
        .find(|capability| capability.name == canonical)
    else {
        return Ok(Vec::new());
    };
    let mut resources = contract
        .locks
        .iter()
        .map(|lock| render_authority_resource(&lock.key, params))
        .collect::<Result<Vec<_>, _>>()?;
    if !matches!(contract.effect, Effect::ReadOnly) && resources.is_empty() {
        let scope = params
            .get("_workspaceSessionId")
            .and_then(Value::as_str)
            .filter(|scope| !scope.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("executor:{executor_id}:{canonical}"));
        resources.push(format!("workspace:{scope}"));
    }
    resources.sort();
    resources.dedup();
    Ok(resources)
}

fn required_str<'a>(params: &'a Value, key: &str) -> Result<&'a str, RpcError> {
    params
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| RpcError::new("INVALID_PARAMS", format!("{key} is required")))
}

fn string_array(params: &Value, key: &str) -> Result<Vec<String>, RpcError> {
    params
        .get(key)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| RpcError::new("INVALID_PARAMS", error.to_string()))?
        .filter(|values: &Vec<String>| !values.is_empty())
        .ok_or_else(|| RpcError::new("INVALID_PARAMS", format!("{key} is required")))
}

fn validate_command(
    argv: &[String],
    cwd: &str,
    approval_digest: Option<&str>,
) -> Result<(), RpcError> {
    let executable = Path::new(&argv[0])
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&argv[0]);
    if matches!(executable, "rm" | "sudo" | "su" | "dd" | "mkfs") {
        let digest = sha256_bytes(
            &serde_json::to_vec(&json!({"cwd": cwd, "argv": argv})).expect("command serializes"),
        );
        if approval_digest != Some(digest.as_str()) {
            return Err(RpcError::new(
                "COMMAND_BLOCKED",
                format!("high-risk executable requires an exact one-shot approval: {executable}"),
            ));
        }
    }
    if matches!(executable, "python" | "python3" | "node" | "ruby" | "perl")
        && argv.iter().any(|value| value == "-c" || value == "-e")
    {
        return Err(RpcError::new(
            "COMMAND_BLOCKED",
            "inline interpreter execution is not allowed",
        ));
    }
    Ok(())
}

fn bounded_output(bytes: &[u8]) -> String {
    const LIMIT: usize = 64 * 1024;
    let start = bytes.len().saturating_sub(LIMIT);
    String::from_utf8_lossy(&bytes[start..]).into_owned()
}

fn digest_file(path: &Path) -> Result<String, RpcError> {
    let bytes = fs::read(path).map_err(|error| io_error("FS_READ_FAILED", path, error))?;
    Ok(sha256_bytes(&bytes))
}

fn digest_tree(root: &Path) -> Result<(String, u64, u64), RpcError> {
    fn collect(root: &Path, directory: &Path, paths: &mut Vec<PathBuf>) -> Result<(), RpcError> {
        for entry in fs::read_dir(directory)
            .map_err(|error| io_error("ARTIFACT_READ_FAILED", directory, error))?
        {
            let entry =
                entry.map_err(|error| io_error("ARTIFACT_READ_FAILED", directory, error))?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
            if metadata.is_dir() {
                collect(root, &path, paths)?;
            } else if metadata.is_file() || metadata.file_type().is_symlink() {
                paths.push(
                    path.strip_prefix(root)
                        .expect("child is below root")
                        .to_path_buf(),
                );
            }
        }
        Ok(())
    }
    let mut paths = Vec::new();
    collect(root, root, &mut paths)?;
    paths.sort();
    let mut digest = Sha256::new();
    let mut size = 0_u64;
    for relative in &paths {
        let path = root.join(relative);
        digest.update(relative.as_os_str().as_encoded_bytes());
        digest.update([0]);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
        if metadata.file_type().is_symlink() {
            let target = fs::read_link(&path)
                .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
            digest.update(target.as_os_str().as_encoded_bytes());
        } else {
            let bytes =
                fs::read(&path).map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
            size = size.saturating_add(bytes.len() as u64);
            digest.update(&bytes);
        }
        digest.update([0]);
    }
    Ok((
        format!("sha256:{}", hex::encode(digest.finalize())),
        size,
        paths.len() as u64,
    ))
}

fn safe_relative(value: &str) -> Result<PathBuf, RpcError> {
    let path = PathBuf::from(value);
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(RpcError::new(
            "INVALID_ARTIFACT_PATH",
            format!("unsafe relative path: {value}"),
        ));
    }
    Ok(path)
}

fn relay_archive_path(root: &Path, token: &str) -> Result<PathBuf, RpcError> {
    if token.len() != 32 || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(RpcError::new(
            "INVALID_ARTIFACT_TOKEN",
            "archive relay token must be 32 hexadecimal characters",
        ));
    }
    Ok(root.join(format!("{token}.tar.gz")))
}

fn extract_relay_archive(
    archive_path: &Path,
    destination: &Path,
    expected_size: u64,
    expected_files: u64,
) -> Result<(), RpcError> {
    let file = fs::File::open(archive_path)
        .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", archive_path, error))?;
    let mut archive = tar::Archive::new(GzDecoder::new(file));
    let mut size = 0_u64;
    let mut files = 0_u64;
    let entries = archive
        .entries()
        .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", archive_path, error))?;
    for entry in entries {
        let mut entry =
            entry.map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", archive_path, error))?;
        let kind = entry.header().entry_type();
        if !(kind.is_file() || kind.is_dir()) {
            return Err(RpcError::new(
                "INVALID_ARTIFACT",
                "archive relay supports only regular files and directories",
            ));
        }
        let path = entry
            .path()
            .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", archive_path, error))?
            .into_owned();
        let safe = path.components().all(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::Normal(_)
            )
        });
        if path.is_absolute() || !safe {
            return Err(RpcError::new(
                "INVALID_ARTIFACT_PATH",
                format!("unsafe archive path: {}", path.display()),
            ));
        }
        if kind.is_file() {
            files = files.saturating_add(1);
            size = size.saturating_add(entry.size());
            if files > expected_files || size > expected_size {
                return Err(RpcError::new(
                    "ARTIFACT_ARCHIVE_LIMIT_EXCEEDED",
                    "archive contents exceed the declared artifact limits",
                ));
            }
        }
        if !entry
            .unpack_in(destination)
            .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", destination, error))?
        {
            return Err(RpcError::new(
                "INVALID_ARTIFACT_PATH",
                format!("archive path escapes destination: {}", path.display()),
            ));
        }
    }
    if files != expected_files || size != expected_size {
        return Err(RpcError::new(
            "ARTIFACT_ARCHIVE_CONTENT_MISMATCH",
            format!(
                "expected {expected_files} files/{expected_size} bytes, got {files} files/{size} bytes"
            ),
        ));
    }
    Ok(())
}

fn commit_relay_staging(destination: &Path, staging: &Path) -> Result<(), RpcError> {
    let backup = destination.with_extension(format!(
        "machine-fabric-backup-{}",
        uuid::Uuid::new_v4().simple()
    ));
    if destination.exists() {
        fs::rename(destination, &backup)
            .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", destination, error))?;
    }
    if let Err(error) = fs::rename(staging, destination) {
        if backup.exists() {
            let _ = fs::rename(&backup, destination);
        }
        return Err(io_error("ARTIFACT_TRANSFER_FAILED", destination, error));
    }
    if backup.exists() {
        fs::remove_dir_all(&backup)
            .map_err(|error| io_error("ARTIFACT_TRANSFER_FAILED", &backup, error))?;
    }
    Ok(())
}

fn relay_manifest(root: &Path, directory: &Path, entries: &mut Vec<Value>) -> Result<(), RpcError> {
    let mut children = fs::read_dir(directory)
        .map_err(|error| io_error("ARTIFACT_READ_FAILED", directory, error))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| io_error("ARTIFACT_READ_FAILED", directory, error))?;
    children.sort_by_key(|entry| entry.file_name());
    for entry in children {
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| io_error("ARTIFACT_READ_FAILED", &path, error))?;
        let relative = path
            .strip_prefix(root)
            .expect("manifest child is below root");
        let relative = relative.to_str().ok_or_else(|| {
            RpcError::new("INVALID_ARTIFACT_PATH", "artifact paths must be UTF-8")
        })?;
        if metadata.is_dir() {
            entries.push(json!({"path": relative, "kind": "directory"}));
            relay_manifest(root, &path, entries)?;
        } else if metadata.is_file() {
            entries.push(json!({"path": relative, "kind": "file", "size": metadata.len()}));
        } else {
            return Err(RpcError::new(
                "INVALID_ARTIFACT",
                format!(
                    "relay does not support symlinks or special files: {}",
                    path.display()
                ),
            ));
        }
    }
    Ok(())
}

fn search_tree(
    path: &Path,
    query: &str,
    max_results: usize,
    matches: &mut Vec<Value>,
) -> Result<(), RpcError> {
    if matches.len() >= max_results {
        return Ok(());
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("FS_SEARCH_FAILED", path, error))?;
    if metadata.is_dir() {
        let mut children = fs::read_dir(path)
            .map_err(|error| io_error("FS_SEARCH_FAILED", path, error))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| io_error("FS_SEARCH_FAILED", path, error))?;
        children.sort_by_key(|entry| entry.file_name());
        for entry in children {
            search_tree(&entry.path(), query, max_results, matches)?;
            if matches.len() >= max_results {
                break;
            }
        }
    } else if metadata.is_file() && metadata.len() <= 16 * 1024 * 1024 {
        let bytes = fs::read(path).map_err(|error| io_error("FS_SEARCH_FAILED", path, error))?;
        let content = String::from_utf8_lossy(&bytes);
        for (index, line) in content.lines().enumerate() {
            if line.contains(query) {
                matches.push(json!({"path": path, "line": index + 1, "text": line}));
                if matches.len() >= max_results {
                    break;
                }
            }
        }
    }
    Ok(())
}

fn io_error(code: &str, path: &Path, error: std::io::Error) -> RpcError {
    RpcError::new(code, format!("{}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "macos")]
    #[test]
    fn launch_admits_new_isolated_profile_but_keeps_path_policy() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let params = json!({"applicationPath": directory.path(), "userDataDir": directory.path().join("new-profile"), "args": ["--example"], "bundleIdentifier": "test.app"});
        let error = runtime
            .dispatch("application.launch", params.clone())
            .unwrap_err();
        assert_eq!(error.code, "UNSAFE_CREDENTIAL_MODE");
        let mut outside = params;
        outside["userDataDir"] = json!(directory.path().parent().unwrap().join("outside-profile"));
        assert_eq!(
            runtime
                .dispatch("application.launch", outside)
                .unwrap_err()
                .code,
            "PATH_OUTSIDE_ALLOWED_ROOTS"
        );
    }

    #[test]
    fn runtime_record_preserves_marker_and_rejects_invalid_evidence() {
        let directory = tempfile::tempdir().unwrap();
        let generation = directory.path().join("generations/example");
        fs::create_dir_all(&generation).unwrap();
        fs::write(generation.join("generation.json"), b"{}").unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let params = json!({"generationRoot": directory.path(), "generationId": "example", "state": "active", "evidence": {"checked": true}, "runtimeMarker": {"build": "test"}});
        runtime
            .dispatch("application.runtime.record", params.clone())
            .unwrap();
        let marker: Value =
            serde_json::from_slice(&fs::read(generation.join("generation.json")).unwrap()).unwrap();
        assert_eq!(marker["evidence"]["runtimeMarker"]["build"], "test");
        assert_eq!(marker["evidence"]["checked"], true);
        assert_eq!(marker["state"], "active");
        let mut invalid = params;
        invalid["evidence"] = Value::Null;
        assert!(
            runtime
                .dispatch("application.runtime.record", invalid)
                .is_err()
        );
    }

    use super::*;

    #[test]
    fn application_launch_contract_requires_explicit_conflict_policy() {
        let descriptor = contract("application.launch", Effect::Mutating);
        assert_eq!(
            descriptor.input_schema["properties"]["terminateConflictingInstances"]["type"],
            "boolean"
        );
        assert!(
            descriptor.input_schema["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|field| field == "terminateConflictingInstances")
        );
    }

    #[test]
    fn application_finalize_contract_accepts_noninteractive_keychain_inputs() {
        let descriptor = contract("application.finalize", Effect::Mutating);
        assert_eq!(
            descriptor.input_schema["properties"]["signingKeychain"]["type"],
            "string"
        );
        assert_eq!(
            descriptor.input_schema["properties"]["signingKeychainPasswordFile"]["type"],
            "string"
        );
    }

    #[test]
    fn publish_contracts_do_not_require_optional_mode_inputs() {
        for (name, required) in [
            (
                "artifact.pack-chromium-datapack",
                json!(["outputRelative", "resourceTrees", "rootPath"]),
            ),
            (
                "application.generation.record",
                json!(["generationId", "generationRoot", "state"]),
            ),
            (
                "application.runtime.record",
                json!(["generationId", "generationRoot", "state"]),
            ),
            ("application.finalize", json!(["applicationPath", "units"])),
            (
                "application.launch",
                json!([
                    "applicationPath",
                    "args",
                    "bundleIdentifier",
                    "terminateConflictingInstances"
                ]),
            ),
            ("application.open-file", json!(["applicationPath", "file"])),
        ] {
            assert_eq!(
                contract(name, Effect::Mutating).input_schema["required"],
                required,
                "{name}"
            );
        }
    }

    #[test]
    fn execution_capacity_queues_conflicting_resources_fairly() {
        let capacity = std::sync::Arc::new(ExecutionCapacity::new(2));
        let first = capacity
            .acquire(vec!["workspace:shared".to_owned()])
            .expect("first permit");
        let waiting_capacity = std::sync::Arc::clone(&capacity);
        let (sender, receiver) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let permit = waiting_capacity
                .acquire(vec!["workspace:shared".to_owned()])
                .expect("queued permit");
            sender.send(()).expect("notify admission");
            std::thread::sleep(Duration::from_millis(25));
            drop(permit);
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while capacity.snapshot().2 == 0 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(capacity.snapshot().2, 1, "same-resource work queues");
        assert!(receiver.try_recv().is_err(), "queued work has not started");
        drop(first);
        receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("queued work is admitted after release");
        waiter.join().unwrap();
        assert_eq!(capacity.snapshot().0, 0);
        assert_eq!(capacity.snapshot().2, 0);
    }

    #[test]
    fn execution_capacity_rejects_only_after_queue_limit_is_reached() {
        let capacity = ExecutionCapacity::configured(1, 0, Duration::from_secs(1));
        let _permit = capacity.acquire(Vec::new()).expect("first permit");
        let error = capacity
            .acquire(Vec::new())
            .err()
            .expect("full queue rejects excess work");
        assert_eq!(error.code, "EXECUTOR_QUEUE_FULL");
        assert!(error.retryable);
    }
    use machine_fabric_protocol::Request;

    #[test]
    fn guards_roots_and_detects_write_conflicts() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let path = directory.path().join("file.txt");
        let first = runtime.handle(Request::new(
            "filesystem.write",
            json!({"path": path, "content": "one"}),
        ));
        assert!(first.ok);
        let nested = directory.path().join("new/deep/file.txt");
        let nested_write = runtime.handle(Request::new(
            "filesystem.write",
            json!({"path": nested, "content": "nested"}),
        ));
        assert!(nested_write.ok, "{nested_write:?}");
        let conflict = runtime.handle(Request::new(
            "filesystem.write",
            json!({"path": path, "content": "two", "expectedDigest": "sha256:wrong"}),
        ));
        assert!(!conflict.ok);
    }

    #[test]
    fn archive_relay_round_trips_a_directory_and_replaces_destination() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let source = directory.path().join("source");
        fs::create_dir_all(source.join("nested")).unwrap();
        fs::write(source.join("small.txt"), b"small").unwrap();
        fs::write(source.join("nested/data.bin"), vec![7_u8; 64 * 1024]).unwrap();
        let destination = directory.path().join("destination");
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("stale.txt"), b"stale").unwrap();

        let created = runtime.handle(Request::new(
            "artifact.relay.archive.create",
            json!({"path": source}),
        ));
        assert!(created.ok, "{created:?}");
        let archive = created.result.unwrap();
        let token = archive["token"].as_str().unwrap();
        let staging = directory.path().join("staging");
        let prepared = runtime.handle(Request::new(
            "artifact.relay.archive.prepare",
            json!({"destination": destination, "staging": staging}),
        ));
        assert!(prepared.ok, "{prepared:?}");
        let read = runtime.handle(Request::new(
            "artifact.relay.archive.read",
            json!({"token": token, "offset": 0, "limit": 1024 * 1024}),
        ));
        assert!(read.ok, "{read:?}");
        let chunk = read.result.unwrap();
        assert_eq!(chunk["bytes"], archive["archiveSize"]);
        let written = runtime.handle(Request::new(
            "artifact.relay.archive.write",
            json!({"destination": destination, "staging": staging, "offset": 0, "data": chunk["data"]}),
        ));
        assert!(written.ok, "{written:?}");
        let committed = runtime.handle(Request::new(
            "artifact.relay.archive.commit",
            json!({
                "destination": destination, "staging": staging,
                "expectedDigest": archive["digest"], "archiveSize": archive["archiveSize"],
                "size": archive["size"], "files": archive["files"]
            }),
        ));
        assert!(committed.ok, "{committed:?}");
        assert_eq!(fs::read(destination.join("small.txt")).unwrap(), b"small");
        assert_eq!(
            fs::read(destination.join("nested/data.bin")).unwrap(),
            vec![7_u8; 64 * 1024]
        );
        assert!(!destination.join("stale.txt").exists());
        let removed = runtime.handle(Request::new(
            "artifact.relay.archive.remove",
            json!({"token": token}),
        ));
        assert!(removed.ok, "{removed:?}");
    }

    #[cfg(unix)]
    #[test]
    fn archive_relay_rejects_symlinks() {
        use std::os::unix::fs::symlink;

        let directory = tempfile::tempdir().unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let source = directory.path().join("source");
        fs::create_dir(&source).unwrap();
        symlink("/etc/passwd", source.join("escape")).unwrap();
        let response = runtime.handle(Request::new(
            "artifact.relay.archive.create",
            json!({"path": source}),
        ));
        assert_eq!(response.error.unwrap().code, "INVALID_ARTIFACT");
    }

    #[test]
    fn persistent_executor_fences_are_issued_locally_across_controllers() {
        let directory = tempfile::tempdir().unwrap();
        let state = directory.path().join("fences.json");
        let path = directory.path().join("fenced.txt");
        let resource = format!("filesystem:{}", path.display());
        let runtime =
            ExecutorRuntime::open("local", vec![directory.path().to_path_buf()], state.clone())
                .unwrap();
        let first = runtime.handle(Request::new(
            "filesystem.write",
            json!({"path": path, "content": "one", "_authority": [{"controllerId": "controller-a", "resource": resource, "fence": 999}]}),
        ));
        assert!(first.ok, "{first:?}");
        drop(runtime);

        let runtime =
            ExecutorRuntime::open("local", vec![directory.path().to_path_buf()], state).unwrap();
        let next = runtime.handle(Request::new(
            "filesystem.write",
            json!({"path": path, "content": "next", "_authority": [{"controllerId": "controller-b", "resource": resource, "fence": 1}]}),
        ));
        assert!(next.ok, "{next:?}");
        let fences = runtime.fences.as_ref().unwrap().lock().unwrap();
        assert_eq!(fences.resources[&resource].controller_id, "executor:local");
        assert_eq!(fences.resources[&resource].fence, 2);
    }

    #[test]
    fn negotiates_contract_major_version_and_rejects_incompatible_callers() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let compatible = runtime.handle(Request::new(
            "capability.negotiate",
            json!({"name": "application.materialize", "version": "1.7.0"}),
        ));
        assert!(compatible.ok);
        let incompatible = runtime.handle(Request::new(
            "capability.negotiate",
            json!({"name": "application.materialize", "version": "2.0.0"}),
        ));
        assert_eq!(
            incompatible.error.unwrap().code,
            "CAPABILITY_VERSION_UNSUPPORTED"
        );
        let contract = capability_catalog()
            .into_iter()
            .find(|item| item.name == "application.materialize")
            .unwrap();
        assert_eq!(
            contract.input_schema["required"],
            json!(["baselinePath", "generationId", "generationRoot"])
        );

        let patch = capability_catalog()
            .into_iter()
            .find(|item| item.name == "filesystem.patch")
            .unwrap();
        assert!(
            patch.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("expectedDigest"))
        );
        let remove = capability_catalog()
            .into_iter()
            .find(|item| item.name == "filesystem.remove")
            .unwrap();
        assert!(
            remove.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("expectedDigest"))
        );
        let write = capability_catalog()
            .into_iter()
            .find(|item| item.name == "filesystem.write")
            .unwrap();
        assert!(
            !write.input_schema["required"]
                .as_array()
                .unwrap()
                .contains(&json!("expectedDigest"))
        );
    }

    #[test]
    fn patches_and_recovers_removed_files() {
        let directory = tempfile::tempdir().unwrap();
        let runtime = ExecutorRuntime::new("local", vec![directory.path().to_path_buf()]).unwrap();
        let path = directory.path().join("file.txt");
        fs::write(&path, "before\nneedle\nafter\n").unwrap();
        let digest = digest_file(&path).unwrap();
        let patched = runtime.handle(Request::new(
            "filesystem.patch",
            json!({"path": path, "expectedDigest": digest, "before": "needle", "after": "changed"}),
        ));
        assert!(patched.ok);
        let patched_digest = patched.result.unwrap()["digest"]
            .as_str()
            .unwrap()
            .to_owned();
        let removed = runtime.handle(Request::new(
            "filesystem.remove",
            json!({"path": path, "expectedDigest": patched_digest}),
        ));
        assert!(removed.ok);
        assert!(!path.exists());
        let token = removed.result.unwrap()["restoreToken"]
            .as_str()
            .unwrap()
            .to_owned();
        let restored = runtime.handle(Request::new(
            "filesystem.restore",
            json!({"path": path, "restoreToken": token}),
        ));
        assert!(restored.ok);
        assert_eq!(
            fs::read_to_string(path).unwrap(),
            "before\nchanged\nafter\n"
        );
    }

    #[test]
    #[cfg(unix)]
    fn command_build_returns_an_immutable_artifact_description() {
        let directory = tempfile::tempdir().unwrap();
        let runtime =
            ExecutorRuntime::new("builder", vec![directory.path().to_path_buf()]).unwrap();
        let artifact = directory.path().join("artifact.txt");
        let built = runtime.handle(Request::new(
            "artifact.build",
            json!({
                "cwd": directory.path(),
                "argv": ["/usr/bin/touch", artifact],
                "artifactPath": artifact,
                "artifactType": "test-output"
            }),
        ));
        assert!(built.ok, "{built:?}");
        assert_eq!(built.result.unwrap()["artifact"]["files"], 1);
    }
}
