//! One durable FIFO per Executor interactive desktop. Controllers are submitters.
use machine_fabric_core::{atomic_replace, now_ms};
use machine_fabric_protocol::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[cfg(unix)]
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::PathBuf};

// Missing broadcasts are bounded by this lease. An explicit false snapshot
// revokes input immediately, independently of its remaining lifetime.
const AVAILABILITY_MAX_AGE_MS: u64 = 10_000;
const AVAILABILITY_MAX_LIFETIME_MS: u64 = 10_000;

fn snapshot_allows_input(v: &Value, now: u64) -> bool {
    let (Some(observed), Some(expires)) = (v["observedAtMs"].as_u64(), v["expiresAtMs"].as_u64())
    else {
        return false;
    };
    v["schemaVersion"] == 1
        && v["provider"] == "away-guard"
        && v["state"] == "armed"
        && v["generation"]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.len() <= 256)
        && v["sequence"].as_u64().is_some_and(|n| n > 0)
        && v["available"] == true
        && observed <= now
        && now - observed <= AVAILABILITY_MAX_AGE_MS
        && expires > now
        && expires - observed <= AVAILABILITY_MAX_LIFETIME_MS
}

#[derive(Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DesktopQueue {
    #[serde(skip)]
    path: Option<PathBuf>,
    epoch: u64,
    jobs: Vec<Job>,
    // Persist before dispatch: an interrupted action is never replayed on restart.
    in_flight: bool,
    blocked: bool,
    // Stable across Executor/helper restarts, different after an OS restart.
    #[serde(default)]
    boot_id: Option<String>,
    #[serde(default)]
    last_recovery: Option<Value>,
    #[serde(default)]
    maintenance_owner: Option<String>,
    // Once a local provider is configured, callers cannot opt out or turn a
    // missing/dead provider into permission. Explicit node administration is
    // required to remove the policy; no desktop RPC clears it.
    #[serde(default)]
    availability_required: bool,
    // An in-memory barrier for the narrow read-only inspection allowed while
    // blocked. It is deliberately not persisted; the durable blocked state is.
    #[serde(skip)]
    recovery_inspection_active: bool,
    #[serde(skip)]
    notification_wakes: u64,
    #[serde(skip)]
    polling_wakes: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Job {
    id: String,
    owner: String,
    request_key: String,
    token: String,
    state: String,
    epoch: u64,
    ttl_ms: u64,
    submitted_at: u64,
    expires_at: u64,
    #[serde(default)]
    requires_availability: bool,
    #[serde(default)]
    deadline_at: Option<u64>,
    #[serde(default)]
    claimed_by: Option<String>,
}
fn error(code: &str, message: impl ToString) -> RpcError {
    RpcError::new(code, message.to_string())
}
fn field<'a>(v: &'a Value, name: &str) -> Result<&'a str, RpcError> {
    v[name]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 256)
        .ok_or_else(|| error("INVALID_PARAMS", format!("{name} must be 1..256 bytes")))
}

pub(crate) fn validate_recovery_inspection_params(
    action: &str,
    params: &Value,
) -> Result<(), RpcError> {
    if action != "ui.native-inspect" {
        return Err(error(
            "RECOVERY_INSPECT_ACTION_INVALID",
            "recovery inspection only admits ui.native-inspect",
        ));
    }
    let Some(object) = params.as_object() else {
        return Err(error("INVALID_PARAMS", "input must be an object"));
    };
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "applicationPath" | "requestPermission" | "recoveryInspection"
        )
    }) || params["recoveryInspection"] != true
        || params["requestPermission"] != false
        || params["applicationPath"].as_str().is_none_or(str::is_empty)
    {
        return Err(error(
            "RECOVERY_INSPECT_PARAMS_INVALID",
            "only applicationPath, recoveryInspection=true, and requestPermission=false are allowed",
        ));
    }
    Ok(())
}
pub(crate) fn protected(action: &str) -> bool {
    action == "computer-use.call"
        || action == "computer-use.tools"
        || action.starts_with("application.")
        || action.starts_with("ui.")
        || action == "clipboard.write"
}
// Boot identifiers are kernel evidence, not process uptime or connectivity.
fn current_boot_id() -> Option<String> {
    #[cfg(target_os = "linux")]
    let value = fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    #[cfg(target_os = "macos")]
    let value = {
        let mut bytes = [0u8; 128];
        let mut size = bytes.len();
        let result = unsafe {
            libc::sysctlbyname(
                c"kern.bootsessionuuid".as_ptr(),
                bytes.as_mut_ptr().cast(),
                &mut size,
                std::ptr::null_mut(),
                0,
            )
        };
        if result != 0 || size == 0 || size > bytes.len() {
            return None;
        }
        std::str::from_utf8(&bytes[..size])
            .ok()?
            .trim_end_matches('\0')
            .to_owned()
    };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    return None;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    uuid::Uuid::parse_str(value.trim())
        .ok()
        .map(|id| id.to_string())
}

impl DesktopQueue {
    pub(crate) fn open(path: PathBuf) -> Result<Self, RpcError> {
        Self::open_with_boot_id(path, current_boot_id())
    }
    fn open_with_boot_id(path: PathBuf, boot_id: Option<String>) -> Result<Self, RpcError> {
        let mut queue: Self = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| error("DESKTOP_STATE_FAILED", e))?)
                .map_err(|e| error("DESKTOP_STATE_FAILED", e))?
        } else {
            Self::default()
        };
        queue.path = Some(path);
        let restarted =
            matches!((&queue.boot_id, &boot_id), (Some(old), Some(current)) if old != current);
        let interrupted = queue.in_flight
            || queue
                .jobs
                .iter()
                .any(|j| matches!(j.state.as_str(), "active" | "draining"));
        if restarted {
            // A real OS restart ends every old helper and clears native key state.
            // Retire old grants; never replay their actions or reuse active tokens.
            let mut retired = 0;
            for job in &mut queue.jobs {
                if matches!(job.state.as_str(), "active" | "draining") {
                    job.state = "interrupted".into();
                    retired += 1;
                }
            }
            if queue.blocked || interrupted {
                queue.last_recovery = Some(
                    json!({"reason":"os_restart", "observedAt":now_ms(), "interruptedSessions":retired, "inputReplayed":false}),
                );
            }
            queue.blocked = false;
            queue.in_flight = false;
        } else if interrupted {
            // A helper may survive an Executor restart. Missing boot evidence is
            // never evidence of a reset (including legacy queue files).
            queue.blocked = true;
            queue.in_flight = false;
        }
        // Preserve known evidence if the OS query temporarily fails.
        if boot_id.is_some() {
            queue.boot_id = boot_id;
        }
        queue.save()?;
        Ok(queue)
    }
    fn save(&self) -> Result<(), RpcError> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let persist = || -> std::io::Result<()> {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(&serde_json::to_vec(self)?)?;
            file.sync_all()?;
            atomic_replace(&temp, path)
        };
        persist().map_err(|e| error("DESKTOP_STATE_FAILED", e))
    }
    fn active(&self) -> Option<usize> {
        self.jobs
            .iter()
            .position(|j| j.state == "active" || j.state == "draining")
    }
    pub(crate) fn needs_cleanup(&mut self) -> bool {
        if self.in_flight {
            return false;
        }
        if let Some(i) = self.active() {
            if self.jobs[i].expires_at <= now_ms()
                || self.jobs[i].deadline_at.is_some_and(|at| at <= now_ms())
            {
                self.jobs[i].state = "draining".into();
            }
            return self.jobs[i].state == "draining";
        }
        false
    }
    pub(crate) fn cleanup_done(&mut self, success: bool) -> Result<(), RpcError> {
        if success {
            if let Some(i) = self.active() {
                // Stopping owned helpers is cleanup, not recovery or evidence
                // that the timed-out desktop action completed successfully.
                self.jobs[i].state = if self.blocked {
                    "interrupted"
                } else if self.jobs[i].expires_at <= now_ms() {
                    "expired"
                } else {
                    "completed"
                }
                .into();
            }
        } else {
            self.blocked = true;
        }
        self.save()?;
        self.promote()
    }
    pub(crate) fn worker_reconcile(&mut self, notification: bool) -> Result<(), RpcError> {
        if notification {
            self.notification_wakes = self.notification_wakes.saturating_add(1);
        } else {
            self.polling_wakes = self.polling_wakes.saturating_add(1);
        }
        self.promote()
    }
    // The provider is local to this Executor. RPC callers can request the gate,
    // but cannot provide a snapshot or select a different file.
    fn availability(&self) -> Value {
        let snapshot = self
            .path
            .as_ref()
            .and_then(|p| fs::read(p.with_file_name("desktop-availability.json")).ok())
            .filter(|bytes| bytes.len() <= 4096)
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
        let now = now_ms();
        let available = snapshot
            .as_ref()
            .is_some_and(|v| snapshot_allows_input(v, now));
        json!({"available":available,"snapshot":snapshot,
            "policy":{"maxAgeMs":AVAILABILITY_MAX_AGE_MS,"maxLifetimeMs":AVAILABILITY_MAX_LIFETIME_MS}})
    }
    fn refresh_availability_policy(&mut self) -> Result<(), RpcError> {
        if !self.availability_required
            && self.path.as_ref().is_some_and(|p| {
                p.with_file_name("desktop-availability.json")
                    .symlink_metadata()
                    .is_ok()
            })
        {
            self.availability_required = true;
            // Keep the in-memory policy closed even if persistence fails.
            self.save()?;
        }
        Ok(())
    }
    // A node-local operator receipt is distinct from the unattended provider.
    // No RPC parameter, environment flag or old token can create this grant.
    fn user_authorization(&self, job: &Job) -> Option<Value> {
        if !matches!(job.state.as_str(), "queued" | "active") {
            return None;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let path = self
                .path
                .as_ref()?
                .with_file_name("desktop-user-authorization.json");
            let metadata = fs::symlink_metadata(&path).ok()?;
            if !metadata.is_file()
                || metadata.len() > 4096
                || metadata.mode() & 0o777 != 0o600
                || metadata.uid() != unsafe { libc::geteuid() }
            {
                return None;
            }
            let value: Value = serde_json::from_slice(&fs::read(path).ok()?).ok()?;
            let now = now_ms();
            let granted = value["grantedAtMs"].as_u64()?;
            let expires = value["expiresAtMs"].as_u64()?;
            if value["schemaVersion"] != 1
                || value["source"] != "explicit-user"
                || value["jobId"] != job.id
                || value["owner"] != job.owner
                || value["requestKey"] != job.request_key
                || value["tokenSha256"] != hex::encode(Sha256::digest(job.token.as_bytes()))
                || value["bootId"].as_str() != self.boot_id.as_deref()
                || self.boot_id.is_none()
                || granted > now
                || expires <= now
                || expires.saturating_sub(granted) > 900_000
                || value["authorizationId"]
                    .as_str()
                    .and_then(|s| uuid::Uuid::parse_str(s).ok())
                    .is_none()
                || !value["reason"]
                    .as_str()
                    .is_some_and(|s| !s.is_empty() && s.len() <= 512)
            {
                return None;
            }
            Some(
                json!({"source":"explicit-user", "authorizationId":value["authorizationId"],
                "grantedAtMs":granted,"expiresAtMs":expires}),
            )
        }
        #[cfg(not(unix))]
        {
            let _ = job;
            None
        }
    }
    pub(crate) fn promote(&mut self) -> Result<(), RpcError> {
        if self.recovery_inspection_active {
            return Ok(());
        }
        self.refresh_availability_policy()?;
        let now = now_ms();
        let mut expired = false;
        for job in &mut self.jobs {
            if job.state == "queued" && job.deadline_at.is_some_and(|at| at <= now) {
                job.state = "expired".into();
                expired = true;
            }
        }
        if expired {
            self.save()?;
        }
        let available = self.availability()["available"] == true;
        // An unavailable unattended head keeps its FIFO position. Only a
        // separately authorized job can run while that head is ineligible.
        let candidate = self
            .jobs
            .iter()
            .position(|j| j.state == "queued")
            .filter(|&i| {
                !(self.jobs[i].requires_availability || self.availability_required) || available
            })
            .or_else(|| {
                self.jobs
                    .iter()
                    .position(|j| j.state == "queued" && self.user_authorization(j).is_some())
            });
        if self.maintenance_owner.is_none()
            && !self.blocked
            && !self.in_flight
            && self.active().is_none()
            && let Some(index) = candidate
        {
            let job = &mut self.jobs[index];
            let old_job = job.clone();
            self.epoch += 1;
            job.epoch = self.epoch;
            job.state = "active".into();
            job.expires_at = now_ms().saturating_add(job.ttl_ms);
            let id = job.id.clone();
            if let Err(error) = self.save() {
                self.epoch -= 1;
                if let Some(job) = self.jobs.iter_mut().find(|j| j.id == id) {
                    *job = old_job;
                }
                return Err(error);
            }
        }
        Ok(())
    }
    fn job_value(&self, job: &Job) -> Value {
        let mut value = serde_json::to_value(job).unwrap();
        let user_authorization = self.user_authorization(job);
        if let Some(receipt) = &user_authorization {
            value["userAuthorization"] = receipt.clone();
        }
        if job.requires_availability || self.availability_required {
            value["requiresAvailability"] = json!(true);
            value["inputAllowed"] = json!(
                job.state == "active"
                    && !self.blocked
                    && job.expires_at > now_ms()
                    && job.deadline_at.is_none_or(|at| at > now_ms())
                    && (self.availability()["available"] == true || user_authorization.is_some())
            );
        }
        value
    }
    pub(crate) fn command(&mut self, action: &str, params: &Value) -> Result<Value, RpcError> {
        if self.recovery_inspection_active && !matches!(action, "desktop.list" | "desktop.get") {
            return Err(error(
                "DESKTOP_RECOVERY_INSPECT_BUSY",
                "queue mutation is unavailable during recovery inspection",
            ));
        }
        // Status reads also advance an eligible queue; no client-side grant.
        self.promote()?;
        match action {
            "desktop.maintenance" => {
                let owner = field(params, "owner")?;
                let enabled = params["enabled"]
                    .as_bool()
                    .ok_or_else(|| error("INVALID_PARAMS", "enabled is required"))?;
                if self
                    .maintenance_owner
                    .as_deref()
                    .is_some_and(|current| current != owner)
                {
                    return Err(error(
                        "MAINTENANCE_OWNED",
                        "another operator owns desktop maintenance",
                    ));
                }
                self.maintenance_owner = enabled.then(|| owner.to_owned());
                self.save()?;
                self.promote()?;
                // A maintenance receipt describes current admission, not the audit history.
                // Historical jobs remain available through an explicit desktop.list read.
                self.command("desktop.list", &json!({"includeTerminal":false}))
            }
            "desktop.submit" => {
                let owner = field(params, "owner")?;
                let key = field(params, "requestKey")?;
                if let Some(job) = self
                    .jobs
                    .iter()
                    .find(|j| j.owner == owner && j.request_key == key)
                {
                    return Ok(self.job_value(job));
                }
                if self.jobs.len() >= 10000 {
                    return Err(error(
                        "DESKTOP_QUEUE_FULL",
                        "archive completed sessions before submitting more",
                    ));
                }
                let ttl = params["ttlMs"].as_u64().unwrap_or(900000);
                if !(1000..=3600000).contains(&ttl) {
                    return Err(error("INVALID_PARAMS", "ttlMs must be 1000..3600000"));
                }
                let requires_availability = match params.get("requiresAvailability") {
                    None => false,
                    Some(Value::Bool(value)) => *value,
                    _ => {
                        return Err(error(
                            "INVALID_PARAMS",
                            "requiresAvailability must be boolean",
                        ));
                    }
                };
                let deadline_at = match params.get("deadlineAt") {
                    None => None,
                    Some(value) => {
                        Some(value.as_u64().filter(|at| *at > now_ms()).ok_or_else(|| {
                            error(
                                "INVALID_PARAMS",
                                "deadlineAt must be a future epoch millisecond",
                            )
                        })?)
                    }
                };
                self.jobs.push(Job {
                    id: uuid::Uuid::new_v4().to_string(),
                    owner: owner.into(),
                    request_key: key.into(),
                    token: uuid::Uuid::new_v4().to_string(),
                    state: "queued".into(),
                    epoch: 0,
                    ttl_ms: ttl,
                    submitted_at: now_ms(),
                    expires_at: 0,
                    requires_availability: requires_availability || self.availability_required,
                    deadline_at,
                    claimed_by: None,
                });
                self.save()?;
                self.promote()?;
                Ok(self.job_value(self.jobs.last().unwrap()))
            }
            "desktop.list" => {
                let include_terminal = match params.get("includeTerminal") {
                    None => true,
                    Some(Value::Bool(value)) => *value,
                    Some(_) => {
                        return Err(error("INVALID_PARAMS", "includeTerminal must be a boolean"));
                    }
                };
                // Retain unknown states: hiding an unrecognized job could conceal
                // contention or recovery work. This filters presentation only.
                let terminal = |job: &Job| {
                    matches!(
                        job.state.as_str(),
                        "completed" | "cancelled" | "expired" | "interrupted"
                    )
                };
                let history_count = self.jobs.iter().filter(|job| terminal(job)).count();
                let jobs: Vec<_> = self
                    .jobs
                    .iter()
                    .filter(|job| include_terminal || !terminal(job))
                    .map(|j| {
                        let mut value = self.job_value(j);
                        value.as_object_mut().unwrap().remove("token");
                        value
                    })
                    .collect();
                Ok(
                    json!({"jobs":jobs,"historyCount":history_count,"totalCount":self.jobs.len(),"historyIncluded":include_terminal,"availabilityRequired":self.availability_required,"worker":{"notificationWakes":self.notification_wakes,"pollingWakes":self.polling_wakes},"availability":self.availability(),"blocked":self.blocked,"inFlight":self.in_flight,"lastRecovery":self.last_recovery,"recoveryPolicy":"verified-os-restart-or-explicit-desktop-reset","maintenanceOwner":self.maintenance_owner,"safePoint":self.maintenance_owner.is_some() && !self.blocked && !self.in_flight && self.active().is_none()}),
                )
            }
            "desktop.claim" => {
                let owner = field(params, "owner")?;
                let token = field(params, "token")?;
                let consumer = field(params, "consumerId")?;
                let available = self.availability()["available"] == true;
                let i = self
                    .jobs
                    .iter()
                    .position(|j| j.owner == owner && j.token == token)
                    .ok_or_else(|| {
                        error("DESKTOP_SESSION_INVALID", "unknown session credentials")
                    })?;
                let available = available || self.user_authorization(&self.jobs[i]).is_some();
                let job = &mut self.jobs[i];
                if self.blocked
                    || job.state != "active"
                    || job.expires_at <= now_ms()
                    || job.deadline_at.is_some_and(|at| at <= now_ms())
                {
                    return Err(error(
                        "DESKTOP_SESSION_INACTIVE",
                        "consumer requires a live active session",
                    ));
                }
                if (job.requires_availability || self.availability_required) && !available {
                    return Err(error(
                        "DESKTOP_UNAVAILABLE",
                        "local availability permission is absent or expired",
                    ));
                }
                if job
                    .claimed_by
                    .as_deref()
                    .is_some_and(|claimed| claimed != consumer)
                {
                    return Err(error(
                        "DESKTOP_ALREADY_CLAIMED",
                        "another consumer has claimed this request; no replay",
                    ));
                }
                let old = job.claimed_by.clone();
                job.claimed_by = Some(consumer.into());
                if let Err(error) = self.save() {
                    self.jobs[i].claimed_by = old;
                    return Err(error);
                }
                Ok(json!({"claimed":true,"id":self.jobs[i].id}))
            }
            "desktop.get" | "desktop.renew" | "desktop.finish" | "desktop.cancel" => {
                let token = field(params, "token")?;
                let owner = field(params, "owner")?;
                let i = self
                    .jobs
                    .iter()
                    .position(|j| j.token == token && j.owner == owner)
                    .ok_or_else(|| {
                        error("DESKTOP_SESSION_INVALID", "unknown session credentials")
                    })?;
                let job = &mut self.jobs[i];
                match action {
                    "desktop.renew" => {
                        if job.state != "active"
                            || job.expires_at <= now_ms()
                            || job.deadline_at.is_some_and(|at| at <= now_ms())
                            || self.blocked
                        {
                            return Err(error(
                                "DESKTOP_SESSION_INACTIVE",
                                "only a live active session can renew",
                            ));
                        }
                        job.expires_at = now_ms().saturating_add(job.ttl_ms);
                    }
                    "desktop.finish" | "desktop.cancel" => {
                        if job.state == "queued" {
                            job.state = "cancelled".into();
                        } else if job.state == "active" {
                            job.state = "draining".into();
                        }
                    }
                    _ => {}
                }
                let value = self.job_value(&self.jobs[i]);
                if action != "desktop.get" {
                    self.save()?;
                }
                Ok(value)
            }
            "desktop.recover" => {
                if !self.blocked || self.in_flight || params["confirmDesktopReset"] != true {
                    return Err(error(
                        "DESKTOP_RECOVERY_REQUIRED",
                        "reset native helpers/desktop first, then confirmDesktopReset",
                    ));
                }
                if let Some(i) = self.active() {
                    self.jobs[i].state = "interrupted".into();
                }
                self.blocked = false;
                self.save()?;
                self.promote()?;
                Ok(json!({"recovered":true}))
            }
            _ => Err(error("UNKNOWN_ACTION", action)),
        }
    }

    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn begin_recovery_inspection(
        &mut self,
        action: &str,
        params: &Value,
    ) -> Result<(), RpcError> {
        validate_recovery_inspection_params(action, params)?;
        if self.recovery_inspection_active {
            return Err(error(
                "DESKTOP_RECOVERY_INSPECT_BUSY",
                "another recovery inspection is active",
            ));
        }
        if !self.blocked || self.in_flight {
            return Err(error(
                "DESKTOP_RECOVERY_REQUIRED",
                "recovery inspection requires a blocked queue with no in-flight action",
            ));
        }
        self.recovery_inspection_active = true;
        Ok(())
    }

    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn end_recovery_inspection(&mut self) {
        self.recovery_inspection_active = false;
    }
    pub(crate) fn begin(&mut self, action: &str, params: &mut Value) -> Result<(), RpcError> {
        if self.blocked {
            return Err(error(
                "DESKTOP_RECOVERY_REQUIRED",
                "desktop outcome is uncertain; explicit reset required",
            ));
        }
        if self.in_flight {
            return Err(error("DESKTOP_BUSY", "desktop action is already in flight"));
        }
        self.refresh_availability_policy()?;
        if let Some(i) = self.active() {
            let job = &self.jobs[i];
            if job.state != "active"
                || job.expires_at <= now_ms()
                || job.deadline_at.is_some_and(|at| at <= now_ms())
            {
                return Err(error("DESKTOP_DRAINING", "previous session is draining"));
            }
            if params["_desktop"]["token"] != job.token || params["_desktop"]["owner"] != job.owner
            {
                return Err(error(
                    "DESKTOP_BUSY",
                    "desktop belongs to another acceptance session",
                ));
            }
            if (job.requires_availability || self.availability_required)
                && self.availability()["available"] != true
                && self.user_authorization(job).is_none()
            {
                return Err(error(
                    "DESKTOP_UNAVAILABLE",
                    "local availability permission is absent or expired; no desktop action dispatched",
                ));
            }
            if action == "computer-use.call" {
                params["sessionId"] = json!(format!("{}:{}", job.id, job.epoch));
            }
        } else if self.maintenance_owner.is_some() {
            return Err(error(
                "DESKTOP_MAINTENANCE",
                "desktop admission is paused for maintenance",
            ));
        } else if self.availability_required
            || action == "computer-use.call"
            || params.get("_desktop").is_some()
        {
            return Err(error(
                "DESKTOP_SESSION_REQUIRED",
                "submit and await a desktop session first",
            ));
        }
        self.in_flight = true;
        self.save()
    }
    pub(crate) fn end(&mut self, uncertain: bool) -> Result<(), RpcError> {
        self.in_flight = false;
        self.blocked |= uncertain;
        self.save()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn submit(q: &mut DesktopQueue, owner: &str) -> Value {
        q.command(
            "desktop.submit",
            &json!({"owner":owner,"requestKey":owner,"ttlMs":1000}),
        )
        .unwrap()
    }
    fn credentials(job: &Value) -> Value {
        json!({"owner":job["owner"],"token":job["token"]})
    }
    #[cfg(unix)]
    fn publish_user_authorization(q: &DesktopQueue, job: &Value) -> Value {
        use std::os::unix::fs::PermissionsExt;
        let now = now_ms();
        let value = json!({"schemaVersion":1,"source":"explicit-user",
            "authorizationId":uuid::Uuid::new_v4().to_string(),"jobId":job["id"],
            "owner":job["owner"],"requestKey":job["requestKey"],
            "tokenSha256":hex::encode(Sha256::digest(job["token"].as_str().unwrap().as_bytes())),
            "bootId":q.boot_id,"grantedAtMs":now,"expiresAtMs":now+60_000,"reason":"test operator consent"});
        let path = q
            .path
            .as_ref()
            .unwrap()
            .with_file_name("desktop-user-authorization.json");
        fs::write(&path, value.to_string()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        value
    }
    #[cfg(unix)]
    #[test]
    fn explicit_user_grant_is_scoped_revocable_and_preserves_waiting_jobs() {
        let dir = tempfile::tempdir().unwrap();
        publish_availability(dir.path(), false);
        let mut q = DesktopQueue::open(dir.path().join("queue.json")).unwrap();
        let waiting = submit(&mut q, "unattended");
        let manual = submit(&mut q, "authorized-now");
        publish_user_authorization(&q, &manual);
        let live = q.command("desktop.get", &credentials(&manual)).unwrap();
        assert_eq!(live["state"], "active");
        assert_eq!(live["inputAllowed"], true);
        assert_eq!(live["requiresAvailability"], true);
        assert_eq!(live["userAuthorization"]["source"], "explicit-user");
        assert_eq!(
            q.command("desktop.get", &credentials(&waiting)).unwrap()["state"],
            "queued"
        );
        assert_eq!(q.availability()["available"], false);
        assert_eq!(
            q.begin(
                "computer-use.call",
                &mut json!({"_desktop":credentials(&waiting)})
            )
            .unwrap_err()
            .code,
            "DESKTOP_BUSY"
        );
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&manual)}),
        )
        .unwrap();
        q.end(false).unwrap();
        fs::remove_file(dir.path().join("desktop-user-authorization.json")).unwrap();
        assert_eq!(
            q.command("desktop.get", &credentials(&manual)).unwrap()["inputAllowed"],
            false
        );
        assert_eq!(
            q.begin(
                "computer-use.call",
                &mut json!({"_desktop":credentials(&manual)})
            )
            .unwrap_err()
            .code,
            "DESKTOP_UNAVAILABLE"
        );
        assert!(!q.in_flight);
        q.command("desktop.finish", &credentials(&manual)).unwrap();
        q.cleanup_done(true).unwrap();
        publish_availability(dir.path(), true);
        assert_eq!(
            q.command("desktop.get", &credentials(&waiting)).unwrap()["state"],
            "active"
        );
    }
    #[cfg(unix)]
    #[test]
    fn user_grant_cannot_override_quarantine_maintenance_or_wrong_boot_and_token() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let dir = tempfile::tempdir().unwrap();
        publish_availability(dir.path(), false);
        let mut q = DesktopQueue::open(dir.path().join("queue.json")).unwrap();
        let manual = submit(&mut q, "authorized-now");
        let valid = publish_user_authorization(&q, &manual);
        let path = dir.path().join("desktop-user-authorization.json");
        for (key, value) in [
            ("jobId", json!("another-job")),
            ("owner", json!("another-owner")),
            ("requestKey", json!("another-generation")),
            ("bootId", json!("another-boot")),
            ("tokenSha256", json!("old-token")),
            ("source", json!("away-guard")),
            ("expiresAtMs", json!(now_ms())),
            ("expiresAtMs", json!(now_ms() + 900_001)),
            ("grantedAtMs", json!(now_ms() + 1000)),
            ("authorizationId", json!("")),
        ] {
            let mut invalid = valid.clone();
            invalid[key] = value;
            fs::write(&path, invalid.to_string()).unwrap();
            assert!(q.user_authorization(&q.jobs[0]).is_none(), "accepted {key}");
        }
        fs::write(&path, valid.to_string()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(q.user_authorization(&q.jobs[0]).is_none());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let target = dir.path().join("elsewhere.json");
        fs::rename(&path, &target).unwrap();
        symlink(&target, &path).unwrap();
        assert!(q.user_authorization(&q.jobs[0]).is_none());
        fs::remove_file(&path).unwrap();
        fs::rename(target, &path).unwrap();
        q.maintenance_owner = Some("operator".into());
        q.promote().unwrap();
        assert_eq!(q.jobs[0].state, "queued");
        q.maintenance_owner = None;
        q.blocked = true;
        q.promote().unwrap();
        assert_eq!(q.jobs[0].state, "queued");
        assert_eq!(
            q.begin(
                "computer-use.call",
                &mut json!({"_desktop":credentials(&manual)})
            )
            .unwrap_err()
            .code,
            "DESKTOP_RECOVERY_REQUIRED"
        );
    }
    fn publish_availability(dir: &std::path::Path, available: bool) {
        let now = now_ms();
        fs::write(
            dir.join("desktop-availability.json"),
            serde_json::to_vec(&json!({
                "schemaVersion":1,"provider":"away-guard","state":"armed",
                "generation":"fixture","sequence":1,"available":available,
                "observedAtMs":now,"expiresAtMs":now+10_000
            }))
            .unwrap(),
        )
        .unwrap();
    }
    #[test]
    fn ten_second_snapshot_lease_does_not_delay_revocation_or_accept_unbounded_grants() {
        let now = 100_000;
        let valid = json!({"schemaVersion":1,"provider":"away-guard","state":"armed",
            "generation":"fixture","sequence":1,"available":true,
            "observedAtMs":now,"expiresAtMs":now+10_000});
        for age in [0, 1501, 9999] {
            assert!(snapshot_allows_input(&valid, now + age));
        }
        assert!(!snapshot_allows_input(&valid, now + 10_000));
        assert!(!snapshot_allows_input(&valid, now - 1));
        for (key, value) in [
            ("available", json!(false)),
            ("provider", json!("caller")),
            ("state", json!("locked")),
            ("generation", json!("")),
            ("sequence", json!(-1)),
            ("observedAtMs", json!(now + 1)),
            ("expiresAtMs", json!(now)),
            ("expiresAtMs", json!(now + 10_001)),
            ("observedAtMs", json!(now - 5000)), // Total lifetime, not just remaining TTL.
            ("observedAtMs", json!("100000")),
            ("expiresAtMs", Value::Null),
        ] {
            let mut invalid = valid.clone();
            invalid[key] = value;
            assert!(!snapshot_allows_input(&invalid, now), "accepted {invalid}");
        }
        let mut legacy = valid;
        legacy["expiresAtMs"] = json!(now + 1500);
        assert!(snapshot_allows_input(&legacy, now)); // Reader-first rollout.
    }
    #[test]
    fn configured_worker_enforces_gate_for_legacy_callers_and_remembers_missing_provider() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");
        publish_availability(dir.path(), false);
        let mut q = DesktopQueue::open(path.clone()).unwrap();
        let a = q
            .command(
                "desktop.submit",
                &json!({"owner":"old","requestKey":"old","requiresAvailability":false}),
            )
            .unwrap();
        assert_eq!(a["state"], "queued");
        assert_eq!(a["requiresAvailability"], true);
        assert!(q.availability_required);
        assert_eq!(
            q.begin("application.__probe__", &mut json!({}))
                .unwrap_err()
                .code,
            "DESKTOP_SESSION_REQUIRED"
        );
        fs::remove_file(dir.path().join("desktop-availability.json")).unwrap();
        drop(q);
        let mut reopened = DesktopQueue::open(path).unwrap();
        assert!(reopened.availability_required);
        assert_eq!(
            reopened.command("desktop.get", &credentials(&a)).unwrap()["state"],
            "queued"
        );
        publish_availability(dir.path(), true);
        assert_eq!(
            reopened.command("desktop.get", &credentials(&a)).unwrap()["inputAllowed"],
            true
        );
    }
    #[test]
    fn one_request_cannot_be_consumed_by_two_waiters() {
        let mut q = DesktopQueue::default();
        let a = submit(&mut q, "a");
        let mut claim = credentials(&a);
        claim["consumerId"] = json!("first");
        q.command("desktop.claim", &claim).unwrap();
        q.command("desktop.claim", &claim).unwrap(); // Lost ack is safe.
        claim["consumerId"] = json!("second");
        assert_eq!(
            q.command("desktop.claim", &claim).unwrap_err().code,
            "DESKTOP_ALREADY_CLAIMED"
        );
    }
    #[test]
    fn failed_grant_persistence_rolls_back_the_in_memory_permission() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = DesktopQueue {
            maintenance_owner: Some("operator".into()),
            ..DesktopQueue::default()
        };
        let a = submit(&mut q, "a");
        q.maintenance_owner = None;
        let path = dir.path().join("not-a-file");
        fs::create_dir(&path).unwrap();
        q.path = Some(path);
        assert!(q.promote().is_err());
        assert_eq!(q.jobs[0].state, "queued");
        assert_eq!(q.epoch, 0);
        assert!(
            q.begin(
                "computer-use.call",
                &mut json!({"_desktop":credentials(&a)})
            )
            .is_err()
        );
    }
    #[test]
    fn unattended_queue_is_fifo_and_revokes_before_dispatch() {
        let dir = tempfile::tempdir().unwrap();
        let mut q = DesktopQueue::open(dir.path().join("queue.json")).unwrap();
        let a = q
            .command(
                "desktop.submit",
                &json!({"owner":"a","requestKey":"a","requiresAvailability":true}),
            )
            .unwrap();
        let b = submit(&mut q, "b");
        assert_eq!(a["state"], "queued");
        assert_eq!(b["state"], "queued"); // No bypass of a waiting head.
        let duplicate = q
            .command(
                "desktop.submit",
                &json!({"owner":"a","requestKey":"a","requiresAvailability":true}),
            )
            .unwrap();
        assert_eq!(duplicate["id"], a["id"]);
        publish_availability(dir.path(), true);
        assert_eq!(
            q.command("desktop.get", &credentials(&a)).unwrap()["state"],
            "active"
        );
        publish_availability(dir.path(), false);
        let mut input = json!({"_desktop":credentials(&a)});
        assert_eq!(
            q.begin("computer-use.call", &mut input).unwrap_err().code,
            "DESKTOP_UNAVAILABLE"
        );
        assert!(!q.in_flight);
        assert_eq!(q.jobs[1].state, "queued");
        q.command("desktop.finish", &credentials(&a)).unwrap();
        q.cleanup_done(true).unwrap();
        assert_eq!(q.jobs[1].state, "queued");
        publish_availability(dir.path(), true);
        q.promote().unwrap();
        assert_eq!(q.jobs[1].state, "active");
    }
    #[test]
    fn unattended_snapshots_fail_closed_and_queue_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");
        let mut q = DesktopQueue::open(path.clone()).unwrap();
        let a = q
            .command(
                "desktop.submit",
                &json!({"owner":"a","requestKey":"a","requiresAvailability":true}),
            )
            .unwrap();
        drop(q);
        let mut q = DesktopQueue::open(path).unwrap();
        for snapshot in [
            json!({}),
            json!({"schemaVersion":1,"available":true,"observedAtMs":0,"expiresAtMs":now_ms()+1000}),
            json!({"schemaVersion":1,"available":true,"observedAtMs":now_ms()+10000,"expiresAtMs":now_ms()+11000}),
        ] {
            fs::write(
                dir.path().join("desktop-availability.json"),
                snapshot.to_string(),
            )
            .unwrap();
            assert_eq!(
                q.command("desktop.get", &credentials(&a)).unwrap()["state"],
                "queued"
            );
        }
        q.jobs[0].deadline_at = Some(now_ms() - 1);
        q.promote().unwrap();
        assert_eq!(q.jobs[0].state, "expired");
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn kernel_boot_identity_is_a_stable_uuid() {
        let first = current_boot_id().expect("kernel boot identity must be readable");
        assert!(uuid::Uuid::parse_str(&first).is_ok());
        assert_eq!(current_boot_id().as_deref(), Some(first.as_str()));
    }

    #[test]
    fn verified_os_restart_retires_old_input_grant_without_replay() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queue.json");
        let mut queue =
            DesktopQueue::open_with_boot_id(path.clone(), Some("boot-a".into())).unwrap();
        let job = submit(&mut queue, "old");
        let mut input = json!({"_desktop":credentials(&job)});
        queue.begin("computer-use.call", &mut input).unwrap();
        // Simulate a process interrupted while native input was in flight.
        drop(queue);
        let mut reopened =
            DesktopQueue::open_with_boot_id(path.clone(), Some("boot-b".into())).unwrap();
        assert!(!reopened.blocked);
        assert!(!reopened.in_flight);
        assert_eq!(
            reopened.command("desktop.get", &credentials(&job)).unwrap()["state"],
            "interrupted"
        );
        assert_eq!(
            reopened.last_recovery.as_ref().unwrap()["reason"],
            "os_restart"
        );
        assert_eq!(
            reopened.last_recovery.as_ref().unwrap()["inputReplayed"],
            false
        );
        assert_eq!(
            reopened
                .begin("computer-use.call", &mut input)
                .unwrap_err()
                .code,
            "DESKTOP_SESSION_REQUIRED"
        );
        let fresh = submit(&mut reopened, "fresh");
        assert_eq!(fresh["state"], "active");
        assert_ne!(fresh["token"], job["token"]);
        assert!(fresh["epoch"].as_u64().unwrap() > job["epoch"].as_u64().unwrap());
        assert_eq!(
            reopened
                .begin("computer-use.call", &mut input)
                .unwrap_err()
                .code,
            "DESKTOP_BUSY"
        );
        // Recovery evidence and terminal old grants survive another restart.
        drop(reopened);
        let reopened = DesktopQueue::open_with_boot_id(path, Some("boot-b".into())).unwrap();
        assert!(reopened.blocked); // fresh active session interrupted in same boot
        assert_eq!(
            reopened.last_recovery.as_ref().unwrap()["interruptedSessions"],
            1
        );
    }

    #[test]
    fn known_read_failure_does_not_clear_prior_input_quarantine() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queue.json");
        let mut queue =
            DesktopQueue::open_with_boot_id(path.clone(), Some("boot-a".into())).unwrap();
        queue.begin("computer-use.tools", &mut json!({})).unwrap();
        queue.end(false).unwrap();
        assert!(!queue.blocked);
        queue.end(true).unwrap();
        queue.end(false).unwrap();
        assert!(queue.blocked);
        assert!(queue.begin("computer-use.tools", &mut json!({})).is_err());
        let reopened = DesktopQueue::open_with_boot_id(path, Some("boot-a".into())).unwrap();
        assert!(reopened.blocked);
    }

    #[test]
    fn same_boot_or_missing_evidence_never_unlocks_uncertain_input() {
        for (old, current) in [
            (Some("boot-a"), Some("boot-a")),
            (Some("boot-a"), None),
            (None, Some("boot-b")),
            (None, None),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("queue.json");
            let mut queue =
                DesktopQueue::open_with_boot_id(path.clone(), old.map(str::to_owned)).unwrap();
            queue.blocked = true;
            queue.save().unwrap();
            let reopened =
                DesktopQueue::open_with_boot_id(path, current.map(str::to_owned)).unwrap();
            assert!(reopened.blocked, "old={old:?}, current={current:?}");
            assert!(reopened.last_recovery.is_none());
        }
    }

    #[test]
    fn reboot_recovery_preserves_maintenance_and_does_not_promote_work() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queue.json");
        let mut queue =
            DesktopQueue::open_with_boot_id(path.clone(), Some("boot-a".into())).unwrap();
        queue
            .command(
                "desktop.maintenance",
                &json!({"owner":"installer","enabled":true}),
            )
            .unwrap();
        let job = submit(&mut queue, "waiting");
        queue.blocked = true;
        queue.save().unwrap();
        let mut reopened = DesktopQueue::open_with_boot_id(path, Some("boot-b".into())).unwrap();
        assert!(!reopened.blocked);
        assert_eq!(reopened.maintenance_owner.as_deref(), Some("installer"));
        reopened.promote().unwrap();
        assert_eq!(
            reopened.command("desktop.get", &credentials(&job)).unwrap()["state"],
            "queued"
        );
        let receipt = reopened
            .command("desktop.list", &json!({"includeTerminal":false}))
            .unwrap();
        assert_eq!(receipt["lastRecovery"]["reason"], "os_restart");
    }

    #[test]
    fn live_listing_keeps_contention_and_recovery_without_historical_payload() {
        let mut q = DesktopQueue::default();
        let first = submit(&mut q, "current");
        let prototype = q.jobs[0].clone();
        q.jobs.clear();
        for n in 0..1000 {
            let mut job = prototype.clone();
            job.id = format!("history-{n}");
            job.state = ["completed", "cancelled", "expired", "interrupted"][n % 4].into();
            q.jobs.push(job);
        }
        for state in ["active", "queued", "draining", "future-recovery-state"] {
            let mut job = prototype.clone();
            job.id = state.into();
            job.state = state.into();
            q.jobs.push(job);
        }
        q.blocked = true;
        q.in_flight = true;
        let before = serde_json::to_value(&q).unwrap();
        let live = q
            .command("desktop.list", &json!({"includeTerminal":false}))
            .unwrap();
        assert_eq!(live["historyCount"], 1000);
        assert_eq!(live["totalCount"], 1004);
        assert_eq!(live["historyIncluded"], false);
        assert_eq!(live["blocked"], true);
        assert_eq!(live["inFlight"], true);
        assert_eq!(live["safePoint"], false);
        let states: Vec<_> = live["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["state"].as_str().unwrap())
            .collect();
        assert_eq!(
            states,
            ["active", "queued", "draining", "future-recovery-state"]
        );
        assert!(!live.to_string().contains(first["token"].as_str().unwrap()));
        let legacy = q.command("desktop.list", &json!({})).unwrap();
        let history = q
            .command("desktop.list", &json!({"includeTerminal":true}))
            .unwrap();
        assert_eq!(legacy, history);
        assert_eq!(history["jobs"].as_array().unwrap().len(), 1004);
        assert!(live.to_string().len() * 50 < history.to_string().len());
        assert_eq!(serde_json::to_value(&q).unwrap(), before);
        for malformed in [Value::Null, json!("false"), json!(0)] {
            assert_eq!(
                q.command("desktop.list", &json!({"includeTerminal":malformed}))
                    .unwrap_err()
                    .code,
                "INVALID_PARAMS"
            );
        }
    }
    #[test]
    fn maintenance_receipt_omits_history_without_losing_fifo_or_audit() {
        let mut q = DesktopQueue::default();
        let first = submit(&mut q, "active");
        let prototype = q.jobs[0].clone();
        for n in 0..1000 {
            let mut job = prototype.clone();
            job.id = format!("history-{n}");
            job.state = "completed".into();
            q.jobs.push(job);
        }
        let waiting = submit(&mut q, "waiting");
        let enabled = q
            .command(
                "desktop.maintenance",
                &json!({"owner":"upgrade", "enabled":true}),
            )
            .unwrap();
        assert_eq!(enabled["historyIncluded"], false);
        assert_eq!(enabled["historyCount"], 1000);
        assert_eq!(enabled["totalCount"], 1002);
        assert_eq!(enabled["safePoint"], false);
        assert_eq!(enabled["jobs"].as_array().unwrap().len(), 2);
        assert_eq!(enabled["jobs"][0]["id"], first["id"]);
        assert_eq!(enabled["jobs"][1]["id"], waiting["id"]);
        assert!(
            !enabled
                .to_string()
                .contains(first["token"].as_str().unwrap())
        );
        let history = q
            .command("desktop.list", &json!({"includeTerminal":true}))
            .unwrap();
        assert_eq!(history["jobs"].as_array().unwrap().len(), 1002);
        assert!(enabled.to_string().len() * 50 < history.to_string().len());
        let disabled = q
            .command(
                "desktop.maintenance",
                &json!({"owner":"upgrade", "enabled":false}),
            )
            .unwrap();
        assert_eq!(disabled["maintenanceOwner"], Value::Null);
        assert_eq!(disabled["historyIncluded"], false);
        assert_eq!(disabled["jobs"][0]["state"], "active");
        assert_eq!(disabled["jobs"][1]["state"], "queued");
        assert_eq!(q.jobs.len(), 1002);
    }
    #[test]
    fn maintenance_drains_owner_and_preserves_fifo_across_restart() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");
        let mut q = DesktopQueue::open(path.clone()).unwrap();
        let a = submit(&mut q, "a");
        let b = submit(&mut q, "b");
        let status = q
            .command(
                "desktop.maintenance",
                &json!({"owner":"upgrade", "enabled":true}),
            )
            .unwrap();
        assert_eq!(status["safePoint"], false);
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&a)}),
        )
        .unwrap();
        q.end(false).unwrap();
        q.command("desktop.finish", &credentials(&a)).unwrap();
        q.cleanup_done(true).unwrap();
        assert_eq!(
            q.command("desktop.list", &json!({})).unwrap()["safePoint"],
            true
        );
        assert_eq!(
            q.command("desktop.get", &credentials(&b)).unwrap()["state"],
            "queued"
        );
        drop(q);
        let mut q = DesktopQueue::open(path).unwrap();
        assert!(q.begin("clipboard.write", &mut json!({})).is_err());
        assert!(
            q.command(
                "desktop.maintenance",
                &json!({"owner":"other", "enabled":false})
            )
            .is_err()
        );
        q.command(
            "desktop.maintenance",
            &json!({"owner":"upgrade", "enabled":false}),
        )
        .unwrap();
        assert_eq!(
            q.command("desktop.get", &credentials(&b)).unwrap()["state"],
            "active"
        );
    }
    #[test]
    fn fifo_dedup_cancel_and_stale_credentials() {
        let mut q = DesktopQueue::default();
        let a = submit(&mut q, "a");
        let b = submit(&mut q, "b");
        let c = submit(&mut q, "c");
        assert_eq!(a["state"], "active");
        assert_eq!(b["state"], "queued");
        assert_eq!(submit(&mut q, "b"), b);
        assert_eq!(
            q.begin(
                "computer-use.call",
                &mut json!({"_desktop":credentials(&b)})
            )
            .unwrap_err()
            .code,
            "DESKTOP_BUSY"
        );
        q.command("desktop.cancel", &credentials(&b)).unwrap();
        q.command("desktop.finish", &credentials(&a)).unwrap();
        assert!(q.needs_cleanup());
        q.cleanup_done(true).unwrap();
        assert_eq!(
            q.command("desktop.get", &credentials(&c)).unwrap()["state"],
            "active"
        );
        assert!(
            q.begin(
                "computer-use.call",
                &mut json!({"_desktop":credentials(&a)})
            )
            .is_err()
        );
        assert_eq!(q.jobs[2].epoch, 2);
        assert!(
            !q.command("desktop.list", &json!({}))
                .unwrap()
                .to_string()
                .contains(a["token"].as_str().unwrap())
        );
    }
    #[test]
    fn expiration_waits_for_in_flight_action_and_cleanup() {
        let mut q = DesktopQueue::default();
        let a = submit(&mut q, "a");
        submit(&mut q, "b");
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&a)}),
        )
        .unwrap();
        q.jobs[0].expires_at = 0;
        assert!(!q.needs_cleanup());
        q.promote().unwrap();
        assert_eq!(q.jobs[1].state, "queued");
        assert!(q.command("desktop.renew", &credentials(&a)).is_err());
        q.end(false).unwrap();
        assert!(q.needs_cleanup());
        q.cleanup_done(true).unwrap();
        assert_eq!(q.jobs[0].state, "expired");
        assert_eq!(q.jobs[1].state, "active");
    }
    #[test]
    fn quarantined_close_cleans_helpers_without_recovering_or_promoting() {
        let mut q = DesktopQueue::default();
        let a = submit(&mut q, "a");
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&a)}),
        )
        .unwrap();
        q.end(true).unwrap();
        submit(&mut q, "b");
        // An uncertain active operation is not implicitly closed on observation.
        assert!(!q.needs_cleanup());
        q.command("desktop.finish", &credentials(&a)).unwrap();
        assert!(q.needs_cleanup());
        q.cleanup_done(true).unwrap();
        assert!(q.blocked);
        assert_eq!(q.jobs[0].state, "interrupted");
        assert_eq!(q.jobs[1].state, "queued");
        assert!(q.begin("ui.input", &mut json!({})).is_err());
        assert!(q.command("desktop.recover", &json!({})).is_err());
        q.command("desktop.recover", &json!({"confirmDesktopReset":true}))
            .unwrap();
        assert_eq!(q.jobs[1].state, "active");
    }

    #[test]
    fn quarantined_cleanup_waits_for_inflight_and_retains_failed_cleanup() {
        let mut q = DesktopQueue::default();
        let a = submit(&mut q, "a");
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&a)}),
        )
        .unwrap();
        q.command("desktop.finish", &credentials(&a)).unwrap();
        assert!(!q.needs_cleanup());
        q.end(true).unwrap();
        assert!(q.needs_cleanup());
        q.cleanup_done(false).unwrap();
        assert!(q.blocked);
        assert_eq!(q.jobs[0].state, "draining");
    }

    #[test]
    fn restart_quarantines_active_session_and_preserves_fifo() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("queue.json");
        let mut q = DesktopQueue::open(path.clone()).unwrap();
        let a = submit(&mut q, "a");
        submit(&mut q, "b");
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&a)}),
        )
        .unwrap();
        drop(q);
        let mut q = DesktopQueue::open(path).unwrap();
        assert!(q.blocked);
        q.promote().unwrap();
        assert_eq!(q.jobs[1].state, "queued");
        assert!(q.command("desktop.recover", &json!({})).is_err());
        q.command("desktop.recover", &json!({"confirmDesktopReset":true}))
            .unwrap();
        assert_eq!(q.jobs[0].state, "interrupted");
        assert_eq!(q.jobs[1].state, "active");
    }
    #[test]
    fn legacy_calls_share_gate_and_ambiguous_actions_block_successors() {
        let mut q = DesktopQueue::default();
        q.begin("application.launch", &mut json!({})).unwrap();
        let a = submit(&mut q, "a");
        assert_eq!(a["state"], "queued");
        q.end(false).unwrap();
        q.promote().unwrap();
        for action in [
            "ui.input",
            "ui.evaluate",
            "application.launch",
            "clipboard.write",
        ] {
            assert!(protected(action));
            assert!(q.begin(action, &mut json!({})).is_err());
        }
        q.begin(
            "computer-use.call",
            &mut json!({"_desktop":credentials(&a)}),
        )
        .unwrap();
        q.end(true).unwrap();
        submit(&mut q, "b");
        q.promote().unwrap();
        assert!(q.blocked);
        assert_eq!(q.jobs[1].state, "queued");
        assert!(
            q.begin("ui.input", &mut json!({"_desktop":credentials(&a)}))
                .is_err()
        );
    }
    #[test]
    fn cleanup_failure_blocks_and_independent_desktops_progress() {
        let mut q = DesktopQueue::default();
        let a = submit(&mut q, "a");
        submit(&mut q, "b");
        q.command("desktop.finish", &credentials(&a)).unwrap();
        q.cleanup_done(false).unwrap();
        assert!(q.blocked);
        assert_eq!(q.jobs[1].state, "queued");
        assert_eq!(submit(&mut DesktopQueue::default(), "c")["state"], "active");
    }

    #[test]
    fn recovery_inspection_is_strict_read_only_and_preserves_blocked_state() {
        let mut q = DesktopQueue {
            epoch: 7,
            jobs: vec![Job {
                id: "job-1".into(),
                owner: "owner-1".into(),
                request_key: "key-1".into(),
                token: "secret-token".into(),
                state: "queued".into(),
                epoch: 0,
                ttl_ms: 1000,
                submitted_at: 10,
                expires_at: 0,
                requires_availability: false,
                deadline_at: None,
                claimed_by: None,
            }],
            blocked: true,
            ..DesktopQueue::default()
        };
        let params = json!({
            "applicationPath": "/Applications/Example.app",
            "requestPermission": false,
            "recoveryInspection": true
        });
        let before = serde_json::to_value(&q).unwrap();
        q.begin_recovery_inspection("ui.native-inspect", &params)
            .unwrap();
        assert_eq!(
            q.command("desktop.list", &json!({})).unwrap()["blocked"],
            true
        );
        assert_eq!(
            q.command(
                "desktop.get",
                &json!({"owner":"owner-1", "token":"secret-token"})
            )
            .unwrap()["state"],
            "queued"
        );
        for action in [
            "desktop.submit",
            "desktop.maintenance",
            "desktop.renew",
            "desktop.finish",
            "desktop.cancel",
            "desktop.recover",
        ] {
            assert_eq!(
                q.command(action, &json!({})).unwrap_err().code,
                "DESKTOP_RECOVERY_INSPECT_BUSY",
                "{action} must not mutate during recovery inspection"
            );
        }
        for action in [
            "ui.inspect",
            "ui.evaluate",
            "application.launch",
            "clipboard.write",
        ] {
            assert_eq!(
                q.begin(action, &mut json!({})).unwrap_err().code,
                "DESKTOP_RECOVERY_REQUIRED",
                "ordinary protected action {action} must remain blocked"
            );
        }
        assert_eq!(
            q.begin_recovery_inspection("ui.native-inspect", &params)
                .unwrap_err()
                .code,
            "DESKTOP_RECOVERY_INSPECT_BUSY"
        );
        assert_eq!(serde_json::to_value(&q).unwrap(), before);
        q.end_recovery_inspection();
        assert_eq!(serde_json::to_value(&q).unwrap(), before);
    }

    #[test]
    fn recovery_inspection_rejects_unblocked_inflight_and_injected_fields() {
        let valid = json!({
            "applicationPath": "/Applications/Example.app",
            "requestPermission": false,
            "recoveryInspection": true
        });
        for (blocked, in_flight, expected) in [
            (false, false, "DESKTOP_RECOVERY_REQUIRED"),
            (true, true, "DESKTOP_RECOVERY_REQUIRED"),
        ] {
            let mut q = DesktopQueue {
                blocked,
                in_flight,
                ..DesktopQueue::default()
            };
            assert_eq!(
                q.begin_recovery_inspection("ui.native-inspect", &valid)
                    .unwrap_err()
                    .code,
                expected
            );
            assert!(!q.recovery_inspection_active);
        }
        for invalid in [
            json!({"applicationPath":"/Applications/Example.app", "requestPermission":true, "recoveryInspection":true}),
            json!({"applicationPath":"/Applications/Example.app", "requestPermission":false, "recoveryInspection":true, "expression":"1+1"}),
            json!({"applicationPath":"/Applications/Example.app", "requestPermission":false, "recoveryInspection":true, "_authority": []}),
            json!({"applicationPath":"/Applications/Example.app", "requestPermission":false, "recoveryInspection":true, "_workspaceSessionId":"session"}),
        ] {
            assert_eq!(
                validate_recovery_inspection_params("ui.native-inspect", &invalid)
                    .unwrap_err()
                    .code,
                "RECOVERY_INSPECT_PARAMS_INVALID"
            );
        }
        assert_eq!(
            validate_recovery_inspection_params("ui.evaluate", &valid)
                .unwrap_err()
                .code,
            "RECOVERY_INSPECT_ACTION_INVALID"
        );
    }
}
