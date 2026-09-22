//! One durable FIFO per Executor interactive desktop. Controllers are submitters.
use machine_fabric_core::{atomic_replace, now_ms};
use machine_fabric_protocol::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{fs, io::Write, path::PathBuf};

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
    #[serde(default)]
    maintenance_owner: Option<String>,
    // An in-memory barrier for the narrow read-only inspection allowed while
    // blocked. It is deliberately not persisted; the durable blocked state is.
    #[serde(skip)]
    recovery_inspection_active: bool,
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
impl DesktopQueue {
    pub(crate) fn open(path: PathBuf) -> Result<Self, RpcError> {
        let mut queue: Self = if path.exists() {
            serde_json::from_slice(&fs::read(&path).map_err(|e| error("DESKTOP_STATE_FAILED", e))?)
                .map_err(|e| error("DESKTOP_STATE_FAILED", e))?
        } else {
            Self::default()
        };
        queue.path = Some(path);
        // Native helpers may outlive a crashed process. Fail closed until an operator
        // confirms the desktop has been reset; never turn a crash into a new grant.
        if queue.in_flight
            || queue
                .jobs
                .iter()
                .any(|j| j.state == "active" || j.state == "draining")
        {
            queue.blocked = true;
            queue.in_flight = false;
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
        if self.blocked || self.in_flight {
            return false;
        }
        if let Some(i) = self.active() {
            if self.jobs[i].expires_at <= now_ms() {
                self.jobs[i].state = "draining".into();
            }
            return self.jobs[i].state == "draining";
        }
        false
    }
    pub(crate) fn cleanup_done(&mut self, success: bool) -> Result<(), RpcError> {
        if success {
            if let Some(i) = self.active() {
                self.jobs[i].state = if self.jobs[i].expires_at <= now_ms() {
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
    pub(crate) fn promote(&mut self) -> Result<(), RpcError> {
        if self.maintenance_owner.is_none()
            && !self.blocked
            && !self.in_flight
            && self.active().is_none()
            && let Some(job) = self.jobs.iter_mut().find(|j| j.state == "queued")
        {
            self.epoch += 1;
            job.epoch = self.epoch;
            job.state = "active".into();
            job.expires_at = now_ms().saturating_add(job.ttl_ms);
            self.save()?;
        }
        Ok(())
    }
    pub(crate) fn command(&mut self, action: &str, params: &Value) -> Result<Value, RpcError> {
        if self.recovery_inspection_active && !matches!(action, "desktop.list" | "desktop.get") {
            return Err(error(
                "DESKTOP_RECOVERY_INSPECT_BUSY",
                "queue mutation is unavailable during recovery inspection",
            ));
        }
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
                self.command("desktop.list", &json!({}))
            }
            "desktop.submit" => {
                let owner = field(params, "owner")?;
                let key = field(params, "requestKey")?;
                if let Some(job) = self
                    .jobs
                    .iter()
                    .find(|j| j.owner == owner && j.request_key == key)
                {
                    return Ok(serde_json::to_value(job).unwrap());
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
                });
                self.save()?;
                self.promote()?;
                Ok(serde_json::to_value(self.jobs.last().unwrap()).unwrap())
            }
            "desktop.list" => {
                let jobs: Vec<_> = self
                    .jobs
                    .iter()
                    .map(|j| {
                        let mut value = serde_json::to_value(j).unwrap();
                        value.as_object_mut().unwrap().remove("token");
                        value
                    })
                    .collect();
                Ok(
                    json!({"jobs":jobs,"blocked":self.blocked,"inFlight":self.in_flight,"maintenanceOwner":self.maintenance_owner,"safePoint":self.maintenance_owner.is_some() && !self.blocked && !self.in_flight && self.active().is_none()}),
                )
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
                        if job.state != "active" || job.expires_at <= now_ms() || self.blocked {
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
                let value = serde_json::to_value(&self.jobs[i]).unwrap();
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
        if let Some(i) = self.active() {
            let job = &self.jobs[i];
            if job.state != "active" || job.expires_at <= now_ms() {
                return Err(error("DESKTOP_DRAINING", "previous session is draining"));
            }
            if params["_desktop"]["token"] != job.token || params["_desktop"]["owner"] != job.owner
            {
                return Err(error(
                    "DESKTOP_BUSY",
                    "desktop belongs to another acceptance session",
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
        } else if action == "computer-use.call" || params.get("_desktop").is_some() {
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
