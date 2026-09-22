use machine_fabric_core::{atomic_replace, now_ms};
use machine_fabric_protocol::RpcError;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::Duration;

pub const OBSERVATION_TTL_MS: u64 = 30_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessRecord {
    pub id: String,
    pub pid: u32,
    #[serde(default)]
    pub identity_verified: bool,
    #[serde(default)]
    pub observed_at: Option<u64>,
    pub cwd: PathBuf,
    pub argv: Vec<String>,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub restartable: bool,
    #[serde(default)]
    pub metadata: Value,
    pub log_path: PathBuf,
    pub state: ProcessState,
    pub readiness: ReadinessStatus,
    #[serde(default)]
    readiness_spec: Option<Value>,
    #[serde(default)]
    readiness_deadline_at: Option<u64>,
    pub started_at: u64,
    pub updated_at: u64,
    #[serde(default)]
    pub last_successful_probe_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ProcessState {
    Running,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ReadinessState {
    Pending,
    Ready,
    Failed,
    NotConfigured,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ReadinessStatus {
    pub state: ReadinessState,
    pub attempts: u64,
}

#[derive(Debug, Default)]
pub struct ProcessTable {
    records: Mutex<BTreeMap<String, ProcessRecord>>,
    state_path: Option<PathBuf>,
}

impl ProcessTable {
    pub fn open(state_path: PathBuf) -> Result<Self, RpcError> {
        let records = if state_path.exists() {
            serde_json::from_slice(
                &fs::read(&state_path)
                    .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))?,
            )
            .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))?
        } else {
            BTreeMap::new()
        };
        let table = Self {
            records: Mutex::new(records),
            state_path: Some(state_path),
        };
        table.refresh_all()?;
        Ok(table)
    }

    // Keep the established process launch contract; these are independent RPC fields.
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        &self,
        id: String,
        cwd: PathBuf,
        argv: Vec<String>,
        env: BTreeMap<String, String>,
        log_path: PathBuf,
        readiness: Option<Value>,
        restartable: bool,
        metadata: Value,
    ) -> Result<ProcessRecord, RpcError> {
        if argv.is_empty() {
            return Err(RpcError::new("INVALID_PARAMS", "argv must not be empty"));
        }
        if self
            .records
            .lock()
            .expect("process lock")
            .get(&id)
            .is_some_and(|record| record.state == ProcessState::Running)
        {
            return Err(RpcError::new(
                "PROCESS_ALREADY_RUNNING",
                format!("process {id} is already running"),
            ));
        }
        if let Some(parent) = log_path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| RpcError::new("LOG_CREATE_FAILED", error.to_string()))?;
        }
        let stdout = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|error| RpcError::new("LOG_CREATE_FAILED", error.to_string()))?;
        let stderr = stdout
            .try_clone()
            .map_err(|error| RpcError::new("LOG_CREATE_FAILED", error.to_string()))?;
        let mut command = Command::new(&argv[0]);
        command
            .args(&argv[1..])
            .current_dir(&cwd)
            .envs(env.clone())
            .stdin(Stdio::null())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .map_err(|error| RpcError::new("PROCESS_START_FAILED", error.to_string()))?;
        let now = now_ms();
        let readiness_deadline_at = readiness.as_ref().map(|spec| {
            now.saturating_add(
                spec.get("timeoutMs")
                    .and_then(Value::as_u64)
                    .unwrap_or(180_000),
            )
        });
        let record = ProcessRecord {
            id: id.clone(),
            pid: child.id(),
            identity_verified: true,
            observed_at: Some(now),
            cwd,
            argv,
            env,
            restartable,
            metadata,
            log_path,
            state: ProcessState::Running,
            readiness: ReadinessStatus {
                state: if readiness.is_some() {
                    ReadinessState::Pending
                } else {
                    ReadinessState::NotConfigured
                },
                attempts: 0,
            },
            readiness_spec: readiness,
            readiness_deadline_at,
            started_at: now,
            updated_at: now,
            last_successful_probe_at: None,
        };
        // Keep a lightweight reaper so short-lived managed processes do not
        // remain zombies and readiness/identity refresh can observe exit.
        std::thread::spawn(move || {
            let _ = child.wait();
        });
        let mut records = self.records.lock().expect("process lock");
        records.insert(id, record.clone());
        self.persist(&records)?;
        Ok(record)
    }

    pub fn get(&self, id: &str) -> Result<ProcessRecord, RpcError> {
        let mut records = self.records.lock().expect("process lock");
        let record = records
            .get_mut(id)
            .ok_or_else(|| RpcError::new("PROCESS_NOT_FOUND", format!("unknown process: {id}")))?;
        refresh(record);
        let result = record.clone();
        self.persist(&records)?;
        Ok(result)
    }

    pub fn list(&self) -> Vec<ProcessRecord> {
        let mut records = self.records.lock().expect("process lock");
        for record in records.values_mut() {
            refresh(record);
        }
        let _ = self.persist(&records);
        records.values().cloned().collect()
    }

    pub fn stop(&self, id: &str) -> Result<ProcessRecord, RpcError> {
        let mut records = self.records.lock().expect("process lock");
        let record = records
            .get_mut(id)
            .ok_or_else(|| RpcError::new("PROCESS_NOT_FOUND", format!("unknown process: {id}")))?;
        if record.state == ProcessState::Running {
            stop_process(record.pid)?;
            record.state = ProcessState::Stopped;
            record.updated_at = now_ms();
        }
        let result = record.clone();
        self.persist(&records)?;
        Ok(result)
    }

    pub fn update_metadata(&self, id: &str, metadata: Value) -> Result<ProcessRecord, RpcError> {
        let mut records = self.records.lock().expect("process lock");
        let record = records
            .get_mut(id)
            .ok_or_else(|| RpcError::new("PROCESS_NOT_FOUND", format!("unknown process: {id}")))?;
        record.metadata = metadata;
        record.updated_at = now_ms();
        let result = record.clone();
        self.persist(&records)?;
        Ok(result)
    }

    pub fn restart(&self, id: &str) -> Result<ProcessRecord, RpcError> {
        let record = self.get(id)?;
        if !record.restartable || record.argv.is_empty() {
            return Err(RpcError::new(
                "PROCESS_NOT_RESTARTABLE",
                format!("process {id} is not restartable"),
            ));
        }
        let _ = self.stop(id);
        self.start(
            id.to_owned(),
            record.cwd,
            record.argv,
            record.env,
            record.log_path,
            record.readiness_spec,
            record.restartable,
            record.metadata,
        )
    }

    pub fn wait_ready(&self, id: &str, timeout_ms: u64) -> Result<ProcessRecord, RpcError> {
        let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms.max(1));
        loop {
            let record = self.get(id)?;
            if record.readiness.state == ReadinessState::Ready {
                return Ok(record);
            }
            if record.state != ProcessState::Running {
                return Err(RpcError::new(
                    "PROCESS_EXITED",
                    format!("process {id} exited"),
                ));
            }
            if record.readiness.state == ReadinessState::Failed {
                return Err(RpcError::new(
                    "READINESS_MARKER_MISSING",
                    format!("process {id} readiness failed"),
                ));
            }
            if std::time::Instant::now() >= deadline {
                return Err(RpcError::new(
                    "READINESS_TIMEOUT",
                    format!("process {id} readiness timed out"),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn logs(&self, id: &str, tail: usize) -> Result<Value, RpcError> {
        let record = self.get(id)?;
        let content = fs::read_to_string(&record.log_path)
            .map_err(|error| RpcError::new("LOG_READ_FAILED", error.to_string()))?;
        let lines: Vec<&str> = content.lines().collect();
        let start = lines.len().saturating_sub(tail);
        Ok(json!({
            "processId": id,
            "path": record.log_path,
            "lines": &lines[start..],
        }))
    }

    fn refresh_all(&self) -> Result<(), RpcError> {
        let mut records = self.records.lock().expect("process lock");
        for record in records.values_mut() {
            refresh(record);
        }
        self.persist(&records)
    }

    fn persist(&self, records: &BTreeMap<String, ProcessRecord>) -> Result<(), RpcError> {
        let Some(path) = &self.state_path else {
            return Ok(());
        };
        let bytes = serde_json::to_vec_pretty(records)
            .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))?;
        }
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let temporary =
            path.with_file_name(format!(".{name}.{}.tmp", uuid::Uuid::new_v4().simple()));
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))?;
        atomic_replace(&temporary, path)
            .map_err(|error| RpcError::new("PROCESS_STATE_FAILED", error.to_string()))
    }
}

fn refresh(record: &mut ProcessRecord) {
    if record.state != ProcessState::Running {
        return;
    }
    if !process_is_alive(record.pid) {
        record.state = ProcessState::Stopped;
        record.updated_at = now_ms();
        return;
    }
    if record.readiness.state == ReadinessState::Pending {
        record.readiness.attempts = record.readiness.attempts.saturating_add(1);
        let ready = record
            .readiness_spec
            .as_ref()
            .map(|spec| readiness_once(spec, &record.log_path))
            .transpose();
        match ready {
            Ok(Some(true)) => {
                record.readiness.state = ReadinessState::Ready;
                record.last_successful_probe_at = Some(now_ms());
            }
            Ok(Some(false))
                if record
                    .readiness_deadline_at
                    .is_some_and(|deadline| now_ms() >= deadline) =>
            {
                record.readiness.state = ReadinessState::Failed;
            }
            Err(_) => record.readiness.state = ReadinessState::Failed,
            _ => {}
        }
        record.updated_at = now_ms();
    }
}

#[cfg(unix)]
fn stop_process(pid: u32) -> Result<(), RpcError> {
    // Processes started by this table lead their own process group. Signal the
    // group so package managers and shells cannot leave product servers or
    // download helpers behind after a restart.
    let result = unsafe { libc::kill(-(pid as i32), libc::SIGTERM) };
    if result == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(RpcError::new("PROCESS_STOP_FAILED", error.to_string()))
    }
}

#[cfg(windows)]
fn stop_process(pid: u32) -> Result<(), RpcError> {
    let status = Command::new("taskkill")
        .args(["/PID", &pid.to_string(), "/T", "/F"])
        .status()
        .map_err(|error| RpcError::new("PROCESS_STOP_FAILED", error.to_string()))?;
    if status.success() || !process_is_alive(pid) {
        Ok(())
    } else {
        Err(RpcError::new(
            "PROCESS_STOP_FAILED",
            format!("taskkill exited with {status}"),
        ))
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let result = unsafe { libc::kill(pid as i32, 0) };
    result == 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(windows)]
fn process_is_alive(pid: u32) -> bool {
    Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .map(|output| {
            output.status.success()
                && String::from_utf8_lossy(&output.stdout).contains(&format!("\"{pid}\""))
        })
        .unwrap_or(true)
}

fn readiness_once(spec: &Value, log_path: &Path) -> Result<bool, RpcError> {
    match spec.get("type").and_then(Value::as_str).unwrap_or("") {
        "all" => {
            let probes = spec
                .get("probes")
                .and_then(Value::as_array)
                .ok_or_else(|| RpcError::new("INVALID_READINESS", "all probes are required"))?;
            if probes.is_empty() {
                return Err(RpcError::new(
                    "INVALID_READINESS",
                    "all requires at least one probe",
                ));
            }
            for probe in probes {
                if !readiness_once(probe, log_path)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        "tcp" => {
            let host = spec
                .get("host")
                .and_then(Value::as_str)
                .unwrap_or("127.0.0.1");
            let port = spec
                .get("port")
                .and_then(Value::as_u64)
                .ok_or_else(|| RpcError::new("INVALID_READINESS", "tcp port is required"))?;
            let address = format!("{host}:{port}");
            Ok(address
                .to_socket_addrs()
                .map_err(|error| RpcError::new("INVALID_READINESS", error.to_string()))?
                .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok()))
        }
        "file" => Ok(spec
            .get("path")
            .and_then(Value::as_str)
            .is_some_and(|path| Path::new(path).exists())),
        "log" => {
            let pattern = spec
                .get("pattern")
                .and_then(Value::as_str)
                .ok_or_else(|| RpcError::new("INVALID_READINESS", "log pattern is required"))?;
            Ok(fs::read_to_string(log_path)
                .map(|content| {
                    if let Some(exact) = pattern
                        .strip_prefix('^')
                        .and_then(|value| value.strip_suffix('$'))
                    {
                        content.lines().any(|line| line == exact)
                    } else {
                        content.contains(pattern)
                    }
                })
                .unwrap_or(false))
        }
        other => Err(RpcError::new(
            "INVALID_READINESS",
            format!("unsupported readiness type: {other}"),
        )),
    }
}

#[cfg(any(target_os = "macos", windows, test))]
pub(crate) fn application_environment(
    value: Option<&Value>,
) -> Result<BTreeMap<String, String>, RpcError> {
    let env: BTreeMap<String, String> = match value {
        None | Some(Value::Null) => BTreeMap::new(),
        Some(value) => serde_json::from_value(value.clone())
            .map_err(|_| RpcError::new("INVALID_PARAMS", "env must contain string values"))?,
    };
    if env
        .iter()
        .any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
    {
        return Err(RpcError::new(
            "INVALID_PARAMS",
            "invalid environment name or value",
        ));
    }
    Ok(env)
}

#[cfg(test)]
mod tests {
    use super::readiness_once;
    #[cfg(unix)]
    use super::{ProcessTable, ReadinessState};
    #[cfg(unix)]
    use serde_json::Value;
    use serde_json::json;
    use std::fs;
    #[cfg(unix)]
    use std::time::Duration;

    #[test]
    fn all_readiness_requires_every_probe() {
        let log_path = std::env::temp_dir().join(format!(
            "fabric-readiness-{}-{}.log",
            std::process::id(),
            machine_fabric_core::now_ms()
        ));
        fs::write(&log_path, "compiler done\n").expect("write log");
        let ready = readiness_once(
            &json!({
                "type": "all",
                "probes": [
                    {"type": "file", "path": log_path},
                    {"type": "log", "pattern": "compiler done"}
                ]
            }),
            &log_path,
        )
        .expect("valid readiness");
        assert!(ready);
        let _ = fs::remove_file(log_path);
    }

    #[cfg(unix)]
    #[test]
    fn process_start_returns_pending_and_get_advances_readiness() {
        let root = std::env::temp_dir().join(format!(
            "fabric-process-{}-{}",
            std::process::id(),
            machine_fabric_core::now_ms()
        ));
        fs::create_dir_all(&root).expect("create root");
        let log_path = root.join("process.log");
        let table = ProcessTable::default();
        let started = std::time::Instant::now();
        let record = table
            .start(
                "readiness-test".to_owned(),
                root.clone(),
                vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    "sleep 0.2; echo ready; sleep 1".to_owned(),
                ],
                Default::default(),
                log_path,
                Some(json!({"type": "log", "pattern": "^ready$", "timeoutMs": 2_000})),
                false,
                Value::Null,
            )
            .expect("start process");
        assert!(started.elapsed() < Duration::from_millis(150));
        assert_eq!(record.readiness.state, ReadinessState::Pending);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let record = table.get("readiness-test").expect("get process");
            if record.readiness.state == ReadinessState::Ready {
                assert!(record.readiness.attempts > 0);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "process did not become ready"
            );
            std::thread::sleep(Duration::from_millis(25));
        }
        table.stop("readiness-test").expect("stop process");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn process_records_survive_table_restart() {
        let root = std::env::temp_dir().join(format!(
            "fabric-process-state-{}-{}",
            std::process::id(),
            machine_fabric_core::now_ms()
        ));
        fs::create_dir_all(&root).expect("create root");
        let state_path = root.join("processes.json");
        let table = ProcessTable::open(state_path.clone()).expect("open table");
        table
            .start(
                "persistent-process".to_owned(),
                root.clone(),
                vec!["sleep".to_owned(), "30".to_owned()],
                Default::default(),
                root.join("process.log"),
                None,
                false,
                Value::Null,
            )
            .expect("start process");
        drop(table);
        let restored = ProcessTable::open(state_path).expect("restore table");
        let record = restored.get("persistent-process").expect("restored record");
        assert_eq!(record.state, super::ProcessState::Running);
        assert!(record.identity_verified);
        assert!(record.metadata.is_null());
        restored.stop("persistent-process").expect("stop process");
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn process_stop_terminates_descendants() {
        let root = std::env::temp_dir().join(format!(
            "fabric-process-group-{}-{}",
            std::process::id(),
            machine_fabric_core::now_ms()
        ));
        fs::create_dir_all(&root).expect("create root");
        let child_pid_path = root.join("child.pid");
        let table = ProcessTable::default();
        table
            .start(
                "process-group-test".to_owned(),
                root.clone(),
                vec![
                    "sh".to_owned(),
                    "-c".to_owned(),
                    format!("sleep 30 & echo $! > {}; wait", child_pid_path.display()),
                ],
                Default::default(),
                root.join("process.log"),
                None,
                false,
                Value::Null,
            )
            .expect("start process");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !child_pid_path.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "child pid was not recorded"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let child_pid: u32 = fs::read_to_string(&child_pid_path)
            .expect("read child pid")
            .trim()
            .parse()
            .expect("parse child pid");
        table.stop("process-group-test").expect("stop process");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while super::process_is_alive(child_pid) {
            assert!(
                std::time::Instant::now() < deadline,
                "descendant survived process stop"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = fs::remove_dir_all(root);
    }
}

#[cfg(any(windows, test))]
pub(crate) fn merge_windows_environment(
    inherited: &[u16],
    overrides: &BTreeMap<String, String>,
) -> Vec<u16> {
    let mut entries: BTreeMap<String, Vec<u16>> = BTreeMap::new();
    for entry in inherited
        .split(|unit| *unit == 0)
        .filter(|entry| !entry.is_empty())
    {
        // Windows also includes special entries such as =C:=C:/path.
        let end = entry
            .iter()
            .enumerate()
            .skip(1)
            .find(|(_, unit)| **unit == b'=' as u16)
            .map(|(index, _)| index)
            .unwrap_or(entry.len());
        entries.insert(
            String::from_utf16_lossy(&entry[..end]).to_uppercase(),
            entry.to_vec(),
        );
    }
    for (key, value) in overrides {
        entries.insert(
            key.to_uppercase(),
            format!("{key}={value}").encode_utf16().collect(),
        );
    }
    let mut result: Vec<u16> = entries
        .into_values()
        .flat_map(|entry| entry.into_iter().chain([0]))
        .collect();
    if result.is_empty() {
        result.push(0);
    }
    result.push(0);
    result
}

#[cfg(test)]
mod application_environment_tests {
    use super::*;
    #[test]
    fn rejects_invalid_overrides_and_preserves_windows_environment() {
        assert!(application_environment(Some(&json!({"BAD=KEY":"x"}))).is_err());
        assert!(application_environment(Some(&json!({"KEY":42}))).is_err());
        assert!(application_environment(Some(&json!({"KEY":"x\u{0}y"}))).is_err());
        let env =
            application_environment(Some(&json!({"path":"new", "UPDATE_DISABLED":"1"}))).unwrap();
        let inherited: Vec<u16> = "Path=old\0USER=用户\0=C:=drive\0\0"
            .encode_utf16()
            .collect();
        let merged = merge_windows_environment(&inherited, &env);
        let text = String::from_utf16(&merged).unwrap();
        assert!(text.contains("path=new\0"));
        assert!(!text.contains("old"));
        assert!(text.contains("USER=用户\0"));
        assert!(text.contains("=C:=drive\0"));
        assert!(text.ends_with("\0\0"));
        assert_eq!(merge_windows_environment(&[], &BTreeMap::new()), vec![0, 0]);
    }
}
