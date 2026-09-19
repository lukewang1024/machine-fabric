use anyhow::{Context, Result, anyhow};
use machine_fabric_core::atomic_replace;
use machine_fabric_protocol::{Request, Response, RpcError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{RpcServer, call_unix, log_event};

const PEER_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const PEER_BACKOFF_MAX: Duration = Duration::from_secs(5);
const PEER_STABLE_CONNECTION: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct PeerConnectConfig {
    pub local_id: String,
    pub peer_id: String,
    pub host: String,
    pub local_controller_socket: PathBuf,
    pub local_executor_socket: PathBuf,
    pub expose_controller_socket: PathBuf,
    pub expose_executor_socket: PathBuf,
    pub remote_executable: String,
    pub remote_state_root: String,
    pub remote_windows: bool,
    pub state_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PeerAcceptConfig {
    pub peer_id: String,
    pub local_id: String,
    pub local_controller_socket: PathBuf,
    pub local_executor_socket: PathBuf,
    pub expose_controller_socket: PathBuf,
    pub expose_executor_socket: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PeerStatus {
    pub peer_id: String,
    pub host: String,
    pub connection_id: String,
    pub generation: u64,
    pub state: String,
    pub updated_at: u64,
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum TargetRole {
    Controller,
    Executor,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
enum PeerFrame {
    Hello {
        protocol: String,
        node_id: String,
        roles: Vec<TargetRole>,
    },
    HelloAck {
        protocol: String,
        node_id: String,
        roles: Vec<TargetRole>,
    },
    Request {
        id: String,
        target_role: TargetRole,
        request: Request,
    },
    Response {
        id: String,
        response: Response,
    },
}

type SharedWriter = Arc<Mutex<Box<dyn Write + Send>>>;
type Pending = Arc<Mutex<HashMap<String, mpsc::Sender<Response>>>>;

#[derive(Clone)]
struct PeerBridge {
    writer: SharedWriter,
    pending: Pending,
}

impl PeerBridge {
    fn call(&self, target_role: TargetRole, request: Request) -> Response {
        let id = format!("peer_request_{}", Uuid::new_v4().simple());
        log_event(
            "info",
            "peer.request.started",
            serde_json::json!({"peerRequestId": id.clone(), "requestId": request.request_id.clone(), "correlationId": request.correlation_id.clone(), "targetRole": target_role}),
        );
        let (sender, receiver) = mpsc::channel();
        self.pending
            .lock()
            .expect("peer pending lock")
            .insert(id.clone(), sender);
        let frame = PeerFrame::Request {
            id: id.clone(),
            target_role,
            request,
        };
        if let Err(error) = write_frame(&self.writer, &frame) {
            self.pending.lock().expect("peer pending lock").remove(&id);
            return Response::failure(
                "peer",
                RpcError::new("PEER_WRITE_FAILED", error.to_string()),
            );
        }
        receiver
            .recv_timeout(Duration::from_secs(3600))
            .unwrap_or_else(|error| {
                self.pending.lock().expect("peer pending lock").remove(&id);
                Response::failure(
                    "peer",
                    RpcError::new("PEER_RESPONSE_TIMEOUT", error.to_string()),
                )
            })
    }
}

pub fn connect_peer(config: PeerConnectConfig) -> Result<()> {
    validate_id("local id", &config.local_id)?;
    validate_id("peer id", &config.peer_id)?;
    validate_id("host", &config.host)?;
    validate_absolute_paths(&[
        &config.local_controller_socket,
        &config.local_executor_socket,
        &config.expose_controller_socket,
        &config.expose_executor_socket,
        &config.state_path,
    ])?;
    validate_remote_root(&config.remote_state_root)?;
    validate_remote_root(&config.remote_executable)?;
    let controller_status = call_unix(
        &config.local_controller_socket,
        &Request::new("ping", serde_json::Value::Null),
    )?;
    let actual_local_id = controller_status
        .result
        .as_ref()
        .and_then(|result| result.pointer("/controller/id"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("local Controller did not report its node identity"))?;
    if actual_local_id != config.local_id {
        return Err(anyhow!(
            "local Controller identity is {actual_local_id}, configured peer identity is {}",
            config.local_id
        ));
    }
    if let Some(parent) = config.state_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let connection_id = format!("connection_{}", Uuid::new_v4().simple());
    let mut generation = read_peer_status(&config.state_path)
        .map(|status| status.generation.saturating_add(1))
        .unwrap_or(1);
    let mut backoff = PEER_BACKOFF_INITIAL;
    loop {
        log_event(
            "info",
            "peer.connecting",
            serde_json::json!({"peerId": config.peer_id, "host": config.host, "connectionId": connection_id, "generation": generation}),
        );
        write_status(
            &config.state_path,
            &config.peer_id,
            &config.host,
            &connection_id,
            generation,
            "connecting",
            None,
        )?;
        let started = Instant::now();
        match connect_once(&config, &connection_id, generation) {
            Ok(()) => write_status(
                &config.state_path,
                &config.peer_id,
                &config.host,
                &connection_id,
                generation,
                "reconnecting",
                Some("peer transport closed".to_owned()),
            )?,
            Err(error) => {
                log_event(
                    "error",
                    "peer.disconnected",
                    serde_json::json!({"peerId": config.peer_id, "connectionId": connection_id, "generation": generation, "error": error.to_string()}),
                );
                write_status(
                    &config.state_path,
                    &config.peer_id,
                    &config.host,
                    &connection_id,
                    generation,
                    "reconnecting",
                    Some(error.to_string()),
                )?
            }
        }
        backoff = next_peer_backoff(backoff, started.elapsed());
        thread::sleep(backoff);
        generation = generation.saturating_add(1);
    }
}

pub fn accept_peer(config: PeerAcceptConfig) -> Result<()> {
    validate_id("peer id", &config.peer_id)?;
    validate_id("local id", &config.local_id)?;
    validate_absolute_paths(&[
        &config.local_controller_socket,
        &config.local_executor_socket,
        &config.expose_controller_socket,
        &config.expose_executor_socket,
    ])?;
    let controller_status = call_unix(
        &config.local_controller_socket,
        &Request::new("ping", serde_json::Value::Null),
    )?;
    let actual_local_id = controller_status
        .result
        .as_ref()
        .and_then(|result| result.pointer("/controller/id"))
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| anyhow!("local Controller did not report its node identity"))?;
    if actual_local_id != config.local_id {
        return Err(anyhow!(
            "local Controller identity is {actual_local_id}, expected {}",
            config.local_id
        ));
    }
    let mut input = BufReader::new(std::io::stdin());
    let mut output = std::io::stdout();
    accept_handshake(&mut input, &mut output, &config.peer_id, actual_local_id)?;
    let (_bridge, reader) = start_bridge(
        Box::new(input),
        Box::new(output),
        config.local_controller_socket,
        config.local_executor_socket,
        config.expose_controller_socket,
        config.expose_executor_socket,
    )?;
    reader
        .join()
        .map_err(|_| anyhow!("peer reader thread panicked"))?
}

pub fn read_peer_status(path: impl AsRef<Path>) -> Result<PeerStatus> {
    serde_json::from_slice(&fs::read(path.as_ref())?).context("decode peer status")
}

fn connect_once(config: &PeerConnectConfig, connection_id: &str, generation: u64) -> Result<()> {
    remove_socket(&config.expose_controller_socket)?;
    remove_socket(&config.expose_executor_socket)?;
    let separator = if config.remote_windows { "\\" } else { "/" };
    let remote_controller = format!(
        "{}{}fabric{}{}-controller.sock",
        config.remote_state_root, separator, separator, config.local_id
    );
    let remote_executor = format!(
        "{}{}fabric{}{}-executor.sock",
        config.remote_state_root, separator, separator, config.local_id
    );
    let remote_local_controller =
        format!("{}{}controller.sock", config.remote_state_root, separator);
    let remote_local_executor = format!("{}{}executor.sock", config.remote_state_root, separator);
    let remote_command = if config.remote_windows {
        format!(
            "powershell.exe -NoProfile -NonInteractive -Command \"& '{}' peer accept --id '{}' --local-id '{}' --local-controller-socket '{}' --local-executor-socket '{}' --expose-controller-socket '{}' --expose-executor-socket '{}'\"",
            powershell_single_quote(&config.remote_executable),
            powershell_single_quote(&config.local_id),
            powershell_single_quote(&config.peer_id),
            powershell_single_quote(&remote_local_controller),
            powershell_single_quote(&remote_local_executor),
            powershell_single_quote(&remote_controller),
            powershell_single_quote(&remote_executor),
        )
    } else {
        format!(
            "'{}' peer accept --id '{}' --local-id '{}' --local-controller-socket '{}' --local-executor-socket '{}' --expose-controller-socket '{}' --expose-executor-socket '{}'",
            shell_single_quote(&config.remote_executable),
            shell_single_quote(&config.local_id),
            shell_single_quote(&config.peer_id),
            shell_single_quote(&remote_local_controller),
            shell_single_quote(&remote_local_executor),
            shell_single_quote(&remote_controller),
            shell_single_quote(&remote_executor),
        )
    };
    let mut child = Command::new("ssh")
        .args([
            "-T",
            "-o",
            "BatchMode=yes",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "HostKeyAlgorithms=rsa-sha2-512,rsa-sha2-256,ecdsa-sha2-nistp256,ssh-ed25519",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=2",
            "-o",
            "TCPKeepAlive=yes",
            "-o",
            "ConnectTimeout=10",
            &config.host,
            &remote_command,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .with_context(|| format!("start SSH peer transport to {}", config.host))?;
    let stdout = child.stdout.take().expect("SSH stdout is piped");
    let mut stdout = BufReader::new(stdout);
    if let Err(error) = initiate_handshake(
        &mut stdout,
        &mut child.stdin.as_mut().expect("SSH stdin is piped"),
        &config.local_id,
        &config.peer_id,
    ) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error);
    }
    let stdin = child.stdin.take().expect("SSH stdin is piped");
    let (bridge, reader) = start_bridge(
        Box::new(stdout),
        Box::new(stdin),
        config.local_controller_socket.clone(),
        config.local_executor_socket.clone(),
        config.expose_controller_socket.clone(),
        config.expose_executor_socket.clone(),
    )?;
    for role in [TargetRole::Controller, TargetRole::Executor] {
        let response = bridge.call(role, Request::new("ping", serde_json::Value::Null));
        if !response.ok {
            let _ = child.kill();
            return Err(anyhow!("remote {role:?} did not become ready"));
        }
    }
    write_status(
        &config.state_path,
        &config.peer_id,
        &config.host,
        connection_id,
        generation,
        "ready",
        None,
    )?;
    let reader_result = reader
        .join()
        .map_err(|_| anyhow!("peer reader thread panicked"))?;
    let _ = child.kill();
    let _ = child.wait();
    reader_result
}

fn start_bridge(
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
    expose_controller_socket: PathBuf,
    expose_executor_socket: PathBuf,
) -> Result<(PeerBridge, thread::JoinHandle<Result<()>>)> {
    remove_socket(&expose_controller_socket)?;
    remove_socket(&expose_executor_socket)?;
    let bridge = PeerBridge {
        writer: Arc::new(Mutex::new(writer)),
        pending: Arc::new(Mutex::new(HashMap::new())),
    };
    let cleanup_controller_socket = expose_controller_socket.clone();
    let cleanup_executor_socket = expose_executor_socket.clone();
    for (socket, role) in [
        (expose_controller_socket, TargetRole::Controller),
        (expose_executor_socket, TargetRole::Executor),
    ] {
        let handler_bridge = bridge.clone();
        thread::spawn(move || {
            if let Err(error) =
                RpcServer::new(socket).serve(move |request| handler_bridge.call(role, request))
            {
                eprintln!("peer role listener failed: {error:#}");
            }
        });
    }
    let reader_bridge = bridge.clone();
    let handle = thread::spawn(move || {
        read_frames(
            reader,
            reader_bridge,
            local_controller_socket,
            local_executor_socket,
            cleanup_controller_socket,
            cleanup_executor_socket,
        )
    });
    Ok((bridge, handle))
}

fn read_frames(
    reader: Box<dyn Read + Send>,
    bridge: PeerBridge,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
    expose_controller_socket: PathBuf,
    expose_executor_socket: PathBuf,
) -> Result<()> {
    let result = read_frames_inner(
        reader,
        bridge.clone(),
        local_controller_socket,
        local_executor_socket,
    );
    let pending = std::mem::take(&mut *bridge.pending.lock().expect("peer pending lock"));
    for sender in pending.into_values() {
        let _ = sender.send(Response::failure(
            "peer",
            RpcError::new("PEER_DISCONNECTED", "peer framed connection closed"),
        ));
    }
    for socket in [&expose_controller_socket, &expose_executor_socket] {
        if let Err(error) = remove_socket(socket) {
            eprintln!("remove disconnected peer socket failed: {error:#}");
        }
    }
    result
}

fn next_peer_backoff(current: Duration, connected_for: Duration) -> Duration {
    if connected_for >= PEER_STABLE_CONNECTION {
        PEER_BACKOFF_INITIAL
    } else {
        (current * 2).min(PEER_BACKOFF_MAX)
    }
}

fn read_frames_inner(
    reader: Box<dyn Read + Send>,
    bridge: PeerBridge,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
) -> Result<()> {
    for line in BufReader::new(reader).lines() {
        let line = line.context("read peer frame")?;
        let frame: PeerFrame = serde_json::from_str(&line).context("decode peer frame")?;
        match frame {
            PeerFrame::Hello { .. } | PeerFrame::HelloAck { .. } => {
                return Err(anyhow!(
                    "unexpected handshake frame after peer became ready"
                ));
            }
            PeerFrame::Response { id, response } => {
                if let Some(sender) = bridge
                    .pending
                    .lock()
                    .expect("peer pending lock")
                    .remove(&id)
                {
                    let _ = sender.send(response);
                }
            }
            PeerFrame::Request {
                id,
                target_role,
                request,
            } => {
                let writer = Arc::clone(&bridge.writer);
                let socket = match target_role {
                    TargetRole::Controller => local_controller_socket.clone(),
                    TargetRole::Executor => local_executor_socket.clone(),
                };
                thread::spawn(move || {
                    let response = call_unix(socket, &request).unwrap_or_else(|error| {
                        Response::failure(
                            request.request_id,
                            RpcError::new("LOCAL_ROLE_UNAVAILABLE", error.to_string()),
                        )
                    });
                    if let Err(error) = write_frame(&writer, &PeerFrame::Response { id, response })
                    {
                        eprintln!("write peer response failed: {error:#}");
                    }
                });
            }
        }
    }
    Err(anyhow!("peer closed the framed connection"))
}

const PEER_PROTOCOL: &str = "machine-fabric.peer/v1";

fn initiate_handshake<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut W,
    local_id: &str,
    expected_peer_id: &str,
) -> Result<()> {
    serde_json::to_writer(
        &mut *writer,
        &PeerFrame::Hello {
            protocol: PEER_PROTOCOL.to_owned(),
            node_id: local_id.to_owned(),
            roles: vec![TargetRole::Controller, TargetRole::Executor],
        },
    )?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    let frame = read_handshake_frame(reader)?;
    match frame {
        PeerFrame::HelloAck {
            protocol,
            node_id,
            roles,
        } if protocol == PEER_PROTOCOL
            && node_id == expected_peer_id
            && has_required_roles(&roles) =>
        {
            Ok(())
        }
        other => Err(anyhow!("invalid peer handshake acknowledgement: {other:?}")),
    }
}

fn accept_handshake<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut W,
    expected_peer_id: &str,
    local_id: &str,
) -> Result<()> {
    let frame = read_handshake_frame(reader)?;
    match frame {
        PeerFrame::Hello {
            protocol,
            node_id,
            roles,
        } if protocol == PEER_PROTOCOL
            && node_id == expected_peer_id
            && has_required_roles(&roles) => {}
        other => return Err(anyhow!("invalid peer handshake: {other:?}")),
    }
    serde_json::to_writer(
        &mut *writer,
        &PeerFrame::HelloAck {
            protocol: PEER_PROTOCOL.to_owned(),
            node_id: local_id.to_owned(),
            roles: vec![TargetRole::Controller, TargetRole::Executor],
        },
    )?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn read_handshake_frame(reader: &mut impl BufRead) -> Result<PeerFrame> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    if line.is_empty() {
        return Err(anyhow!("peer closed before handshake"));
    }
    serde_json::from_str(&line).context("decode peer handshake")
}

fn has_required_roles(roles: &[TargetRole]) -> bool {
    roles
        .iter()
        .any(|role| matches!(role, TargetRole::Controller))
        && roles
            .iter()
            .any(|role| matches!(role, TargetRole::Executor))
}

fn write_frame(writer: &SharedWriter, frame: &PeerFrame) -> Result<()> {
    let mut writer = writer.lock().expect("peer writer lock");
    serde_json::to_writer(&mut *writer, frame)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(unix)]
fn remove_socket(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("remove stale socket {}", path.display()))?;
    }
    Ok(())
}

#[cfg(windows)]
fn remove_socket(_path: &Path) -> Result<()> {
    Ok(())
}

fn validate_id(name: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
    {
        return Err(anyhow!("invalid {name}: {value}"));
    }
    Ok(())
}

fn validate_absolute_paths(paths: &[&PathBuf]) -> Result<()> {
    for path in paths {
        if !path.is_absolute() {
            return Err(anyhow!("peer path must be absolute: {}", path.display()));
        }
    }
    Ok(())
}

fn validate_remote_root(path: &str) -> Result<()> {
    if path.is_empty() || path.contains('\n') || path.contains('\r') || path.contains('\'') {
        return Err(anyhow!("invalid remote state root: {path}"));
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    value.replace('\'', "'\\''")
}

fn powershell_single_quote(value: &str) -> String {
    value.replace('\'', "''")
}

fn write_status(
    path: &Path,
    peer_id: &str,
    host: &str,
    connection_id: &str,
    generation: u64,
    state: &str,
    error: Option<String>,
) -> Result<()> {
    let value = PeerStatus {
        peer_id: peer_id.to_owned(),
        host: host.to_owned(),
        connection_id: connection_id.to_owned(),
        generation,
        state: state.to_owned(),
        updated_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
        error,
    };
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&value)?)?;
    atomic_replace(&temporary, path)?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::os::unix::net::UnixStream;

    #[test]
    fn one_framed_connection_routes_both_roles_in_both_directions() {
        let directory = tempfile::tempdir().unwrap();
        let controller_a = directory.path().join("controller-a.sock");
        let executor_a = directory.path().join("executor-a.sock");
        let controller_b = directory.path().join("controller-b.sock");
        let executor_b = directory.path().join("executor-b.sock");
        for (socket, role) in [
            (controller_a.clone(), "controller-a"),
            (executor_a.clone(), "executor-a"),
            (controller_b.clone(), "controller-b"),
            (executor_b.clone(), "executor-b"),
        ] {
            thread::spawn(move || {
                RpcServer::new(socket)
                    .serve(move |request| {
                        Response::success(request.request_id, serde_json::json!({"role": role}))
                    })
                    .unwrap();
            });
        }
        let (a_to_b, b_to_a) = UnixStream::pair().unwrap();
        let a_read = a_to_b.try_clone().unwrap();
        let b_read = b_to_a.try_clone().unwrap();
        let (bridge_a, _reader_a) = start_bridge(
            Box::new(a_read),
            Box::new(a_to_b),
            controller_a,
            executor_a,
            directory.path().join("a-sees-b-controller.sock"),
            directory.path().join("a-sees-b-executor.sock"),
        )
        .unwrap();
        let (bridge_b, _reader_b) = start_bridge(
            Box::new(b_read),
            Box::new(b_to_a),
            controller_b,
            executor_b,
            directory.path().join("b-sees-a-controller.sock"),
            directory.path().join("b-sees-a-executor.sock"),
        )
        .unwrap();
        thread::sleep(Duration::from_millis(50));
        let a_calls_b = bridge_a.call(
            TargetRole::Executor,
            Request::new("status", serde_json::Value::Null),
        );
        let b_calls_a = bridge_b.call(
            TargetRole::Controller,
            Request::new("status", serde_json::Value::Null),
        );
        assert_eq!(a_calls_b.result.unwrap()["role"], "executor-b");
        assert_eq!(b_calls_a.result.unwrap()["role"], "controller-a");
    }

    #[test]
    fn handshake_rejects_wrong_node_identity() {
        let hello = serde_json::to_vec(&PeerFrame::HelloAck {
            protocol: PEER_PROTOCOL.to_owned(),
            node_id: "unexpected".to_owned(),
            roles: vec![TargetRole::Controller, TargetRole::Executor],
        })
        .unwrap();
        let mut input = Cursor::new([hello, b"\n".to_vec()].concat());
        let mut output = Vec::new();
        let error = initiate_handshake(&mut input, &mut output, "laptop", "devbox").unwrap_err();
        assert!(error.to_string().contains("invalid peer handshake"));
    }

    #[test]
    fn peer_retry_backoff_recovers_quickly_after_wake() {
        assert_eq!(
            next_peer_backoff(Duration::from_secs(1), Duration::from_secs(1)),
            Duration::from_secs(2)
        );
        assert_eq!(
            next_peer_backoff(Duration::from_secs(4), Duration::from_secs(1)),
            Duration::from_secs(5)
        );
        assert_eq!(
            next_peer_backoff(Duration::from_secs(5), Duration::from_secs(30)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn disconnected_bridge_removes_exposed_role_sockets() {
        let directory = tempfile::tempdir().unwrap();
        let controller = directory.path().join("controller.sock");
        let executor = directory.path().join("executor.sock");
        let exposed_controller = directory.path().join("peer-controller.sock");
        let exposed_executor = directory.path().join("peer-executor.sock");
        let (local, remote) = UnixStream::pair().unwrap();
        let reader = local.try_clone().unwrap();
        let (_bridge, reader_thread) = start_bridge(
            Box::new(reader),
            Box::new(local),
            controller,
            executor,
            exposed_controller.clone(),
            exposed_executor.clone(),
        )
        .unwrap();
        for _ in 0..100 {
            if exposed_controller.exists() && exposed_executor.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(exposed_controller.exists());
        assert!(exposed_executor.exists());
        drop(remote);
        assert!(reader_thread.join().unwrap().is_err());
        assert!(!exposed_controller.exists());
        assert!(!exposed_executor.exists());
    }
}
