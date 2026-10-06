use anyhow::{Context, Result, anyhow};
use base64::{Engine, engine::general_purpose::STANDARD};
use flate2::{Compression, read::GzDecoder, write::GzEncoder};
use machine_fabric_core::atomic_replace;
use machine_fabric_protocol::{Request, Response, RpcError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{RpcServer, call_unix, log_event};

const PEER_BACKOFF_INITIAL: Duration = Duration::from_secs(1);
const PEER_BACKOFF_MAX: Duration = Duration::from_secs(5);
const PEER_STABLE_CONNECTION: Duration = Duration::from_secs(30);

fn peer_response_timeout(action: &str) -> Duration {
    match action {
        "ping"
        | "status"
        | "availability"
        | "capability.list"
        | "capability.describe"
        | "desktop.list"
        | "desktop.get"
        | "process.get"
        | "process.list" => Duration::from_secs(10),
        _ => Duration::from_secs(3600),
    }
}

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
        #[serde(default)]
        response_gzip: bool,
    },
    HelloAck {
        protocol: String,
        node_id: String,
        roles: Vec<TargetRole>,
        #[serde(default)]
        response_gzip: bool,
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
    ResponseGzip {
        id: String,
        expanded_bytes: usize,
        payload: String,
    },
}

type SharedWriter = Arc<Mutex<Box<dyn Write + Send>>>;
type Pending = Arc<Mutex<HashMap<String, mpsc::Sender<Response>>>>;

#[derive(Clone)]
struct PeerBridge {
    writer: SharedWriter,
    pending: Pending,
    connected: Arc<AtomicBool>,
    response_gzip: bool,
}

impl PeerBridge {
    fn call(&self, target_role: TargetRole, request: Request) -> Response {
        let timeout = peer_response_timeout(&request.action);
        self.call_with_timeout(target_role, request, timeout)
    }

    fn call_with_timeout(
        &self,
        target_role: TargetRole,
        request: Request,
        timeout: Duration,
    ) -> Response {
        let request_id = request.request_id.clone();
        let id = format!("peer_request_{}", Uuid::new_v4().simple());
        log_event(
            "info",
            "peer.request.started",
            serde_json::json!({"peerRequestId": id.clone(), "requestId": request.request_id.clone(), "correlationId": request.correlation_id.clone(), "targetRole": target_role}),
        );
        let (sender, receiver) = mpsc::channel();
        {
            let mut pending = self.pending.lock().expect("peer pending lock");
            if !self.connected.load(Ordering::Acquire) {
                return Response::failure(
                    request_id,
                    RpcError::new("PEER_DISCONNECTED", "peer framed connection closed"),
                );
            }
            pending.insert(id.clone(), sender);
        }
        let frame = PeerFrame::Request {
            id: id.clone(),
            target_role,
            request,
        };
        if let Err(error) = write_frame(&self.writer, &frame) {
            self.pending.lock().expect("peer pending lock").remove(&id);
            return Response::failure(
                request_id,
                RpcError::new("PEER_WRITE_FAILED", error.to_string()),
            );
        }
        receiver.recv_timeout(timeout).unwrap_or_else(|error| {
            self.pending.lock().expect("peer pending lock").remove(&id);
            Response::failure(
                request_id,
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
    let response_gzip =
        accept_handshake(&mut input, &mut output, &config.peer_id, actual_local_id)?;
    let (_bridge, reader) = start_bridge_with_compression(
        Box::new(input),
        Box::new(output),
        config.local_controller_socket,
        config.local_executor_socket,
        config.expose_controller_socket,
        config.expose_executor_socket,
        response_gzip,
    )?;
    reader
        .join()
        .map_err(|_| anyhow!("peer reader thread panicked"))?
}

pub fn read_peer_status(path: impl AsRef<Path>) -> Result<PeerStatus> {
    serde_json::from_slice(&fs::read(path.as_ref())?).context("decode peer status")
}

fn connect_once(config: &PeerConnectConfig, connection_id: &str, generation: u64) -> Result<()> {
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
    let response_gzip = match initiate_handshake(
        &mut stdout,
        &mut child.stdin.as_mut().expect("SSH stdin is piped"),
        &config.local_id,
        &config.peer_id,
    ) {
        Ok(enabled) => enabled,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let stdin = child.stdin.take().expect("SSH stdin is piped");
    let (bridge, reader) = match start_bridge_with_compression(
        Box::new(stdout),
        Box::new(stdin),
        config.local_controller_socket.clone(),
        config.local_executor_socket.clone(),
        config.expose_controller_socket.clone(),
        config.expose_executor_socket.clone(),
        response_gzip,
    ) {
        Ok(bridge) => bridge,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let ready = (|| -> Result<()> {
        for role in [TargetRole::Controller, TargetRole::Executor] {
            let response = bridge.call(role, Request::new("ping", serde_json::Value::Null));
            if !response.ok {
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
        )
    })();
    if ready.is_err() {
        // Drive framed EOF so the reader retires both listeners before retry.
        let _ = child.kill();
        let _ = child.wait();
    }
    let reader_result = reader
        .join()
        .map_err(|_| anyhow!("peer reader thread panicked"));
    let _ = child.kill();
    let _ = child.wait();
    ready?;
    reader_result?
}

#[cfg(all(test, unix))]
fn start_bridge(
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
    expose_controller_socket: PathBuf,
    expose_executor_socket: PathBuf,
) -> Result<(PeerBridge, thread::JoinHandle<Result<()>>)> {
    start_bridge_with_compression(
        reader,
        writer,
        local_controller_socket,
        local_executor_socket,
        expose_controller_socket,
        expose_executor_socket,
        false,
    )
}

fn start_bridge_with_compression(
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
    expose_controller_socket: PathBuf,
    expose_executor_socket: PathBuf,
    response_gzip: bool,
) -> Result<(PeerBridge, thread::JoinHandle<Result<()>>)> {
    let bridge = PeerBridge {
        writer: Arc::new(Mutex::new(writer)),
        pending: Arc::new(Mutex::new(HashMap::new())),
        connected: Arc::new(AtomicBool::new(true)),
        response_gzip,
    };
    // Acquire both endpoints before any background listener starts. A partial
    // bind failure drops the first endpoint and cannot announce a healthy peer.
    let controller = RpcServer::new(expose_controller_socket).bind()?;
    let executor = RpcServer::new(expose_executor_socket).bind()?;
    let stop = Arc::new(AtomicBool::new(false));
    let mut listeners = Vec::new();
    for (server, role) in [
        (controller, TargetRole::Controller),
        (executor, TargetRole::Executor),
    ] {
        let handler_bridge = bridge.clone();
        let listener_stop = Arc::clone(&stop);
        listeners.push(thread::spawn(move || {
            server.serve(
                move |request| handler_bridge.call(role, request),
                Some(listener_stop),
            )
        }));
    }
    let reader_bridge = bridge.clone();
    let handle = thread::spawn(move || {
        let result = read_frames(
            reader,
            reader_bridge,
            local_controller_socket,
            local_executor_socket,
        );
        stop.store(true, Ordering::Release);
        // Retire and join both role listeners before this connection returns or
        // its supervisor attempts another generation. Existing requests receive
        // PEER_DISCONNECTED; no request or desktop input is replayed.
        for listener in listeners {
            match listener.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => eprintln!("peer role listener failed: {error:#}"),
                Err(_) => eprintln!("peer role listener panicked"),
            }
        }
        result
    });
    Ok((bridge, handle))
}

fn read_frames(
    reader: Box<dyn Read + Send>,
    bridge: PeerBridge,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
) -> Result<()> {
    let result = read_frames_inner(
        reader,
        bridge.clone(),
        local_controller_socket,
        local_executor_socket,
    );
    let pending = {
        let mut pending = bridge.pending.lock().expect("peer pending lock");
        bridge.connected.store(false, Ordering::Release);
        std::mem::take(&mut *pending)
    };
    for sender in pending.into_values() {
        let _ = sender.send(Response::failure(
            "peer",
            RpcError::new("PEER_DISCONNECTED", "peer framed connection closed"),
        ));
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
        let frame = decode_response_frame(frame, bridge.response_gzip)?;
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
            PeerFrame::ResponseGzip { .. } => unreachable!("compressed responses were decoded"),
            PeerFrame::Request {
                id,
                target_role,
                request,
            } => {
                let writer = Arc::clone(&bridge.writer);
                let response_gzip = bridge.response_gzip;
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
                    let frame = encode_response_frame(id, response, response_gzip);
                    if let Err(error) = write_frame(&writer, &frame) {
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
) -> Result<bool> {
    serde_json::to_writer(
        &mut *writer,
        &PeerFrame::Hello {
            protocol: PEER_PROTOCOL.to_owned(),
            node_id: local_id.to_owned(),
            roles: vec![TargetRole::Controller, TargetRole::Executor],
            response_gzip: true,
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
            response_gzip,
        } if protocol == PEER_PROTOCOL
            && node_id == expected_peer_id
            && has_required_roles(&roles) =>
        {
            Ok(response_gzip)
        }
        other => Err(anyhow!("invalid peer handshake acknowledgement: {other:?}")),
    }
}

fn accept_handshake<R: BufRead, W: Write>(
    reader: &mut R,
    writer: &mut W,
    expected_peer_id: &str,
    local_id: &str,
) -> Result<bool> {
    let frame = read_handshake_frame(reader)?;
    let response_gzip = match frame {
        PeerFrame::Hello {
            protocol,
            node_id,
            roles,
            response_gzip,
        } if protocol == PEER_PROTOCOL
            && node_id == expected_peer_id
            && has_required_roles(&roles) =>
        {
            response_gzip
        }
        other => return Err(anyhow!("invalid peer handshake: {other:?}")),
    };
    serde_json::to_writer(
        &mut *writer,
        &PeerFrame::HelloAck {
            protocol: PEER_PROTOCOL.to_owned(),
            node_id: local_id.to_owned(),
            roles: vec![TargetRole::Controller, TargetRole::Executor],
            response_gzip,
        },
    )?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(response_gzip)
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

// Each response has its own gzip dictionary. Requests and handshake frames stay
// unchanged; old peers omit the capability and receive ordinary responses.
const MAX_GZIP_RESPONSE: usize = 32 * 1024 * 1024;
const MIN_GZIP_RESPONSE: usize = 64 * 1024;

struct ResponseSize(usize);
impl Write for ResponseSize {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|n| *n <= MAX_GZIP_RESPONSE)
            .ok_or_else(|| std::io::Error::other("response exceeds compression budget"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn encode_response_frame(id: String, response: Response, negotiated: bool) -> PeerFrame {
    if negotiated && response.ok {
        let mut size = ResponseSize(0);
        if serde_json::to_writer(&mut size, &response).is_ok() && size.0 >= MIN_GZIP_RESPONSE {
            let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
            let mut buffered = BufWriter::with_capacity(64 * 1024, &mut gzip);
            let serialized = serde_json::to_writer(&mut buffered, &response)
                .map_err(std::io::Error::other)
                .and_then(|()| buffered.flush());
            let _ = buffered.into_parts();
            if serialized.is_ok()
                && let Ok(bytes) = gzip.finish()
            {
                // Include base64/envelope overhead; incompressible responses
                // use the existing format without changing the tool result.
                if bytes.len().div_ceil(3) * 4 + 256 < size.0 {
                    return PeerFrame::ResponseGzip {
                        id,
                        expanded_bytes: size.0,
                        payload: STANDARD.encode(bytes),
                    };
                }
            }
        }
    }
    PeerFrame::Response { id, response }
}

fn decode_response_frame(frame: PeerFrame, negotiated: bool) -> Result<PeerFrame> {
    let PeerFrame::ResponseGzip {
        id,
        expanded_bytes,
        payload,
    } = frame
    else {
        return Ok(frame);
    };
    if !negotiated
        || !(MIN_GZIP_RESPONSE..=MAX_GZIP_RESPONSE).contains(&expanded_bytes)
        || payload.len() > MAX_GZIP_RESPONSE.div_ceil(3) * 4
    {
        return Err(anyhow!("invalid or unnegotiated compressed peer response"));
    }
    let compressed = STANDARD
        .decode(payload)
        .context("decode compressed peer response base64")?;
    let mut bytes = Vec::new();
    GzDecoder::new(compressed.as_slice())
        .take(expanded_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .context("decode compressed peer response gzip")?;
    if bytes.len() != expanded_bytes {
        return Err(anyhow!("compressed peer response length mismatch"));
    }
    let response: Response =
        serde_json::from_slice(&bytes).context("decode compressed peer response JSON")?;
    if !response.ok {
        return Err(anyhow!("compressed error response is not supported"));
    }
    Ok(PeerFrame::Response { id, response })
}

fn write_frame(writer: &SharedWriter, frame: &PeerFrame) -> Result<()> {
    let mut writer = writer.lock().expect("peer writer lock");
    // Serialize under the existing whole-frame lock, but coalesce serde's
    // small writes with bounded memory before reaching the peer pipe.
    let mut buffered = BufWriter::with_capacity(64 * 1024, &mut *writer);
    let result: Result<()> = (|| {
        serde_json::to_writer(&mut buffered, frame)?;
        buffered.write_all(b"\n")?;
        buffered.flush()?;
        Ok(())
    })();
    // BufWriter's Drop may write pending bytes after an error. Discard them
    // explicitly: a partial peer write is unknown and must never be replayed.
    let _ = buffered.into_parts();
    result
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
    fn readonly_diagnostics_do_not_shorten_action_or_build_waits() {
        for action in [
            "ping",
            "status",
            "availability",
            "capability.list",
            "capability.describe",
            "desktop.list",
            "desktop.get",
            "process.get",
            "process.list",
        ] {
            assert_eq!(peer_response_timeout(action), Duration::from_secs(10));
        }
        for action in [
            "command.run",
            "computer-use.call",
            "computer-use.tools",
            "desktop.finish",
            "desktop.recover",
            "desktop.maintenance",
            "desktop.submit",
            "process.start",
            "process.stop",
        ] {
            assert_eq!(peer_response_timeout(action), Duration::from_secs(3600));
        }
    }

    #[test]
    fn response_timeout_keeps_request_identity_sends_once_and_ignores_late_reply() {
        let (writer, reader) = UnixStream::pair().unwrap();
        reader
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let bridge = PeerBridge {
            writer: Arc::new(Mutex::new(Box::new(writer))),
            pending: Arc::new(Mutex::new(HashMap::new())),
            connected: Arc::new(AtomicBool::new(true)),
            response_gzip: false,
        };
        let request = Request::new("status", serde_json::Value::Null);
        let expected_id = request.request_id.clone();
        let response =
            bridge.call_with_timeout(TargetRole::Executor, request, Duration::from_millis(5));
        assert_eq!(response.request_id, expected_id);
        assert_eq!(response.error.unwrap().code, "PEER_RESPONSE_TIMEOUT");
        assert!(bridge.pending.lock().unwrap().is_empty());
        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let PeerFrame::Request { id, .. } = serde_json::from_str(&line).unwrap() else {
            panic!("expected one request frame");
        };
        let mut late = serde_json::to_vec(&PeerFrame::Response {
            id,
            response: Response::success(expected_id, serde_json::json!({"late":true})),
        })
        .unwrap();
        late.push(b'\n');
        let _ = read_frames_inner(
            Box::new(Cursor::new(late)),
            bridge.clone(),
            PathBuf::new(),
            PathBuf::new(),
        );
        assert!(bridge.pending.lock().unwrap().is_empty());
        line.clear();
        assert!(
            reader.read_line(&mut line).is_err(),
            "timeout or late reply must not resend the request"
        );
    }

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
        let ready_deadline = Instant::now() + Duration::from_secs(2);
        while ![&controller_a, &executor_a, &controller_b, &executor_b]
            .iter()
            .all(|socket| socket.exists())
        {
            assert!(
                Instant::now() < ready_deadline,
                "fake role listeners did not bind"
            );
            thread::sleep(Duration::from_millis(5));
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
    fn compressed_responses_route_both_roles_in_both_directions() {
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
                        Response::success(request.request_id, serde_json::json!({"role": role, "outline": "@e1 中文🙂".repeat(20000)}))
                    })
                    .unwrap();
            });
        }
        let ready_deadline = Instant::now() + Duration::from_secs(2);
        while ![&controller_a, &executor_a, &controller_b, &executor_b]
            .iter()
            .all(|socket| socket.exists())
        {
            assert!(
                Instant::now() < ready_deadline,
                "fake role listeners did not bind"
            );
            thread::sleep(Duration::from_millis(5));
        }
        let (a_to_b, b_to_a) = UnixStream::pair().unwrap();
        let a_read = a_to_b.try_clone().unwrap();
        let b_read = b_to_a.try_clone().unwrap();
        let (bridge_a, _reader_a) = start_bridge_with_compression(
            Box::new(a_read),
            Box::new(a_to_b),
            controller_a,
            executor_a,
            directory.path().join("a-sees-b-controller.sock"),
            directory.path().join("a-sees-b-executor.sock"),
            true,
        )
        .unwrap();
        let (bridge_b, _reader_b) = start_bridge_with_compression(
            Box::new(b_read),
            Box::new(b_to_a),
            controller_b,
            executor_b,
            directory.path().join("b-sees-a-controller.sock"),
            directory.path().join("b-sees-a-executor.sock"),
            true,
        )
        .unwrap();
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
            response_gzip: true,
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
    #[test]
    fn peer_disconnect_retires_listeners_before_rebinding() {
        let directory = tempfile::tempdir().unwrap();
        let c = directory.path().join("peer-c.sock");
        let e = directory.path().join("peer-e.sock");
        for _ in 0..3 {
            let (local, remote) = UnixStream::pair().unwrap();
            let (bridge, reader) = start_bridge(
                Box::new(local.try_clone().unwrap()),
                Box::new(local),
                directory.path().join("c.sock"),
                directory.path().join("e.sock"),
                c.clone(),
                e.clone(),
            )
            .unwrap();
            assert!(c.exists() && e.exists());
            assert!(
                start_bridge(
                    Box::new(Cursor::new(Vec::<u8>::new())),
                    Box::new(Vec::<u8>::new()),
                    directory.path().join("c.sock"),
                    directory.path().join("e.sock"),
                    c.clone(),
                    e.clone()
                )
                .is_err()
            );
            assert!(c.exists() && e.exists());
            drop(remote);
            assert!(reader.join().unwrap().is_err());
            assert!(!c.exists() && !e.exists());
            let response = bridge.call(
                TargetRole::Executor,
                Request::new("desktop.list", serde_json::Value::Null),
            );
            assert_eq!(response.error.unwrap().code, "PEER_DISCONNECTED");
            assert!(bridge.pending.lock().unwrap().is_empty());
            drop(bridge);
        }
    }

    #[test]
    fn second_role_bind_failure_releases_first_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        let c = directory.path().join("peer-c.sock");
        let e = directory.path().join("peer-e.sock");
        let _holder = RpcServer::new(&e).bind().unwrap();
        assert!(
            start_bridge(
                Box::new(Cursor::new(Vec::<u8>::new())),
                Box::new(Vec::<u8>::new()),
                directory.path().join("c.sock"),
                directory.path().join("e.sock"),
                c.clone(),
                e.clone()
            )
            .is_err()
        );
        assert!(!c.exists());
        assert!(e.exists());
    }
}

#[cfg(test)]
mod buffered_frame_tests {
    use super::*;

    #[derive(Default)]
    struct Stats {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
    }
    struct Probe {
        stats: Arc<Mutex<Stats>>,
        fail: bool,
        max_write: usize,
    }
    impl Write for Probe {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let mut stats = self.stats.lock().unwrap();
            stats.writes += 1;
            if self.fail {
                return Err(std::io::Error::other("controlled pipe write failure"));
            }
            let count = bytes.len().min(self.max_write);
            stats.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.stats.lock().unwrap().flushes += 1;
            Ok(())
        }
    }
    fn probe(fail: bool, max_write: usize) -> (Probe, Arc<Mutex<Stats>>) {
        let stats = Arc::new(Mutex::new(Stats::default()));
        (
            Probe {
                stats: stats.clone(),
                fail,
                max_write,
            },
            stats,
        )
    }
    fn frame(count: usize) -> PeerFrame {
        let nodes: Vec<_> = (0..count)
            .map(|i| {
                serde_json::json!({
                    "ref": format!("@e{i}"), "role": "AXTextArea", "title": "Owned 中文🙂",
                    "value": "quotes \" and newline\n", "focused": false
                })
            })
            .collect();
        PeerFrame::Response {
            id: "controlled-frame".into(),
            response: Response::success(
                "controlled-request",
                serde_json::json!({"outline": nodes}),
            ),
        }
    }
    #[test]
    fn frames_preserve_protocol_bytes_and_coalesce_writes() {
        for count in [1, 2000] {
            let frame = frame(count);
            let (mut baseline, baseline_stats) = probe(false, usize::MAX);
            serde_json::to_writer(&mut baseline, &frame).unwrap();
            baseline.write_all(b"\n").unwrap();
            baseline.flush().unwrap();
            let (buffered, buffered_stats) = probe(false, usize::MAX);
            let writer: SharedWriter = Arc::new(Mutex::new(Box::new(buffered)));
            write_frame(&writer, &frame).unwrap();
            let baseline = baseline_stats.lock().unwrap();
            let buffered = buffered_stats.lock().unwrap();
            assert_eq!(buffered.bytes, baseline.bytes);
            assert_eq!(buffered.flushes, 1);
            if count == 1 {
                assert_eq!(buffered.writes, 1);
            } else {
                assert!(buffered.writes * 100 < baseline.writes);
            }
            eprintln!(
                "peer frame: nodes={count} bytes={} originalWrites={} bufferedWrites={}",
                buffered.bytes.len(),
                baseline.writes,
                buffered.writes
            );
        }
    }
    #[test]
    fn short_writes_preserve_the_complete_frame() {
        let frame = frame(2);
        let (probe, stats) = probe(false, 7);
        let writer: SharedWriter = Arc::new(Mutex::new(Box::new(probe)));
        write_frame(&writer, &frame).unwrap();
        let mut expected = serde_json::to_vec(&frame).unwrap();
        expected.push(b'\n');
        assert_eq!(stats.lock().unwrap().bytes, expected);
    }
    #[test]
    fn partial_delivery_failure_discards_pending_bytes() {
        struct PartialFailure {
            stats: Arc<Mutex<Stats>>,
        }
        impl Write for PartialFailure {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let mut stats = self.stats.lock().unwrap();
                stats.writes += 1;
                if stats.writes == 1 {
                    let count = bytes.len().min(7);
                    stats.bytes.extend_from_slice(&bytes[..count]);
                    Ok(count)
                } else {
                    Err(std::io::Error::other("failure after partial delivery"))
                }
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.stats.lock().unwrap().flushes += 1;
                Ok(())
            }
        }
        for count in [1, 2000] {
            let stats = Arc::new(Mutex::new(Stats::default()));
            let writer: SharedWriter = Arc::new(Mutex::new(Box::new(PartialFailure {
                stats: stats.clone(),
            })));
            let frame = frame(count);
            assert!(write_frame(&writer, &frame).is_err());
            let stats = stats.lock().unwrap();
            assert_eq!(stats.writes, 2, "no write after observed failure");
            assert_eq!(stats.flushes, 0);
            assert_eq!(stats.bytes, serde_json::to_vec(&frame).unwrap()[..7]);
        }
    }

    #[test]
    fn failed_writes_do_not_flush_or_retry_on_drop() {
        for count in [1, 2000] {
            let (probe, stats) = probe(true, usize::MAX);
            let writer: SharedWriter = Arc::new(Mutex::new(Box::new(probe)));
            assert!(write_frame(&writer, &frame(count)).is_err());
            let stats = stats.lock().unwrap();
            assert_eq!(
                stats.writes, 1,
                "error must not trigger an implicit buffered retry"
            );
            assert_eq!(stats.flushes, 0);
            assert!(stats.bytes.is_empty());
        }
    }
}

#[cfg(test)]
mod compression_tests {
    use super::*;
    use std::io::Cursor;

    fn response() -> Response {
        Response::success(
            "request-identity",
            serde_json::json!({
                "stateId":"fresh-state", "outline": "@e1 中文🙂\n".repeat(12000),
                "actions":[{"ref":"@e1","outcome":"unknown-effect"}], "image":"unchanged"
            }),
        )
    }
    fn assert_same(original: &Response, frame: PeerFrame) {
        let PeerFrame::Response { id, response } = decode_response_frame(frame, true).unwrap()
        else {
            panic!("expected response");
        };
        assert_eq!(id, "routing-identity");
        assert_eq!(
            serde_json::to_value(original).unwrap(),
            serde_json::to_value(response).unwrap()
        );
    }
    #[test]
    fn unicode_refs_ids_and_unknown_action_outcomes_roundtrip_exactly() {
        let original = response();
        let encoded = encode_response_frame("routing-identity".into(), original.clone(), true);
        assert!(matches!(encoded, PeerFrame::ResponseGzip { .. }));
        assert!(
            serde_json::to_vec(&encoded).unwrap().len()
                < serde_json::to_vec(&original).unwrap().len() / 10
        );
        assert_same(&original, encoded);
    }
    #[test]
    fn unnegotiated_small_and_error_responses_keep_old_format() {
        assert!(matches!(
            encode_response_frame("id".into(), response(), false),
            PeerFrame::Response { .. }
        ));
        let small = Response::success("id", serde_json::json!({"ok":true}));
        assert!(matches!(
            encode_response_frame("id".into(), small, true),
            PeerFrame::Response { .. }
        ));
        let error = Response::failure("id", RpcError::new("INPUT_UNKNOWN", "x".repeat(100000)));
        assert!(matches!(
            encode_response_frame("id".into(), error, true),
            PeerFrame::Response { .. }
        ));
    }
    #[test]
    fn incompressible_responses_keep_original_format_and_result() {
        let mut seed = 0x123456789abcdef_u64;
        let text: String = (0..200000)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                char::from(33 + (seed % 94) as u8)
            })
            .collect();
        let original = Response::success("request-identity", serde_json::json!({"text":text}));
        let frame = encode_response_frame("routing-identity".into(), original.clone(), true);
        assert!(matches!(frame, PeerFrame::Response { .. }));
        assert_same(&original, frame);
    }

    #[test]
    fn both_old_peer_handshake_directions_disable_compression() {
        for kind in ["hello", "hello-ack"] {
            let input = serde_json::json!({"type":kind,"protocol":PEER_PROTOCOL,"node_id":"remote","roles":["controller","executor"]});
            let mut reader = Cursor::new(format!("{input}\n"));
            let mut writer = Vec::new();
            let negotiated = if kind == "hello" {
                accept_handshake(&mut reader, &mut writer, "remote", "local")
            } else {
                initiate_handshake(&mut reader, &mut writer, "local", "remote")
            }
            .unwrap();
            assert!(!negotiated);
            if kind == "hello" {
                assert!(
                    !serde_json::from_slice::<serde_json::Value>(&writer).unwrap()["response_gzip"]
                        .as_bool()
                        .unwrap()
                );
            }
        }
    }
    #[test]
    fn both_new_peer_handshake_directions_negotiate_compression() {
        for kind in ["hello", "hello-ack"] {
            let input = serde_json::json!({"type":kind,"protocol":PEER_PROTOCOL,"node_id":"remote","roles":["controller","executor"],"response_gzip":true});
            let mut reader = Cursor::new(format!("{input}\n"));
            let mut writer = Vec::new();
            assert!(
                if kind == "hello" {
                    accept_handshake(&mut reader, &mut writer, "remote", "local")
                } else {
                    initiate_handshake(&mut reader, &mut writer, "local", "remote")
                }
                .unwrap()
            );
        }
    }
    #[test]
    fn compressed_frames_reject_unnegotiated_length_mismatch_and_corruption() {
        let frame = encode_response_frame("id".into(), response(), true);
        assert!(decode_response_frame(frame, false).is_err());
        let PeerFrame::ResponseGzip {
            id,
            expanded_bytes,
            payload,
        } = encode_response_frame("id".into(), response(), true)
        else {
            panic!();
        };
        assert!(
            decode_response_frame(
                PeerFrame::ResponseGzip {
                    id: id.clone(),
                    expanded_bytes: expanded_bytes - 1,
                    payload: payload.clone()
                },
                true
            )
            .is_err()
        );
        let mut gzip = STANDARD.decode(&payload).unwrap();
        gzip.truncate(gzip.len() - 4);
        assert!(
            decode_response_frame(
                PeerFrame::ResponseGzip {
                    id,
                    expanded_bytes,
                    payload: STANDARD.encode(gzip)
                },
                true
            )
            .is_err()
        );
    }
    #[test]
    fn oversized_declarations_and_expansion_bombs_are_bounded() {
        let frame = PeerFrame::ResponseGzip {
            id: "id".into(),
            expanded_bytes: MAX_GZIP_RESPONSE + 1,
            payload: String::new(),
        };
        assert!(decode_response_frame(frame, true).is_err());
        let mut gzip = GzEncoder::new(Vec::new(), Compression::fast());
        gzip.write_all(&vec![b'x'; MIN_GZIP_RESPONSE * 8]).unwrap();
        let frame = PeerFrame::ResponseGzip {
            id: "id".into(),
            expanded_bytes: MIN_GZIP_RESPONSE,
            payload: STANDARD.encode(gzip.finish().unwrap()),
        };
        assert!(decode_response_frame(frame, true).is_err());
        let mut size = ResponseSize(MAX_GZIP_RESPONSE - 1);
        assert!(size.write_all(b"xx").is_err());
    }
}
