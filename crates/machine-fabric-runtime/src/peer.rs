use crate::rpc::{ControlledRpcServer, call_unix_controlled};
use anyhow::{Context, Result, anyhow};
use machine_fabric_core::atomic_replace;
use machine_fabric_peer::{
    Handshake, PeerConfig, PeerConnection, PeerLimits, PeerProtocol, Transport,
};
use machine_fabric_protocol::{Request, Response, RpcError};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::{call_unix, log_event};

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
    let (bridge, reader) = start_bridge(
        Transport::new(std::io::stdin(), std::io::stdout(), || {}),
        (&config.local_id, &config.peer_id),
        Handshake::Accept,
        config.local_controller_socket,
        config.local_executor_socket,
        config.expose_controller_socket,
        config.expose_executor_socket,
    )?;
    bridge.wait_ready()?;
    reader
        .join()
        .map_err(|_| anyhow!("peer lifecycle thread panicked"))?
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
    let stdin = child.stdin.take().expect("SSH stdin is piped");
    let child = Arc::new(Mutex::new(child));
    let closer = child.clone();
    let transport = Transport::new(BufReader::new(stdout), stdin, move || {
        let _ = closer.lock().expect("SSH child").kill();
    });
    let (bridge, reader) = match start_bridge(
        transport,
        (&config.local_id, &config.peer_id),
        Handshake::Initiate,
        config.local_controller_socket.clone(),
        config.local_executor_socket.clone(),
        config.expose_controller_socket.clone(),
        config.expose_executor_socket.clone(),
    ) {
        Ok(result) => result,
        Err(error) => {
            let mut child = child.lock().expect("SSH child");
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let ready = bridge.wait_ready().and_then(|_| {
        for role in ["controller", "executor"] {
            let response = bridge.call(role, Request::new("ping", serde_json::Value::Null));
            if !response.ok {
                return Err(anyhow!("remote {role} did not become ready"));
            }
        }
        Ok(())
    });
    if let Err(error) = ready {
        bridge.close();
        let _ = child.lock().expect("SSH child").wait();
        let _ = reader.join();
        return Err(error);
    }
    if let Err(error) = write_status(
        &config.state_path,
        &config.peer_id,
        &config.host,
        connection_id,
        generation,
        "ready",
        None,
    ) {
        bridge.close();
        let _ = child.lock().expect("SSH child").wait();
        let _ = reader.join();
        return Err(error);
    }
    let reader_result = reader
        .join()
        .map_err(|_| anyhow!("peer reader thread panicked"))?;
    bridge.close();
    let _ = child.lock().expect("SSH child").wait();
    reader_result
}

fn start_bridge(
    transport: Transport,
    identities: (&str, &str),
    handshake: Handshake,
    local_controller_socket: PathBuf,
    local_executor_socket: PathBuf,
    expose_controller_socket: PathBuf,
    expose_executor_socket: PathBuf,
) -> Result<(PeerConnection, thread::JoinHandle<Result<()>>)> {
    let limits = PeerLimits {
        max_frame_bytes: 64 * 1024 * 1024,
        max_inbound_requests: 128,
        max_outbound_requests: 128,
        request_timeout: Duration::from_secs(3600),
        ..PeerLimits::default()
    };
    let maximum_frame = limits.max_frame_bytes;
    // Bind synchronously, so closure cannot race an as-yet unbound listener.
    let controller_server = ControlledRpcServer::bind_legacy(&expose_controller_socket)?;
    let executor_server = ControlledRpcServer::bind_legacy(&expose_executor_socket)?;
    let bridge = PeerConnection::start(
        transport,
        PeerConfig {
            protocol: PeerProtocol::LegacyV1,
            local_id: identities.0.to_owned(),
            expected_peer_id: identities.1.to_owned(),
            services: vec!["controller".into(), "executor".into()],
            required_peer_services: vec!["controller".into(), "executor".into()],
            binding: None,
            lease: None,
            limits,
        },
        handshake,
        Arc::new(move |context, request| {
            let socket = if context.service == "controller" {
                &local_controller_socket
            } else {
                &local_executor_socket
            };
            call_unix_controlled(
                socket,
                &request,
                maximum_frame,
                peer_response_timeout(&request.action),
                &|| context.cancellation.is_cancelled(),
            )
            .unwrap_or_else(|_| {
                Response::failure(
                    request.request_id,
                    RpcError::new(
                        "LOCAL_ROLE_UNAVAILABLE",
                        "local role unavailable or interrupted",
                    ),
                )
            })
        }),
        Arc::new(|_, _| Ok(())),
    )?;
    let mut listeners = Vec::new();
    for (server, role) in [
        (controller_server, "controller"),
        (executor_server, "executor"),
    ] {
        let caller = bridge.clone();
        let lifecycle = bridge.clone();
        listeners.push(thread::spawn(move || {
            server.serve(
                move |request| {
                    log_event(
                        "info",
                        "peer.request.started",
                        serde_json::json!({"requestId": request.request_id, "correlationId": request.correlation_id, "targetRole": role}),
                    );
                    let timeout = peer_response_timeout(&request.action);
                    caller.call_timeout(role, request, timeout)
                },
                move || lifecycle.status().closed.is_some(),
                maximum_frame,
                128,
            )
        }));
    }
    let lifecycle = bridge.clone();
    let waiter = thread::spawn(move || {
        while lifecycle.status().closed.is_none() {
            if listeners.iter().any(thread::JoinHandle::is_finished) {
                lifecycle.close();
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        let reason = lifecycle.wait_closed();
        for listener in listeners {
            listener
                .join()
                .map_err(|_| anyhow!("peer IPC listener panicked"))??;
        }
        Err(anyhow!("peer connection closed: {reason:?}"))
    });
    Ok((bridge, waiter))
}

fn next_peer_backoff(current: Duration, connected_for: Duration) -> Duration {
    if connected_for >= PEER_STABLE_CONNECTION {
        PEER_BACKOFF_INITIAL
    } else {
        (current * 2).min(PEER_BACKOFF_MAX)
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};

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
    fn legacy_runtime_routes_both_roles_and_releases_all_exposed_listeners() {
        let directory = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let mut local_servers = Vec::new();
        let mut paths = Vec::new();
        for role in ["ca", "ea", "cb", "eb"] {
            let path = directory.path().join(role);
            let server = ControlledRpcServer::bind(&path).unwrap();
            let flag = stop.clone();
            local_servers.push(thread::spawn(move || {
                server
                    .serve(
                        move |request| {
                            Response::success(request.request_id, serde_json::json!({"role":role}))
                        },
                        move || flag.load(Ordering::Acquire),
                        1024 * 1024,
                        4,
                    )
                    .unwrap()
            }));
            paths.push(path);
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let a_stream = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let b_stream = listener.accept().unwrap().0;
        let exposed: Vec<_> = ["ac", "ae", "bc", "be"]
            .iter()
            .map(|p| directory.path().join(p))
            .collect();
        let (a, a_wait) = start_bridge(
            Transport::from_tcp(a_stream).unwrap(),
            ("a", "b"),
            Handshake::Initiate,
            paths[0].clone(),
            paths[1].clone(),
            exposed[0].clone(),
            exposed[1].clone(),
        )
        .unwrap();
        let (b, b_wait) = start_bridge(
            Transport::from_tcp(b_stream).unwrap(),
            ("b", "a"),
            Handshake::Accept,
            paths[2].clone(),
            paths[3].clone(),
            exposed[2].clone(),
            exposed[3].clone(),
        )
        .unwrap();
        a.wait_ready().unwrap();
        b.wait_ready().unwrap();
        for (path, expected) in exposed.iter().zip(["cb", "eb", "ca", "ea"]) {
            let response = call_unix(path, &Request::new("ping", serde_json::Value::Null)).unwrap();
            assert!(response.ok, "{:?}", response.error);
            assert_eq!(response.result.unwrap()["role"], expected);
        }
        a.close();
        assert!(a_wait.join().unwrap().is_err());
        assert!(b_wait.join().unwrap().is_err());
        assert!(a.wait_drained(Duration::from_secs(2)));
        assert!(b.wait_drained(Duration::from_secs(2)));
        // Rebinding proves the listeners really closed, including Named Pipes.
        for path in exposed {
            drop(ControlledRpcServer::bind(&path).unwrap());
        }
        stop.store(true, Ordering::Release);
        for server in local_servers {
            server.join().unwrap();
        }
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
}
