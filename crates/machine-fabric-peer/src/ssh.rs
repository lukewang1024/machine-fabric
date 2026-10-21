use crate::{
    Authorizer, CloseReason, Handler, Handshake, PeerConfig, PeerConnection, PeerProtocol, Request,
    Response, RpcError, Transport,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Private executor configuration, never an Agent-facing connection descriptor.
/// `remote_command` is the administrator-provisioned forced peer entrypoint,
/// not an arbitrary tool command or string supplied by the Coding Agent.
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SshEndpoint {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub identity_file: PathBuf,
    pub known_hosts_file: PathBuf,
    pub remote_command: String,
    #[serde(default = "default_ssh")]
    pub executable: PathBuf,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_seconds: u64,
}

fn default_ssh() -> PathBuf {
    PathBuf::from("ssh")
}
fn default_connect_timeout() -> u64 {
    10
}

impl SshEndpoint {
    pub fn validate(&self) -> Result<()> {
        if self.host.is_empty()
            || self.host.starts_with('-')
            || !self
                .host
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || ".:-_[]".contains(c))
            || self.port == 0
            || self.username.is_empty()
            || !self
                .username
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-\\@".contains(c))
            || !self.identity_file.is_absolute()
            || !self.known_hosts_file.is_absolute()
            || self.remote_command.is_empty()
            || self.remote_command.len() > 16384
            || self
                .remote_command
                .chars()
                .any(|c| c == '\0' || c == '\r' || c == '\n')
            || self.connect_timeout_seconds == 0
            || self.connect_timeout_seconds > 300
            || self.executable.as_os_str().is_empty()
        {
            bail!("invalid executor-owned SSH configuration");
        }
        for path in [&self.identity_file, &self.known_hosts_file] {
            if !path.is_file() {
                bail!("SSH identity or pinned known-hosts file is unavailable");
            }
        }
        Ok(())
    }

    /// Native argv; does not run a local shell, read the user's ssh config, use
    /// an agent, forward sockets, or fall back to passwords/interactive prompts.
    pub fn command(&self) -> Result<Command> {
        self.validate()?;
        let mut command = Command::new(&self.executable);
        command.args([
            "-F",
            "none",
            "-T",
            "-a",
            "-x",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "IdentityAgent=none",
            "-o",
            "PasswordAuthentication=no",
            "-o",
            "KbdInteractiveAuthentication=no",
            "-o",
            "PreferredAuthentications=publickey",
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "ControlMaster=no",
            "-o",
            "ControlPath=none",
            "-o",
            "ProxyCommand=none",
            "-o",
            "ProxyJump=none",
            "-o",
            "ServerAliveInterval=5",
            "-o",
            "ServerAliveCountMax=2",
            "-o",
            "TCPKeepAlive=yes",
        ]);
        #[cfg(windows)]
        command.args(["-o", "GlobalKnownHostsFile=NUL"]);
        #[cfg(not(windows))]
        command.args(["-o", "GlobalKnownHostsFile=/dev/null"]);
        command
            .arg("-o")
            .arg(format!("ConnectTimeout={}", self.connect_timeout_seconds));
        // A separate argv item still needs ssh_config quoting for paths with spaces.
        let known_hosts = self.known_hosts_file.to_string_lossy();
        #[cfg(windows)]
        let known_hosts = known_hosts.replace('\\', "/");
        if known_hosts
            .chars()
            .any(|c| matches!(c, '"' | '\r' | '\n' | '\0' | '%'))
        {
            bail!("unsupported known-hosts path");
        }
        command
            .arg("-o")
            .arg(format!("UserKnownHostsFile=\"{known_hosts}\""));
        command.arg("-i").arg(&self.identity_file);
        command.arg("-p").arg(self.port.to_string());
        command.arg("-l").arg(&self.username);
        command.arg("--").arg(&self.host).arg(&self.remote_command);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        Ok(command)
    }

    fn open(&self) -> Result<(Transport, Arc<Mutex<Child>>)> {
        let mut child = self
            .command()?
            .spawn()
            .context("start executor SSH transport")?;
        let reader = child.stdout.take().expect("piped SSH stdout");
        let writer = child.stdin.take().expect("piped SSH stdin");
        let child = Arc::new(Mutex::new(child));
        let shutdown_child = child.clone();
        Ok((
            Transport::new(reader, writer, move || {
                let mut child = shutdown_child.lock().expect("SSH child");
                let _ = child.kill();
            }),
            child,
        ))
    }
}

#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// Do not reconnect while old handlers/IO remain alive after closure.
    pub drain_timeout: Duration,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedSshStatus {
    pub state: String,
    pub generation: u64,
    pub last_close: Option<CloseReason>,
}

struct ManagedState {
    current: Option<PeerConnection>,
    status: ManagedSshStatus,
    stopped: bool,
}

struct Managed {
    state: Mutex<ManagedState>,
    changed: Condvar,
    stop: AtomicBool,
}

/// Reconnects a *channel*, never a request. A single lease spans all attempts.
/// The executor remains responsible for endpoint setup, credentials, its
/// independent management channel, and cleanup of remote application processes.
pub struct ManagedSshPeer {
    managed: Arc<Managed>,
}

impl ManagedSshPeer {
    pub fn start(
        endpoint: SshEndpoint,
        config: PeerConfig,
        handler: Handler,
        authorizer: Authorizer,
        policy: ReconnectPolicy,
    ) -> Result<Self> {
        endpoint.validate()?;
        config.validate()?;
        if config.protocol != PeerProtocol::ScopedV2 || config.lease.is_none() {
            bail!("managed SSH requires a scoped v2 lease");
        }
        if policy.initial_backoff.is_zero()
            || policy.max_backoff < policy.initial_backoff
            || policy.drain_timeout.is_zero()
        {
            bail!("invalid reconnect policy");
        }
        let managed = Arc::new(Managed {
            state: Mutex::new(ManagedState {
                current: None,
                status: ManagedSshStatus {
                    state: "connecting".into(),
                    generation: 0,
                    last_close: None,
                },
                stopped: false,
            }),
            changed: Condvar::new(),
            stop: AtomicBool::new(false),
        });
        let worker = managed.clone();
        thread::spawn(move || {
            let lease = config.lease.as_ref().expect("validated lease");
            let mut backoff = policy.initial_backoff;
            while !worker.stop.load(Ordering::Acquire) && lease.ended().is_none() {
                {
                    let mut state = worker.state.lock().expect("managed peer");
                    state.status.state = "connecting".into();
                    state.status.generation += 1;
                    worker.changed.notify_all();
                }
                let started = Instant::now();
                let mut last_close = CloseReason::Disconnected;
                if let Ok((transport, child)) = endpoint.open() {
                    match PeerConnection::start(
                        transport,
                        config.clone(),
                        Handshake::Initiate,
                        handler.clone(),
                        authorizer.clone(),
                    ) {
                        Ok(peer) => {
                            {
                                let mut state = worker.state.lock().expect("managed peer");
                                state.current = Some(peer.clone());
                                if worker.stop.load(Ordering::Acquire) || lease.ended().is_some() {
                                    peer.close();
                                }
                            }
                            if peer.wait_ready().is_ok() {
                                let mut state = worker.state.lock().expect("managed peer");
                                state.status.state = "ready".into();
                                worker.changed.notify_all();
                            }
                            last_close = peer.wait_closed();
                            // Close happens before reaping, so a blocked SSH read is interrupted.
                            peer.close();
                            let _ = child.lock().expect("SSH child").wait();
                            if !peer.wait_drained(policy.drain_timeout) {
                                last_close = CloseReason::DrainIncomplete;
                                worker.stop.store(true, Ordering::Release);
                            }
                        }
                        Err(_) => {
                            let mut child = child.lock().expect("SSH child");
                            let _ = child.kill();
                            let _ = child.wait();
                        }
                    }
                }
                {
                    let mut state = worker.state.lock().expect("managed peer");
                    state.current = None;
                    state.status.state = "backoff".into();
                    state.status.last_close = Some(last_close);
                    worker.changed.notify_all();
                }
                if started.elapsed() >= Duration::from_secs(30) {
                    backoff = policy.initial_backoff;
                }
                let until = Instant::now() + backoff;
                while Instant::now() < until
                    && !worker.stop.load(Ordering::Acquire)
                    && lease.ended().is_none()
                {
                    let state = worker.state.lock().expect("managed peer");
                    let _ = worker
                        .changed
                        .wait_timeout(state, Duration::from_millis(20))
                        .expect("managed peer");
                }
                backoff = backoff.saturating_mul(2).min(policy.max_backoff);
            }
            let mut state = worker.state.lock().expect("managed peer");
            state.current = None;
            state.status.state = "stopped".into();
            state.status.last_close = match lease.ended() {
                Some("expired") => Some(CloseReason::LeaseExpired),
                Some(_) => Some(CloseReason::LeaseRevoked),
                None if state.status.last_close == Some(CloseReason::DrainIncomplete) => {
                    Some(CloseReason::DrainIncomplete)
                }
                None => Some(CloseReason::Requested),
            };
            state.stopped = true;
            worker.changed.notify_all();
        });
        Ok(Self { managed })
    }

    pub fn call(&self, service: &str, request: Request) -> Response {
        let peer = self
            .managed
            .state
            .lock()
            .expect("managed peer")
            .current
            .clone();
        match peer {
            Some(peer) => peer.call(service, request),
            None => Response::failure(
                request.request_id,
                RpcError::new(
                    "PEER_NOT_READY",
                    "channel is unavailable; request was not sent",
                ),
            ),
        }
    }

    pub fn status(&self) -> ManagedSshStatus {
        self.managed
            .state
            .lock()
            .expect("managed peer")
            .status
            .clone()
    }

    pub fn close(&self) {
        self.managed.stop.store(true, Ordering::Release);
        let peer = self
            .managed
            .state
            .lock()
            .expect("managed peer")
            .current
            .clone();
        if let Some(peer) = peer {
            peer.close();
        }
        self.managed.changed.notify_all();
    }

    pub fn wait_stopped(&self, timeout: Duration) -> bool {
        let state = self.managed.state.lock().expect("managed peer");
        self.managed
            .changed
            .wait_timeout_while(state, timeout, |s| !s.stopped)
            .expect("managed peer")
            .0
            .stopped
    }
}

impl Drop for ManagedSshPeer {
    fn drop(&mut self) {
        self.close();
    }
}
