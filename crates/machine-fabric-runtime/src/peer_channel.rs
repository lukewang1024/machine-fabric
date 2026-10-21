//! Language-neutral executor adapter. Configuration and both local IPC endpoints
//! belong to the trusted executor, not its Coding Agent.
use crate::rpc::{ControlledRpcServer, call_unix_controlled};
use anyhow::{Context, Result, bail};
use machine_fabric_peer::{
    Authorizer, ChannelBinding, Handler, Handshake, Lease, ManagedSshPeer, PeerConfig,
    PeerConnection, PeerLimits, PeerProtocol, ReconnectPolicy, SshEndpoint, Transport,
};
use machine_fabric_protocol::{Request, Response, RpcError};
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PeerChannelConfig {
    pub api_version: String,
    pub local_id: String,
    pub peer_id: String,
    pub services: Vec<String>,
    pub required_peer_services: Vec<String>,
    pub binding: ChannelBinding,
    pub expires_at_ms: u64,
    pub handler_socket: PathBuf,
    pub request_socket: PathBuf,
    pub control_socket: PathBuf,
    pub allowed_inbound_actions: Vec<String>,
    pub allowed_outbound_actions: Vec<String>,
    #[serde(default)]
    pub ssh: Option<SshEndpoint>,
    #[serde(default)]
    pub limits: ChannelLimits,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChannelLimits {
    #[serde(default = "frame_limit")]
    pub max_frame_bytes: usize,
    #[serde(default = "outbound_limit")]
    pub max_outbound_requests: usize,
    #[serde(default = "inbound_limit")]
    pub max_inbound_requests: usize,
    #[serde(default = "outbound_limit")]
    pub max_queued_frames: usize,
    #[serde(default = "handshake_timeout")]
    pub handshake_timeout_ms: u64,
    #[serde(default = "request_timeout")]
    pub request_timeout_ms: u64,
}
fn frame_limit() -> usize {
    8 * 1024 * 1024
}
fn outbound_limit() -> usize {
    64
}
fn inbound_limit() -> usize {
    16
}
fn handshake_timeout() -> u64 {
    15_000
}
fn request_timeout() -> u64 {
    60_000
}
impl Default for ChannelLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: frame_limit(),
            max_outbound_requests: outbound_limit(),
            max_inbound_requests: inbound_limit(),
            max_queued_frames: outbound_limit(),
            handshake_timeout_ms: handshake_timeout(),
            request_timeout_ms: request_timeout(),
        }
    }
}

impl PeerChannelConfig {
    /// Reads a private daemon config. OS isolation of the service account remains
    /// the installer's responsibility; a same-user Agent is not isolated by this.
    pub fn read(path: &Path) -> Result<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(path)?;
            if metadata.mode() & 0o077 != 0 || metadata.uid() != unsafe { libc::geteuid() } {
                bail!("peer channel config must be owned by this service user and mode 0600");
            }
        }
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .context("open daemon peer config")?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1024 * 1024 {
            bail!("peer channel config exceeds 1 MiB");
        }
        serde_json::from_slice(&bytes).context("decode daemon peer config")
    }

    fn peer_config(&self, lease: Lease) -> Result<PeerConfig> {
        if self.api_version != "machine-fabric.peer-channel/v1" {
            bail!("unsupported peer channel config version");
        }
        let paths = [
            &self.handler_socket,
            &self.request_socket,
            &self.control_socket,
        ];
        if paths.iter().any(|path| !path.is_absolute())
            || self.handler_socket == self.request_socket
            || self.handler_socket == self.control_socket
            || self.request_socket == self.control_socket
        {
            bail!("channel IPC paths must be absolute and distinct");
        }
        for actions in [
            &self.allowed_inbound_actions,
            &self.allowed_outbound_actions,
        ] {
            if actions.len() > 256
                || actions.iter().any(|action| {
                    action.is_empty() || action.len() > 256 || action.chars().any(char::is_control)
                })
            {
                bail!("invalid exact action allowlist");
            }
        }
        let config = PeerConfig {
            protocol: PeerProtocol::ScopedV2,
            local_id: self.local_id.clone(),
            expected_peer_id: self.peer_id.clone(),
            services: self.services.clone(),
            required_peer_services: self.required_peer_services.clone(),
            binding: Some(self.binding.clone()),
            lease: Some(lease),
            limits: PeerLimits {
                max_frame_bytes: self.limits.max_frame_bytes,
                max_outbound_requests: self.limits.max_outbound_requests,
                max_inbound_requests: self.limits.max_inbound_requests,
                max_queued_frames: self.limits.max_queued_frames,
                handshake_timeout: Duration::from_millis(self.limits.handshake_timeout_ms),
                request_timeout: Duration::from_millis(self.limits.request_timeout_ms),
            },
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Clone)]
enum Channel {
    Stdio(PeerConnection),
    Ssh(Arc<ManagedSshPeer>),
}
impl Channel {
    fn call(&self, service: &str, request: Request) -> Response {
        match self {
            Self::Stdio(peer) => peer.call(service, request),
            Self::Ssh(peer) => peer.call(service, request),
        }
    }
    fn close(&self) {
        match self {
            Self::Stdio(peer) => peer.close(),
            Self::Ssh(peer) => peer.close(),
        }
    }
    fn ended(&self) -> bool {
        match self {
            Self::Stdio(peer) => peer.status().closed.is_some(),
            Self::Ssh(peer) => peer.status().state == "stopped",
        }
    }
    fn status(&self) -> Value {
        match self {
            Self::Stdio(peer) => serde_json::to_value(peer.status()).expect("peer status"),
            Self::Ssh(peer) => serde_json::to_value(peer.status()).expect("SSH status"),
        }
    }
}

/// Runs a private adapter process. `connect=true` owns an SSH child; accepting
/// processes use their SSH session's stdin/stdout and terminate when it closes.
/// No platform credential acquisition, sshd setup, or global registration occurs.
pub fn run_peer_channel(config: PeerChannelConfig, connect: bool) -> Result<()> {
    if connect != config.ssh.is_some() {
        bail!("connect requires SSH settings; accept must not contain SSH settings");
    }
    let lease = Lease::new(&config.binding.lease_id, config.expires_at_ms)?;
    let peer_config = config.peer_config(lease.clone())?;
    if let Some(ssh) = &config.ssh {
        ssh.validate()?;
    }
    let requests = ControlledRpcServer::bind(&config.request_socket)?;
    let controls = ControlledRpcServer::bind(&config.control_socket)?;
    let maximum_frame = config.limits.max_frame_bytes;
    let maximum_clients = config.limits.max_outbound_requests;
    let handler_socket = config.handler_socket;
    let request_timeout = Duration::from_millis(config.limits.request_timeout_ms);
    let handler: Handler = Arc::new(move |context, request| {
        // This envelope is generated by the trusted adapter. Agent-supplied
        // params cannot overwrite its binding, source identity or service.
        let forwarded = Request {
            api_version: request.api_version.clone(),
            request_id: request.request_id.clone(),
            correlation_id: request.correlation_id.clone(),
            parent_request_id: request.parent_request_id.clone(),
            action: "peer.dispatch".into(),
            params: json!({
                "peerId":context.peer_id,
                "service":context.service,
                "binding":context.binding,
                "request":request,
            }),
        };
        call_unix_controlled(
            &handler_socket,
            &forwarded,
            maximum_frame,
            request_timeout,
            &|| context.cancellation.is_cancelled(),
        )
        .unwrap_or_else(|_| {
            Response::failure(
                forwarded.request_id,
                RpcError::new(
                    "PEER_OUTCOME_UNKNOWN",
                    "local handler interrupted; query the command journal",
                ),
            )
        })
    });
    let inbound = config.allowed_inbound_actions;
    let authorize: Authorizer = Arc::new(move |_, request| {
        if inbound.contains(&request.action) {
            Ok(())
        } else {
            Err(RpcError::new(
                "PEER_ACTION_DENIED",
                "action is outside channel grant",
            ))
        }
    });
    let channel = if let Some(ssh) = config.ssh {
        Channel::Ssh(Arc::new(ManagedSshPeer::start(
            ssh,
            peer_config,
            handler,
            authorize,
            ReconnectPolicy::default(),
        )?))
    } else {
        Channel::Stdio(PeerConnection::start(
            Transport::new(std::io::stdin(), std::io::stdout(), || {}),
            peer_config,
            Handshake::Accept,
            handler,
            authorize,
        )?)
    };
    let request_channel = channel.clone();
    let request_lifecycle = channel.clone();
    let outbound = config.allowed_outbound_actions;
    let request_thread = thread::spawn(move || {
        requests.serve(
            move |request| {
                if request.action != "peer.call" {
                    return Response::failure(
                        request.request_id,
                        RpcError::new(
                            "PEER_ACTION_DENIED",
                            "only peer.call is available on this endpoint",
                        ),
                    );
                }
                let service = request.params.get("service").and_then(Value::as_str);
                let inner = serde_json::from_value::<Request>(request.params["request"].clone());
                match (service, inner) {
                    (Some(service), Ok(inner)) if outbound.contains(&inner.action) => {
                        let result = request_channel.call(service, inner);
                        // Preserve the local RPC's identity and the remote RPC's full envelope.
                        Response::success(request.request_id, result)
                    }
                    _ => Response::failure(
                        request.request_id,
                        RpcError::new(
                            "PEER_ACTION_DENIED",
                            "invalid or unauthorized outbound request",
                        ),
                    ),
                }
            },
            move || request_lifecycle.ended(),
            maximum_frame,
            maximum_clients,
        )
    });
    let control_channel = channel.clone();
    let control_lifecycle = channel.clone();
    let control_lease = lease.clone();
    let control_thread = thread::spawn(move || {
        controls.serve(move |request| {
        match request.action.as_str() {
            "peer.status"=>Response::success(request.request_id,json!({
                "channel":control_channel.status(),"leaseId":control_lease.id(),
                "expiresAtMs":control_lease.expires_at_ms(),"leaseEnded":control_lease.ended(),
            })),
            "peer.revoke"=>{
                control_lease.revoke();
                control_channel.close();
                Response::success(request.request_id,json!({"revoked":true}))
            }
            "peer.close"=>{
                control_channel.close();
                Response::success(request.request_id,json!({"closed":true}))
            }
            "peer.renew"=>{
                match request.params["expiresAtMs"].as_u64().ok_or_else(|| anyhow::anyhow!("missing expiry"))
                    .and_then(|expiry|control_lease.renew(expiry)) {
                    Ok(())=>Response::success(request.request_id,json!({"expiresAtMs":control_lease.expires_at_ms()})),
                    Err(_)=>Response::failure(request.request_id,RpcError::new("PEER_RENEW_DENIED","lease cannot be renewed")),
                }
            }
            _=>Response::failure(request.request_id,RpcError::new("PEER_ACTION_DENIED","unsupported control action")),
        }
    },move || control_lifecycle.ended(),64*1024,8)
    });
    while !channel.ended() {
        if request_thread.is_finished() || control_thread.is_finished() {
            channel.close();
        }
        thread::sleep(Duration::from_millis(20));
    }
    channel.close();
    request_thread
        .join()
        .map_err(|_| anyhow::anyhow!("channel IPC worker panicked"))??;
    control_thread
        .join()
        .map_err(|_| anyhow::anyhow!("channel control worker panicked"))??;
    if let Channel::Ssh(peer) = channel
        && !peer.wait_stopped(Duration::from_secs(5))
    {
        bail!("SSH child shutdown is not confirmed");
    }
    // An accepting adapter is an owned process: returning exits and closes its
    // stdin/stdout threads. Native tool process cleanup is separately confirmed
    // by the executor, never inferred from this return.
    Ok(())
}
