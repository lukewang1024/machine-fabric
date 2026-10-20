use crate::Lease;
use crate::compression::{decode_response_frame, encode_response_frame};
use anyhow::{Context, Result, anyhow, bail};
use machine_fabric_protocol::{Request, Response, RpcError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};
use uuid::Uuid;

const TICK: Duration = Duration::from_millis(20);

/// A stream plus its owner-provided interrupt. `shutdown` must return promptly
/// and unblock both IO halves (e.g. socket shutdown or killing an owned SSH
/// child). A process-owned stdio adapter may instead exit its process after
/// `wait_closed`; it must not claim in-process IO draining.
pub struct Transport {
    reader: Box<dyn Read + Send>,
    writer: Box<dyn Write + Send>,
    shutdown: Arc<dyn Fn() + Send + Sync>,
}

impl Transport {
    pub fn new(
        reader: impl Read + Send + 'static,
        writer: impl Write + Send + 'static,
        shutdown: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            reader: Box::new(reader),
            writer: Box::new(writer),
            shutdown: Arc::new(shutdown),
        }
    }

    /// Convenience for an already authenticated stream, primarily local tests.
    /// TCP alone supplies no authentication or encryption.
    pub fn from_tcp(stream: TcpStream) -> std::io::Result<Self> {
        let reader = stream.try_clone()?;
        let closer = stream.try_clone()?;
        Ok(Self::new(reader, stream, move || {
            let _ = closer.shutdown(Shutdown::Both);
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerProtocol {
    /// Compatibility adapter for existing Fabric nodes. No scoped downgrade.
    LegacyV1,
    /// Requires matching channel bindings and a daemon-owned lease.
    ScopedV2,
}

impl PeerProtocol {
    fn name(self) -> &'static str {
        match self {
            Self::LegacyV1 => "machine-fabric.peer/v1",
            Self::ScopedV2 => "machine-fabric.peer/v2",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ChannelBinding {
    pub lease_id: String,
    /// Trusted platform claims, e.g. allocation, attempt, node epochs, controller.
    /// The component compares these exactly; the authorizer validates authority.
    pub scope: Value,
}

#[derive(Debug, Clone)]
pub struct PeerLimits {
    pub max_frame_bytes: usize,
    pub max_outbound_requests: usize,
    pub max_inbound_requests: usize,
    pub max_queued_frames: usize,
    pub handshake_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for PeerLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 8 * 1024 * 1024,
            max_outbound_requests: 64,
            max_inbound_requests: 16,
            max_queued_frames: 64,
            handshake_timeout: Duration::from_secs(15),
            request_timeout: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub protocol: PeerProtocol,
    pub local_id: String,
    pub expected_peer_id: String,
    pub services: Vec<String>,
    pub required_peer_services: Vec<String>,
    pub binding: Option<ChannelBinding>,
    pub lease: Option<Lease>,
    pub limits: PeerLimits,
}

impl PeerConfig {
    pub fn validate(&self) -> Result<()> {
        for id in [&self.local_id, &self.expected_peer_id] {
            validate_name(id)?;
        }
        for services in [&self.services, &self.required_peer_services] {
            if services.is_empty() || services.len() > 64 {
                bail!("provide 1..64 service names");
            }
            let mut seen = std::collections::HashSet::new();
            for service in services {
                validate_name(service)?;
                if !seen.insert(service) {
                    bail!("duplicate service name");
                }
            }
        }
        match (&self.protocol, &self.binding, &self.lease) {
            (PeerProtocol::LegacyV1, None, None) => {}
            (PeerProtocol::ScopedV2, Some(binding), Some(lease))
                if binding.lease_id == lease.id() && binding.scope.is_object() =>
            {
                if lease.ended().is_some() {
                    bail!("lease is no longer active");
                }
            }
            _ => bail!("scoped peers require a matching binding and lease; v1 is unscoped"),
        }
        let limits = &self.limits;
        if limits.max_frame_bytes < 1024
            || limits.max_frame_bytes > 256 * 1024 * 1024
            || limits.max_outbound_requests == 0
            || limits.max_inbound_requests == 0
            || limits.max_queued_frames == 0
            || limits.handshake_timeout.is_zero()
            || limits.request_timeout.is_zero()
        {
            bail!("invalid peer limits");
        }
        Ok(())
    }
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 256
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "._:-".contains(c))
    {
        bail!("invalid peer/service identity");
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub enum Handshake {
    Initiate,
    Accept,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CloseReason {
    Requested,
    LeaseExpired,
    LeaseRevoked,
    Disconnected,
    ProtocolError,
    HandshakeTimeout,
    Backpressure,
    DrainIncomplete,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerStatus {
    pub ready: bool,
    pub closed: Option<CloseReason>,
    pub pending_requests: usize,
    pub active_handlers: usize,
    pub io_threads: usize,
}

#[derive(Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);

impl Cancellation {
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct RequestContext {
    pub peer_id: String,
    pub service: String,
    pub binding: Option<ChannelBinding>,
    /// Cooperative cancellation. It is not proof a GUI action was undone.
    pub cancellation: Cancellation,
}

pub type Handler = Arc<dyn Fn(RequestContext, Request) -> Response + Send + Sync>;
pub type Authorizer = Arc<dyn Fn(&RequestContext, &Request) -> Result<(), RpcError> + Send + Sync>;

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub(crate) enum Frame {
    Hello {
        protocol: String,
        node_id: String,
        roles: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<ChannelBinding>,
        #[serde(default)]
        response_gzip: bool,
    },
    HelloAck {
        protocol: String,
        node_id: String,
        roles: Vec<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        binding: Option<ChannelBinding>,
        #[serde(default)]
        response_gzip: bool,
    },
    Request {
        id: String,
        target_role: String,
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
    Cancel {
        id: String,
    },
}

struct Pending {
    request_id: String,
    sender: mpsc::Sender<Response>,
}

struct State {
    response_gzip: bool,
    ready: bool,
    remote_services: Vec<String>,
    closed: Option<CloseReason>,
    pending: HashMap<String, Pending>,
    inbound: HashMap<String, Cancellation>,
    io_threads: usize,
}

struct Shared {
    config: PeerConfig,
    state: Mutex<State>,
    changed: Condvar,
    outgoing: mpsc::SyncSender<Vec<u8>>,
    shutdown: Arc<dyn Fn() + Send + Sync>,
}

struct HandleOwner(Arc<Shared>);
impl Drop for HandleOwner {
    fn drop(&mut self) {
        self.0.close(CloseReason::Requested);
    }
}

#[derive(Clone)]
pub struct PeerConnection {
    shared: Arc<Shared>,
    _owner: Arc<HandleOwner>,
}

impl PeerConnection {
    /// Starts bounded IO and handshake workers. Call `wait_ready` before use.
    /// The authorizer is called before every inbound dispatch.
    pub fn start(
        transport: Transport,
        config: PeerConfig,
        handshake: Handshake,
        handler: Handler,
        authorizer: Authorizer,
    ) -> Result<Self> {
        config.validate()?;
        let (outgoing, messages) = mpsc::sync_channel(config.limits.max_queued_frames);
        let shared = Arc::new(Shared {
            config,
            state: Mutex::new(State {
                response_gzip: false,
                ready: false,
                remote_services: Vec::new(),
                closed: None,
                pending: HashMap::new(),
                inbound: HashMap::new(),
                io_threads: 2,
            }),
            changed: Condvar::new(),
            outgoing,
            shutdown: transport.shutdown,
        });
        let peer = Self {
            _owner: Arc::new(HandleOwner(shared.clone())),
            shared: shared.clone(),
        };
        let write_shared = shared.clone();
        thread::spawn(move || {
            let mut writer = transport.writer;
            while !write_shared.is_closed() {
                match messages.recv_timeout(TICK) {
                    Ok(frame) => {
                        if write_shared.ensure_live().is_err() {
                            break;
                        }
                        if writer
                            .write_all(&frame)
                            .and_then(|_| writer.flush())
                            .is_err()
                        {
                            write_shared.close(CloseReason::Disconnected);
                            break;
                        }
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
            write_shared.io_finished();
        });
        let read_shared = shared.clone();
        thread::spawn(move || {
            let result = read_loop(
                transport.reader,
                &read_shared,
                handshake,
                handler,
                authorizer,
            );
            if let Err(reason) = result {
                read_shared.close(reason);
            }
            read_shared.io_finished();
        });
        thread::spawn(move || {
            let deadline = Instant::now() + shared.config.limits.handshake_timeout;
            loop {
                if shared.ensure_live().is_err() {
                    break;
                }
                let state = shared.state.lock().expect("peer state");
                if !state.ready && Instant::now() >= deadline {
                    drop(state);
                    shared.close(CloseReason::HandshakeTimeout);
                    break;
                }
                let _ = shared
                    .changed
                    .wait_timeout(state, TICK)
                    .expect("peer state");
            }
        });
        Ok(peer)
    }

    pub fn wait_ready(&self) -> Result<()> {
        let mut state = self.shared.state.lock().expect("peer state");
        while !state.ready && state.closed.is_none() {
            state = self.shared.changed.wait(state).expect("peer state");
        }
        if let Some(reason) = &state.closed {
            bail!("peer closed before ready: {reason:?}");
        }
        Ok(())
    }

    pub fn status(&self) -> PeerStatus {
        let state = self.shared.state.lock().expect("peer state");
        PeerStatus {
            ready: state.ready && state.closed.is_none(),
            closed: state.closed.clone(),
            pending_requests: state.pending.len(),
            active_handlers: state.inbound.len(),
            io_threads: state.io_threads,
        }
    }

    pub fn call(&self, service: &str, request: Request) -> Response {
        self.call_timeout(service, request, self.shared.config.limits.request_timeout)
    }

    /// Sends at most once. A timeout/disconnect after enqueue has UNKNOWN outcome.
    /// Retrying the same platform command requires the executor's durable journal.
    pub fn call_timeout(&self, service: &str, request: Request, timeout: Duration) -> Response {
        let request_id = request.request_id.clone();
        if let Err(error) = self.shared.ensure_live() {
            return Response::failure(request_id, error);
        }
        if timeout.is_zero() {
            return failure(request_id, "PEER_INVALID_TIMEOUT", "request was not sent");
        }
        let id = format!("peer_request_{}", Uuid::new_v4().simple());
        let bytes = match encode(
            &Frame::Request {
                id: id.clone(),
                target_role: service.to_owned(),
                request,
            },
            self.shared.config.limits.max_frame_bytes,
        ) {
            Ok(bytes) => bytes,
            Err(_) => return failure(request_id, "PEER_FRAME_TOO_LARGE", "request was not sent"),
        };
        let (sender, receiver) = mpsc::channel();
        {
            let mut state = self.shared.state.lock().expect("peer state");
            if state.closed.is_some() || !state.ready {
                return failure(request_id, "PEER_NOT_READY", "request was not sent");
            }
            if !state.remote_services.iter().any(|s| s == service) {
                return failure(
                    request_id,
                    "PEER_SERVICE_UNAVAILABLE",
                    "request was not sent",
                );
            }
            if state.pending.len() >= self.shared.config.limits.max_outbound_requests {
                return failure(
                    request_id,
                    "PEER_BUSY",
                    "outbound limit; request was not sent",
                );
            }
            state.pending.insert(
                id.clone(),
                Pending {
                    request_id: request_id.clone(),
                    sender,
                },
            );
            if self.shared.outgoing.try_send(bytes).is_err() {
                state.pending.remove(&id);
                return failure(
                    request_id,
                    "PEER_BUSY",
                    "outgoing queue full; request was not sent",
                );
            }
        }
        match receiver.recv_timeout(timeout) {
            Ok(response) => response,
            Err(_) => {
                self.shared
                    .state
                    .lock()
                    .expect("peer state")
                    .pending
                    .remove(&id);
                if self.shared.config.protocol == PeerProtocol::ScopedV2 {
                    let _ = self.shared.send(&Frame::Cancel { id });
                }
                failure(
                    request_id,
                    "PEER_OUTCOME_UNKNOWN",
                    "response deadline elapsed; query execution before any retry",
                )
            }
        }
    }

    pub fn close(&self) {
        self.shared.close(CloseReason::Requested);
    }

    pub fn wait_closed(&self) -> CloseReason {
        let mut state = self.shared.state.lock().expect("peer state");
        while state.closed.is_none() {
            state = self.shared.changed.wait(state).expect("peer state");
        }
        state.closed.clone().expect("closed peer")
    }

    /// Waits for IO and cooperative handlers, never reports task/desktop cleanup.
    pub fn wait_drained(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let mut state = self.shared.state.lock().expect("peer state");
        while state.io_threads != 0 || !state.inbound.is_empty() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .expect("peer state")
                .0;
        }
        true
    }
}

fn failure(id: impl Into<String>, code: &str, message: &str) -> Response {
    Response::failure(id, RpcError::new(code, message))
}

impl Shared {
    fn is_closed(&self) -> bool {
        self.state.lock().expect("peer state").closed.is_some()
    }

    fn ensure_live(&self) -> Result<(), RpcError> {
        if let Some(ended) = self.config.lease.as_ref().and_then(Lease::ended) {
            self.close(if ended == "revoked" {
                CloseReason::LeaseRevoked
            } else {
                CloseReason::LeaseExpired
            });
            return Err(RpcError::new(
                "PEER_LEASE_ENDED",
                "channel lease is no longer active",
            ));
        }
        if self.is_closed() {
            return Err(RpcError::new(
                "PEER_CLOSED",
                "channel is closed; request was not sent",
            ));
        }
        Ok(())
    }

    fn close(&self, reason: CloseReason) {
        let pending = {
            let mut state = self.state.lock().expect("peer state");
            if state.closed.is_some() {
                return;
            }
            state.closed = Some(reason);
            for cancellation in state.inbound.values() {
                cancellation.cancel();
            }
            let pending = std::mem::take(&mut state.pending);
            self.changed.notify_all();
            pending
        };
        for pending in pending.into_values() {
            let _ = pending.sender.send(failure(
                pending.request_id,
                "PEER_OUTCOME_UNKNOWN",
                "connection ended before response; query execution before any retry",
            ));
        }
        (self.shutdown)();
    }

    fn io_finished(&self) {
        self.state.lock().expect("peer state").io_threads -= 1;
        self.changed.notify_all();
    }

    fn send(&self, frame: &Frame) -> Result<()> {
        self.ensure_live().map_err(|e| anyhow!(e.message))?;
        let encoded = encode(frame, self.config.limits.max_frame_bytes)?;
        if self.outgoing.try_send(encoded).is_err() {
            self.close(CloseReason::Backpressure);
            bail!("peer write queue is full");
        }
        Ok(())
    }

    fn send_response(&self, id: String, response: Response) {
        let request_id = response.request_id.clone();
        let frame = Frame::Response {
            id: id.clone(),
            response,
        };
        let frame = if encode(&frame, self.config.limits.max_frame_bytes).is_ok() {
            frame
        } else {
            Frame::Response {
                id,
                response: failure(
                    request_id,
                    "PEER_FRAME_TOO_LARGE",
                    "response exceeds frame limit",
                ),
            }
        };
        let negotiated = self.state.lock().expect("peer state").response_gzip;
        let frame = match frame {
            Frame::Response { id, response } => encode_response_frame(id, response, negotiated),
            other => other,
        };
        let _ = self.send(&frame);
    }
}

fn read_loop(
    reader: Box<dyn Read + Send>,
    shared: &Arc<Shared>,
    handshake: Handshake,
    handler: Handler,
    authorizer: Authorizer,
) -> Result<(), CloseReason> {
    let mut reader = BufReader::new(reader);
    let config = &shared.config;
    if matches!(handshake, Handshake::Initiate) {
        shared
            .send(&Frame::Hello {
                protocol: config.protocol.name().into(),
                node_id: config.local_id.clone(),
                roles: config.services.clone(),
                binding: config.binding.clone(),
                response_gzip: true,
            })
            .map_err(|_| CloseReason::Disconnected)?;
    }
    let frame = read_frame(&mut reader, config.limits.max_frame_bytes)?;
    let (protocol, node_id, roles, binding, response_gzip) = match (handshake, frame) {
        (
            Handshake::Initiate,
            Frame::HelloAck {
                protocol,
                node_id,
                roles,
                binding,
                response_gzip,
            },
        )
        | (
            Handshake::Accept,
            Frame::Hello {
                protocol,
                node_id,
                roles,
                binding,
                response_gzip,
            },
        ) => (protocol, node_id, roles, binding, response_gzip),
        _ => return Err(CloseReason::ProtocolError),
    };
    if protocol != config.protocol.name()
        || node_id != config.expected_peer_id
        || binding != config.binding
        || roles.len() > 64
        || config
            .required_peer_services
            .iter()
            .any(|role| !roles.contains(role))
        || roles.iter().any(|role| validate_name(role).is_err())
    {
        return Err(CloseReason::ProtocolError);
    }
    if matches!(handshake, Handshake::Accept) {
        shared
            .send(&Frame::HelloAck {
                protocol: config.protocol.name().into(),
                node_id: config.local_id.clone(),
                roles: config.services.clone(),
                binding: config.binding.clone(),
                response_gzip,
            })
            .map_err(|_| CloseReason::Disconnected)?;
    }
    {
        let mut state = shared.state.lock().expect("peer state");
        if state.closed.is_some() {
            return Ok(());
        }
        state.ready = true;
        state.remote_services = roles;
        state.response_gzip = response_gzip;
        shared.changed.notify_all();
    }
    loop {
        let frame = read_frame(&mut reader, config.limits.max_frame_bytes)?;
        let frame = decode_response_frame(frame, response_gzip, config.limits.max_frame_bytes)
            .map_err(|_| CloseReason::ProtocolError)?;
        shared
            .ensure_live()
            .map_err(|_| CloseReason::Disconnected)?;
        match frame {
            Frame::Response { id, response } => {
                let pending = shared.state.lock().expect("peer state").pending.remove(&id);
                if let Some(pending) = pending {
                    if response.request_id != pending.request_id
                        && config.protocol == PeerProtocol::ScopedV2
                    {
                        let _ = pending.sender.send(failure(
                            pending.request_id,
                            "PEER_OUTCOME_UNKNOWN",
                            "invalid response identity",
                        ));
                        return Err(CloseReason::ProtocolError);
                    }
                    let _ = pending.sender.send(response);
                }
            }
            Frame::Cancel { id } if config.protocol == PeerProtocol::ScopedV2 => {
                if let Some(cancellation) =
                    shared.state.lock().expect("peer state").inbound.get(&id)
                {
                    cancellation.cancel();
                }
            }
            Frame::Request {
                id,
                target_role,
                request,
            } => {
                if id.is_empty() || id.len() > 256 {
                    return Err(CloseReason::ProtocolError);
                }
                if !config.services.contains(&target_role) {
                    shared.send_response(
                        id,
                        failure(
                            request.request_id,
                            "PEER_SERVICE_UNAVAILABLE",
                            "service is not exposed",
                        ),
                    );
                    continue;
                }
                let cancellation = Cancellation::default();
                {
                    let mut state = shared.state.lock().expect("peer state");
                    if state.inbound.contains_key(&id) {
                        return Err(CloseReason::ProtocolError);
                    }
                    if state.inbound.len() >= config.limits.max_inbound_requests {
                        drop(state);
                        shared.send_response(
                            id,
                            failure(
                                request.request_id,
                                "PEER_BUSY",
                                "inbound limit; request was not dispatched",
                            ),
                        );
                        continue;
                    }
                    state.inbound.insert(id.clone(), cancellation.clone());
                }
                let shared = shared.clone();
                let handler = handler.clone();
                let authorizer = authorizer.clone();
                thread::spawn(move || {
                    let context = RequestContext {
                        peer_id: shared.config.expected_peer_id.clone(),
                        service: target_role,
                        binding: shared.config.binding.clone(),
                        cancellation,
                    };
                    let request_id = request.request_id.clone();
                    let response = catch_unwind(AssertUnwindSafe(|| {
                        if let Err(error) = shared
                            .ensure_live()
                            .and_then(|_| authorizer(&context, &request))
                        {
                            return Response::failure(request_id.clone(), error);
                        }
                        if context.cancellation.is_cancelled() {
                            return failure(
                                request_id.clone(),
                                "PEER_CANCELLED",
                                "dispatch was cancelled",
                            );
                        }
                        handler(context, request)
                    }))
                    .unwrap_or_else(|_| {
                        failure(
                            request_id,
                            "PEER_HANDLER_PANICKED",
                            "handler outcome is unknown",
                        )
                    });
                    shared.send_response(id.clone(), response);
                    shared.state.lock().expect("peer state").inbound.remove(&id);
                    shared.changed.notify_all();
                });
            }
            _ => return Err(CloseReason::ProtocolError),
        }
    }
}

fn read_frame(reader: &mut impl BufRead, maximum: usize) -> Result<Frame, CloseReason> {
    let mut bytes = Vec::new();
    reader
        .take(maximum as u64 + 1)
        .read_until(b'\n', &mut bytes)
        .map_err(|_| CloseReason::Disconnected)?;
    if bytes.is_empty() {
        return Err(CloseReason::Disconnected);
    }
    if bytes.len() > maximum || bytes.last() != Some(&b'\n') {
        return Err(CloseReason::ProtocolError);
    }
    serde_json::from_slice(&bytes).map_err(|_| CloseReason::ProtocolError)
}

fn encode(frame: &Frame, maximum: usize) -> Result<Vec<u8>> {
    struct BoundedBytes {
        bytes: Vec<u8>,
        maximum: usize,
    }
    impl Write for BoundedBytes {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("frame limit exceeded"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedBytes {
        bytes: Vec::new(),
        maximum: maximum - 1,
    };
    serde_json::to_writer(&mut writer, frame).context("encode bounded peer frame")?;
    writer.bytes.push(b'\n');
    Ok(writer.bytes)
}
