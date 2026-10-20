use machine_fabric_peer::{
    Authorizer, ChannelBinding, CloseReason, Handler, Handshake, Lease, PeerConfig, PeerConnection,
    PeerLimits, PeerProtocol, Request, Response, RpcError, Transport,
};
use serde_json::json;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn streams() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let b = listener.accept().unwrap().0;
    a.set_nodelay(true).unwrap();
    b.set_nodelay(true).unwrap();
    (a, b)
}
fn config(local: &str, remote: &str, lease: Lease) -> PeerConfig {
    PeerConfig {
        protocol: PeerProtocol::ScopedV2,
        local_id: local.into(),
        expected_peer_id: remote.into(),
        services: vec!["tools".into()],
        required_peer_services: vec!["tools".into()],
        binding: Some(ChannelBinding {
            lease_id: lease.id().into(),
            scope: json!({"allocation":"allocation-1","attempt":3,"sourceEpoch":2,"targetEpoch":7}),
        }),
        lease: Some(lease),
        limits: PeerLimits {
            handshake_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(2),
            ..PeerLimits::default()
        },
    }
}
fn allow() -> Authorizer {
    Arc::new(|_, _| Ok(()))
}
fn echo() -> Handler {
    Arc::new(|context, r| {
        Response::success(
            r.request_id,
            json!({"input":r.params,"peer":context.peer_id}),
        )
    })
}
fn start_pair(
    a: PeerConfig,
    b: PeerConfig,
    handler: Handler,
    authorize: Authorizer,
) -> (PeerConnection, PeerConnection) {
    let (a_stream, b_stream) = streams();
    let a = PeerConnection::start(
        Transport::from_tcp(a_stream).unwrap(),
        a,
        Handshake::Initiate,
        echo(),
        allow(),
    )
    .unwrap();
    let b = PeerConnection::start(
        Transport::from_tcp(b_stream).unwrap(),
        b,
        Handshake::Accept,
        handler,
        authorize,
    )
    .unwrap();
    (a, b)
}
fn pair(handler: Handler) -> (PeerConnection, PeerConnection) {
    let lease = Lease::new("lease-1", now() + 30_000).unwrap();
    let (a, b) = start_pair(
        config("a", "b", lease.clone()),
        config("b", "a", lease),
        handler,
        allow(),
    );
    a.wait_ready().unwrap();
    b.wait_ready().unwrap();
    (a, b)
}
fn close(a: &PeerConnection, b: &PeerConnection) {
    a.close();
    b.close();
    assert!(a.wait_drained(Duration::from_secs(2)));
    assert!(b.wait_drained(Duration::from_secs(2)));
}

#[test]
fn independent_executors_call_both_directions_concurrently_without_controller() {
    let (a, b) = pair(echo());
    let mut tasks = Vec::new();
    for i in 0..12 {
        for (peer, expected) in [(a.clone(), "a"), (b.clone(), "b")] {
            tasks.push(thread::spawn(move || {
                let response = peer.call("tools", Request::new("observe", json!({"index":i})));
                assert!(response.ok, "{:?}", response.error);
                assert_eq!(response.result.as_ref().unwrap()["peer"], expected);
                assert_eq!(response.result.unwrap()["input"]["index"], i);
            }));
        }
    }
    for task in tasks {
        task.join().unwrap();
    }
    close(&a, &b);
}

#[test]
fn wrong_node_scope_and_service_are_rejected_before_dispatch() {
    for mismatch in ["node", "scope", "service"] {
        let invoked = Arc::new(AtomicBool::new(false));
        let seen = invoked.clone();
        let lease = Lease::new("lease-1", now() + 30_000).unwrap();
        let a = config("a", "b", lease.clone());
        let mut b = config("b", "a", lease);
        match mismatch {
            "node" => b.expected_peer_id = "impostor".into(),
            "scope" => b.binding.as_mut().unwrap().scope["attempt"] = json!(4),
            _ => b.required_peer_services = vec!["not-offered".into()],
        }
        let (a, b) = start_pair(
            a,
            b,
            Arc::new(move |_, r| {
                seen.store(true, Ordering::SeqCst);
                Response::success(r.request_id, json!({}))
            }),
            allow(),
        );
        assert!(a.wait_ready().is_err());
        assert!(b.wait_ready().is_err());
        assert!(!invoked.load(Ordering::SeqCst));
        close(&a, &b);
    }
}

#[test]
fn authorizer_runs_for_every_request_and_can_revoke_action_authority() {
    let authorized = Arc::new(AtomicBool::new(true));
    let authorizer_state = authorized.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let handler_count = count.clone();
    let lease = Lease::new("lease-1", now() + 30_000).unwrap();
    let (a, b) = start_pair(
        config("a", "b", lease.clone()),
        config("b", "a", lease),
        Arc::new(move |_, r| {
            handler_count.fetch_add(1, Ordering::SeqCst);
            Response::success(r.request_id, json!({}))
        }),
        Arc::new(move |context, request| {
            assert_eq!(
                context.binding.as_ref().unwrap().scope["allocation"],
                "allocation-1"
            );
            if authorizer_state.load(Ordering::SeqCst) && request.action == "observe" {
                Ok(())
            } else {
                Err(RpcError::new(
                    "ALLOCATION_DENIED",
                    "permission is no longer valid",
                ))
            }
        }),
    );
    a.wait_ready().unwrap();
    assert!(a.call("tools", Request::new("observe", json!({}))).ok);
    authorized.store(false, Ordering::SeqCst);
    let denied = a.call("tools", Request::new("observe", json!({})));
    assert_eq!(denied.error.unwrap().code, "ALLOCATION_DENIED");
    assert_eq!(count.load(Ordering::SeqCst), 1);
    close(&a, &b);
}

#[test]
fn revoke_cancels_pending_and_handlers_and_is_terminal() {
    let lease = Lease::new("lease-1", now() + 30_000).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (a, b) = start_pair(
        config("a", "b", lease.clone()),
        config("b", "a", lease.clone()),
        Arc::new(move |context, r| {
            entered_tx.send(()).unwrap();
            while !context.cancellation.is_cancelled() {
                thread::sleep(Duration::from_millis(2));
            }
            Response::success(r.request_id, json!({"stopped":true}))
        }),
        allow(),
    );
    a.wait_ready().unwrap();
    let request_peer = a.clone();
    let request = thread::spawn(move || request_peer.call("tools", Request::new("act", json!({}))));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    lease.revoke();
    assert_eq!(
        request.join().unwrap().error.unwrap().code,
        "PEER_OUTCOME_UNKNOWN"
    );
    assert!(lease.renew(now() + 60_000).is_err());
    assert!(a.wait_drained(Duration::from_secs(2)));
    assert!(b.wait_drained(Duration::from_secs(2)));
    assert!(!a.call("tools", Request::new("act", json!({}))).ok);
    assert!(config("a", "b", lease).validate().is_err());
}

#[test]
fn deadline_sends_cooperative_cancel_without_replaying_input() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let (cancel_tx, cancel_rx) = mpsc::channel();
    let (a, b) = pair(Arc::new(move |context, r| {
        count.fetch_add(1, Ordering::SeqCst);
        while !context.cancellation.is_cancelled() {
            thread::sleep(Duration::from_millis(2));
        }
        cancel_tx.send(()).unwrap();
        Response::success(r.request_id, json!({}))
    }));
    let response = a.call_timeout(
        "tools",
        Request::new("click", json!({})),
        Duration::from_millis(100),
    );
    assert_eq!(response.error.unwrap().code, "PEER_OUTCOME_UNKNOWN");
    cancel_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    close(&a, &b);
}

#[test]
fn inbound_and_outbound_limits_reject_without_extra_execution() {
    for limit in ["inbound", "outbound"] {
        let lease = Lease::new("lease-1", now() + 30_000).unwrap();
        let mut a_config = config("a", "b", lease.clone());
        let mut b_config = config("b", "a", lease);
        if limit == "inbound" {
            b_config.limits.max_inbound_requests = 1;
        } else {
            a_config.limits.max_outbound_requests = 1;
        }
        let (entered_tx, entered_rx) = mpsc::channel();
        let release = Arc::new(AtomicBool::new(false));
        let released = release.clone();
        let (a, b) = start_pair(
            a_config,
            b_config,
            Arc::new(move |context, r| {
                entered_tx.send(()).unwrap();
                while !released.load(Ordering::SeqCst) && !context.cancellation.is_cancelled() {
                    thread::sleep(Duration::from_millis(2));
                }
                Response::success(r.request_id, json!({}))
            }),
            allow(),
        );
        a.wait_ready().unwrap();
        let caller = a.clone();
        let first = thread::spawn(move || caller.call("tools", Request::new("hold", json!({}))));
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let second = a.call("tools", Request::new("must-not-run", json!({})));
        assert_eq!(second.error.unwrap().code, "PEER_BUSY");
        assert!(entered_rx.try_recv().is_err());
        release.store(true, Ordering::SeqCst);
        assert!(first.join().unwrap().ok);
        close(&a, &b);
    }
}

#[test]
fn oversized_outbound_frame_is_not_sent() {
    let lease = Lease::new("lease-1", now() + 30_000).unwrap();
    let mut cfg = config("a", "b", lease.clone());
    cfg.limits.max_frame_bytes = 1024;
    let (a, b) = start_pair(cfg, config("b", "a", lease), echo(), allow());
    a.wait_ready().unwrap();
    let result = a.call(
        "tools",
        Request::new("large", json!({"text":"x".repeat(4096)})),
    );
    assert_eq!(result.error.unwrap().code, "PEER_FRAME_TOO_LARGE");
    assert!(a.call("tools", Request::new("small", json!({}))).ok);
    close(&a, &b);
}

#[test]
fn idle_handshake_is_interrupted_by_timeout_and_lease_expiry() {
    for expiry in [false, true] {
        let (stream, _silent) = streams();
        let lease = Lease::new("lease-1", now() + if expiry { 100 } else { 30_000 }).unwrap();
        let mut cfg = config("a", "b", lease);
        cfg.limits.handshake_timeout = Duration::from_millis(if expiry { 2000 } else { 100 });
        let peer = PeerConnection::start(
            Transport::from_tcp(stream).unwrap(),
            cfg,
            Handshake::Accept,
            echo(),
            allow(),
        )
        .unwrap();
        assert!(peer.wait_ready().is_err());
        assert_eq!(
            peer.wait_closed(),
            if expiry {
                CloseReason::LeaseExpired
            } else {
                CloseReason::HandshakeTimeout
            }
        );
        assert!(peer.wait_drained(Duration::from_secs(2)));
    }
}

#[test]
fn oversized_inbound_handshake_is_bounded_and_rejected() {
    let (stream, mut remote) = streams();
    let lease = Lease::new("lease-1", now() + 30_000).unwrap();
    let mut cfg = config("a", "b", lease);
    cfg.limits.max_frame_bytes = 1024;
    let peer = PeerConnection::start(
        Transport::from_tcp(stream).unwrap(),
        cfg,
        Handshake::Accept,
        echo(),
        allow(),
    )
    .unwrap();
    remote.write_all(&vec![b'x'; 2048]).unwrap();
    assert!(peer.wait_ready().is_err());
    assert_eq!(peer.wait_closed(), CloseReason::ProtocolError);
    assert!(peer.wait_drained(Duration::from_secs(2)));
}

#[test]
fn legacy_v1_wire_is_compatible_without_scoped_downgrade() {
    let (stream, mut old_peer) = streams();
    let legacy = PeerConfig {
        protocol: PeerProtocol::LegacyV1,
        local_id: "new".into(),
        expected_peer_id: "old".into(),
        services: vec!["controller".into(), "executor".into()],
        required_peer_services: vec!["controller".into(), "executor".into()],
        binding: None,
        lease: None,
        limits: PeerLimits::default(),
    };
    let peer = PeerConnection::start(
        Transport::from_tcp(stream).unwrap(),
        legacy,
        Handshake::Initiate,
        echo(),
        allow(),
    )
    .unwrap();
    let mut reader = BufReader::new(old_peer.try_clone().unwrap());
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let hello: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(hello["protocol"], "machine-fabric.peer/v1");
    assert!(hello.get("binding").is_none());
    writeln!(old_peer,"{}",json!({"type":"hello-ack","protocol":"machine-fabric.peer/v1","node_id":"old","roles":["controller","executor"]})).unwrap();
    peer.wait_ready().unwrap();
    let caller = peer.clone();
    let call = thread::spawn(move || caller.call("executor", Request::new("ping", json!({}))));
    line.clear();
    reader.read_line(&mut line).unwrap();
    let frame: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(frame["target_role"], "executor");
    writeln!(old_peer,"{}",json!({"type":"response","id":frame["id"],"response":Response::success(frame["request"]["requestId"].as_str().unwrap(),json!({"legacy":true}))})).unwrap();
    assert_eq!(call.join().unwrap().result.unwrap()["legacy"], true);
    peer.close();
    assert!(peer.wait_drained(Duration::from_secs(2)));
}

#[test]
fn close_does_not_claim_noncooperative_handler_has_stopped() {
    let release = Arc::new(AtomicBool::new(false));
    let released = release.clone();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (a, b) = pair(Arc::new(move |_, request| {
        entered_tx.send(()).unwrap();
        while !released.load(Ordering::Acquire) {
            thread::sleep(Duration::from_millis(2));
        }
        Response::success(request.request_id, json!({}))
    }));
    let caller = a.clone();
    let call = thread::spawn(move || caller.call("tools", Request::new("hold", json!({}))));
    entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    b.close();
    assert_eq!(
        call.join().unwrap().error.unwrap().code,
        "PEER_OUTCOME_UNKNOWN"
    );
    assert!(!b.wait_drained(Duration::from_millis(50)));
    assert_eq!(b.status().active_handlers, 1);
    release.store(true, Ordering::Release);
    close(&a, &b);
}

#[test]
fn renewal_extends_live_lease_but_cannot_revive_expiry() {
    let lease = Lease::new("renewable", now() + 200).unwrap();
    let first_expiry = lease.expires_at_ms();
    assert!(lease.renew(first_expiry - 1).is_err());
    lease.renew(first_expiry + 300).unwrap();
    thread::sleep(Duration::from_millis(250));
    assert!(lease.ended().is_none());
    thread::sleep(Duration::from_millis(300));
    assert_eq!(lease.ended(), Some("expired"));
    assert!(lease.renew(now() + 30_000).is_err());
}

#[test]
fn ssh_transport_ignores_user_config_and_requires_pinned_key_auth() {
    let root = tempfile::tempdir().unwrap();
    let identity_file = root.path().join("private key");
    let known_hosts_file = root.path().join("pinned hosts");
    std::fs::write(&identity_file, "test").unwrap();
    std::fs::write(&known_hosts_file, "test").unwrap();
    let endpoint = machine_fabric_peer::SshEndpoint {
        host: "127.0.0.1".into(),
        port: 12345,
        username: "executor".into(),
        identity_file,
        known_hosts_file,
        remote_command: "machine-fabric peer channel-accept --config fixed.json".into(),
        executable: "ssh".into(),
        connect_timeout_seconds: 2,
    };
    let command = endpoint.command().unwrap();
    let argv: Vec<_> = command
        .get_args()
        .map(|s| s.to_string_lossy().into_owned())
        .collect();
    assert_eq!(&argv[..2], &["-F", "none"]);
    for option in [
        "StrictHostKeyChecking=yes",
        "BatchMode=yes",
        "IdentityAgent=none",
        "ClearAllForwardings=yes",
        "PasswordAuthentication=no",
        "ProxyCommand=none",
    ] {
        assert!(argv.iter().any(|arg| arg == option), "{option}");
    }
    assert!(
        argv.iter()
            .any(|arg| arg.starts_with("UserKnownHostsFile=\"") && arg.ends_with('"'))
    );
    assert_eq!(
        &argv[argv.len() - 3..],
        &["--", "127.0.0.1", endpoint.remote_command.as_str()]
    );
}

#[test]
fn gzip_negotiates_in_both_directions_and_falls_back_for_old_peers() {
    use base64::{Engine, engine::general_purpose::STANDARD};
    use std::io::Read;
    for initiate in [true, false] {
        for gzip in [true, false] {
            let (stream, mut remote) = streams();
            remote
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut reader = BufReader::new(remote.try_clone().unwrap());
            let peer = PeerConnection::start(
                Transport::from_tcp(stream).unwrap(),
                PeerConfig {
                    protocol: PeerProtocol::LegacyV1,
                    local_id: "local".into(),
                    expected_peer_id: "remote".into(),
                    services: vec!["tools".into()],
                    required_peer_services: vec!["tools".into()],
                    binding: None,
                    lease: None,
                    limits: PeerLimits::default(),
                },
                if initiate {
                    Handshake::Initiate
                } else {
                    Handshake::Accept
                },
                echo(),
                allow(),
            )
            .unwrap();
            let mut line = String::new();
            if initiate {
                reader.read_line(&mut line).unwrap();
            }
            let mut hello = json!({"type": if initiate { "hello-ack" } else { "hello" },
                "protocol":"machine-fabric.peer/v1", "node_id":"remote", "roles":["tools"]});
            if gzip {
                hello["response_gzip"] = json!(true);
            }
            writeln!(remote, "{hello}").unwrap();
            if !initiate {
                reader.read_line(&mut line).unwrap();
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(&line).unwrap()["response_gzip"],
                    gzip
                );
            }
            peer.wait_ready().unwrap();
            let input = json!({"outline": "@e1 中文🙂\n".repeat(12000)});
            let request = Request::new("observe", input.clone());
            writeln!(
                remote,
                "{}",
                json!({"type":"request", "id":"wire-1", "target_role":"tools", "request":request})
            )
            .unwrap();
            line.clear();
            reader.read_line(&mut line).unwrap();
            let frame: serde_json::Value = serde_json::from_str(&line).unwrap();
            let response: Response = if gzip {
                assert_eq!(frame["type"], "response-gzip");
                let compressed = STANDARD.decode(frame["payload"].as_str().unwrap()).unwrap();
                let mut bytes = Vec::new();
                flate2::read::GzDecoder::new(compressed.as_slice())
                    .read_to_end(&mut bytes)
                    .unwrap();
                assert_eq!(
                    bytes.len() as u64,
                    frame["expanded_bytes"].as_u64().unwrap()
                );
                serde_json::from_slice(&bytes).unwrap()
            } else {
                assert_eq!(frame["type"], "response");
                serde_json::from_value(frame["response"].clone()).unwrap()
            };
            assert_eq!(response.request_id, request.request_id);
            assert_eq!(response.result.unwrap()["input"], input);
            peer.close();
            assert!(peer.wait_drained(Duration::from_secs(2)));
        }
    }
}
