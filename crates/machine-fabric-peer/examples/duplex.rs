//! A local, unauthenticated transport demo; production supplies authenticated IO.
use anyhow::Result;
use machine_fabric_peer::{
    Authorizer, ChannelBinding, Handler, Handshake, Lease, PeerConfig, PeerConnection, PeerLimits,
    PeerProtocol, Request, Response, RpcError, Transport,
};
use serde_json::json;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn main() -> Result<()> {
    let expires = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64 + 30_000;
    let lease = Lease::new("demo-lease", expires)?;
    let config = |local: &str, remote: &str| PeerConfig {
        protocol: PeerProtocol::ScopedV2,
        local_id: local.into(),
        expected_peer_id: remote.into(),
        services: vec!["executor.tools".into()],
        required_peer_services: vec!["executor.tools".into()],
        binding: Some(ChannelBinding {
            lease_id: lease.id().into(),
            scope: json!({"allocationId": "demo-allocation", "attempt": 1}),
        }),
        lease: Some(lease.clone()),
        limits: PeerLimits::default(),
    };
    let authorize: Authorizer = Arc::new(|_, request| {
        // A real executor also validates the live platform grant here.
        if request.action == "echo" {
            Ok(())
        } else {
            Err(RpcError::new("ACTION_DENIED", "only echo is allowed"))
        }
    });
    let handler: Handler = Arc::new(|context, request| {
        Response::success(
            request.request_id,
            json!({"from": context.peer_id, "input": request.params}),
        )
    });
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let a_stream = TcpStream::connect(listener.local_addr()?)?;
    let b_stream = listener.accept()?.0;
    let a = PeerConnection::start(
        Transport::from_tcp(a_stream)?,
        config("cloudide", "windows"),
        Handshake::Initiate,
        handler.clone(),
        authorize.clone(),
    )?;
    let b = PeerConnection::start(
        Transport::from_tcp(b_stream)?,
        config("windows", "cloudide"),
        Handshake::Accept,
        handler,
        authorize,
    )?;
    a.wait_ready()?;
    b.wait_ready()?;
    for peer in [&a, &b] {
        let response = peer.call(
            "executor.tools",
            Request::new("echo", json!({"hello": true})),
        );
        anyhow::ensure!(response.ok, "demo call failed: {:?}", response.error);
        println!("{}", serde_json::to_string(&response)?);
    }
    lease.revoke();
    for peer in [&a, &b] {
        peer.wait_closed();
        anyhow::ensure!(
            peer.wait_drained(Duration::from_secs(2)),
            "cleanup incomplete"
        );
    }
    Ok(())
}
