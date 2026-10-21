//! Executor-owned, bounded duplex RPC. This crate has no dependency on a Fabric
//! Controller, Executor, task store, local IPC implementation, or desktop runtime.
//!
//! A transport authenticates and encrypts the byte stream. The caller supplies
//! trusted node identities, a channel binding, and a per-request authorizer.
//! Neither a matching handshake nor an RPC request ID is a platform permission
//! or a durable execution receipt. See `docs/executor-peer.md` in the repository.

mod compression;
mod connection;
mod lease;
mod ssh;

pub use connection::{
    Authorizer, Cancellation, ChannelBinding, CloseReason, Handler, Handshake, PeerConfig,
    PeerConnection, PeerLimits, PeerProtocol, PeerStatus, RequestContext, Transport,
};
pub use lease::Lease;
pub use machine_fabric_protocol::{Request, Response, RpcError};
pub use ssh::{ManagedSshPeer, ManagedSshStatus, ReconnectPolicy, SshEndpoint};
