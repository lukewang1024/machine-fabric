mod computer_use;
mod controller;
mod datapack;
mod desktop;
mod executor;
mod generation;
#[cfg(target_os = "macos")]
mod macos;
mod peer;
mod process;
mod rpc;
mod telemetry;
mod transport;
#[cfg(windows)]
mod windows_computer_use;

pub use controller::Controller;
pub use executor::{ExecutorRuntime, capability_catalog};
pub use peer::{
    PeerAcceptConfig, PeerConnectConfig, PeerStatus, accept_peer, connect_peer, read_peer_status,
};
pub use rpc::{RpcServer, call_unix};
pub use telemetry::{event_fields as log_event, init_logging, request_event};
pub use transport::deploy_binary_over_ssh;
