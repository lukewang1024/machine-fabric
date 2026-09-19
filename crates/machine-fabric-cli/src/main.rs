use anyhow::{Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use machine_fabric_core::JsonStore;
use machine_fabric_protocol::Request;
use machine_fabric_runtime::{
    Controller, ExecutorRuntime, PeerAcceptConfig, PeerConnectConfig, RpcServer, accept_peer,
    call_unix, connect_peer, init_logging, read_peer_status,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashSet;
use std::env;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Parser)]
#[command(
    name = "machine-fabric",
    version,
    about = "Manage a small fabric of machines for agent workloads"
)]
struct Cli {
    #[arg(long, global = true)]
    socket: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Status,
    Context {
        #[arg(long)]
        executor: Option<String>,
        #[arg(long)]
        capability: Option<String>,
        #[arg(long = "resource")]
        resources: Vec<String>,
    },
    Logs {
        #[arg(long)]
        component: Option<String>,
        #[arg(long)]
        correlation_id: Option<String>,
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long)]
        task_id: Option<String>,
        #[arg(long)]
        connection_id: Option<String>,
        #[arg(long)]
        since_ms: Option<u64>,
        #[arg(long, default_value_t = 200)]
        tail: usize,
    },
    Call {
        action: String,
        #[arg(default_value = "{}")]
        params: String,
    },
    Controller {
        #[command(subcommand)]
        command: ControllerCommand,
    },
    Executor {
        #[command(subcommand)]
        command: ExecutorCommand,
    },
    Peer {
        #[command(subcommand)]
        command: Box<PeerCommand>,
    },
    Manifest {
        #[command(subcommand)]
        command: ManifestCommand,
    },
}

#[derive(Debug, Subcommand)]
enum ControllerCommand {
    Serve {
        #[arg(long)]
        state: Option<PathBuf>,
        #[arg(long)]
        id: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
enum ExecutorCommand {
    Serve {
        #[arg(long)]
        id: String,
        #[arg(long = "allow-root", required = true)]
        allow_roots: Vec<PathBuf>,
        #[arg(long)]
        state: Option<PathBuf>,
    },
}

#[derive(Debug, Subcommand)]
enum PeerCommand {
    Connect {
        #[arg(long)]
        id: String,
        #[arg(long)]
        local_id: String,
        #[arg(long)]
        host: String,
        #[arg(long)]
        expose_controller_socket: PathBuf,
        #[arg(long)]
        expose_executor_socket: PathBuf,
        #[arg(long)]
        remote_executable: Option<String>,
        #[arg(long)]
        remote_state_root: String,
        #[arg(long, value_enum, default_value_t = RemotePlatform::Posix)]
        remote_platform: RemotePlatform,
        #[arg(long)]
        state: PathBuf,
        #[arg(long)]
        local_controller_socket: Option<PathBuf>,
        #[arg(long)]
        local_executor_socket: Option<PathBuf>,
    },
    Accept {
        #[arg(long)]
        id: String,
        #[arg(long)]
        local_id: String,
        #[arg(long)]
        expose_controller_socket: PathBuf,
        #[arg(long)]
        expose_executor_socket: PathBuf,
        #[arg(long)]
        local_controller_socket: Option<PathBuf>,
        #[arg(long)]
        local_executor_socket: Option<PathBuf>,
    },
    Status {
        #[arg(long)]
        state: PathBuf,
    },
}

#[derive(Debug, Subcommand)]
enum ManifestCommand {
    Validate {
        #[arg(long)]
        file: PathBuf,
    },
    Plan {
        #[arg(long)]
        file: PathBuf,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FabricManifest {
    #[serde(rename = "apiVersion")]
    api_version: String,
    kind: String,
    initiator_node: String,
    nodes: Vec<FabricNode>,
    topology: FabricTopology,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FabricNode {
    id: String,
    platform: FabricPlatform,
    architecture: FabricArchitecture,
    #[serde(default)]
    connection: Option<FabricConnection>,
    allow_roots: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FabricConnection {
    ssh_alias: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum FabricPlatform {
    Macos,
    Linux,
    Windows,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
enum FabricArchitecture {
    #[serde(rename = "aarch64")]
    Aarch64,
    #[serde(rename = "x86_64")]
    X86_64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FabricTopology {
    mode: FabricTopologyMode,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
enum FabricTopologyMode {
    FullMesh,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum RemotePlatform {
    Posix,
    Windows,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match &cli.command {
        Command::Controller { .. } => {
            init_logging("controller", default_log_dir().join("controller.jsonl"))
        }
        Command::Executor { .. } => {
            init_logging("executor", default_log_dir().join("executor.jsonl"))
        }
        Command::Peer { .. } => init_logging("peer", default_log_dir().join("peer.jsonl")),
        _ => {}
    }
    match cli.command {
        Command::Status => print_response(call_unix(
            cli.socket.unwrap_or_else(default_controller_socket),
            &Request::new("status", Value::Null),
        )?),
        Command::Context {
            executor,
            capability,
            resources,
        } => print_response(call_unix(
            cli.socket.unwrap_or_else(default_controller_socket),
            &Request::new(
                "fabric.context",
                serde_json::json!({
                    "executorId": executor,
                    "capability": capability,
                    "resources": resources,
                }),
            ),
        )?),
        Command::Logs {
            component,
            correlation_id,
            request_id,
            task_id,
            connection_id,
            since_ms,
            tail,
        } => print_logs(
            component.as_deref(),
            correlation_id.as_deref(),
            request_id.as_deref(),
            task_id.as_deref(),
            connection_id.as_deref(),
            since_ms,
            tail,
        ),
        Command::Call { action, params } => print_response(call_unix(
            cli.socket.unwrap_or_else(default_controller_socket),
            &Request::new(action, serde_json::from_str(&params)?),
        )?),
        Command::Controller {
            command: ControllerCommand::Serve { state, id },
        } => {
            let controller = Arc::new(Controller::open_with_id(
                JsonStore::new(state.unwrap_or_else(default_controller_state)),
                id,
            )?);
            let handler = Arc::clone(&controller);
            RpcServer::new(cli.socket.unwrap_or_else(default_controller_socket))
                .serve(move |request| handler.handle(request))
        }
        Command::Executor {
            command:
                ExecutorCommand::Serve {
                    id,
                    allow_roots,
                    state,
                },
        } => {
            let executor = Arc::new(
                ExecutorRuntime::open(
                    id,
                    allow_roots,
                    state.unwrap_or_else(default_executor_state),
                )
                .map_err(|error| anyhow::anyhow!("{}: {}", error.code, error.message))?,
            );
            let handler = Arc::clone(&executor);
            RpcServer::new(cli.socket.unwrap_or_else(default_executor_socket))
                .serve(move |request| handler.handle(request))
        }
        Command::Peer { command } => match *command {
            PeerCommand::Connect {
                id,
                local_id,
                host,
                expose_controller_socket,
                expose_executor_socket,
                remote_executable,
                remote_state_root,
                remote_platform,
                state,
                local_controller_socket,
                local_executor_socket,
            } => connect_peer(PeerConnectConfig {
                local_id,
                peer_id: id,
                host,
                local_controller_socket: local_controller_socket
                    .unwrap_or_else(default_controller_socket),
                local_executor_socket: local_executor_socket
                    .unwrap_or_else(default_executor_socket),
                expose_controller_socket,
                expose_executor_socket,
                remote_executable: remote_executable
                    .unwrap_or_else(|| ".local/bin/machine-fabric".to_owned()),
                remote_state_root,
                remote_windows: matches!(remote_platform, RemotePlatform::Windows),
                state_path: state,
            }),
            PeerCommand::Accept {
                id,
                local_id,
                expose_controller_socket,
                expose_executor_socket,
                local_controller_socket,
                local_executor_socket,
            } => accept_peer(PeerAcceptConfig {
                peer_id: id,
                local_id,
                local_controller_socket: local_controller_socket
                    .unwrap_or_else(default_controller_socket),
                local_executor_socket: local_executor_socket
                    .unwrap_or_else(default_executor_socket),
                expose_controller_socket,
                expose_executor_socket,
            }),
            PeerCommand::Status { state } => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&read_peer_status(state)?)?
                );
                Ok(())
            }
        },
        Command::Manifest {
            command: ManifestCommand::Validate { file },
        } => validate_fabric_manifest(&file),
        Command::Manifest {
            command: ManifestCommand::Plan { file },
        } => plan_fabric_manifest(&file),
    }
}

fn validate_fabric_manifest(path: &Path) -> Result<()> {
    let manifest = load_fabric_manifest(path)?;
    println!("{}", serde_json::to_string_pretty(&manifest)?);
    Ok(())
}

fn load_fabric_manifest(path: &Path) -> Result<FabricManifest> {
    let manifest: FabricManifest = serde_yaml::from_slice(&std::fs::read(path)?)?;
    if manifest.api_version != "machine-fabric.dev/v1" {
        bail!("apiVersion must be machine-fabric.dev/v1");
    }
    if manifest.kind != "Fabric" {
        bail!("kind must be Fabric");
    }
    if manifest.nodes.is_empty() {
        bail!("nodes must not be empty");
    }
    let mut identities = HashSet::new();
    for node in &manifest.nodes {
        validate_identity(&node.id)?;
        if !identities.insert(node.id.as_str()) {
            bail!("duplicate node id: {}", node.id);
        }
        if node.id != manifest.initiator_node {
            let connection = node.connection.as_ref().ok_or_else(|| {
                anyhow::anyhow!("remote node {} needs connection.sshAlias", node.id)
            })?;
            validate_identity(&connection.ssh_alias)?;
        }
        if !matches!(
            (node.platform, node.architecture),
            (FabricPlatform::Macos, FabricArchitecture::Aarch64)
                | (FabricPlatform::Linux, FabricArchitecture::X86_64)
                | (FabricPlatform::Windows, FabricArchitecture::X86_64)
        ) {
            bail!(
                "node {} uses an unsupported release platform/architecture combination",
                node.id
            );
        }
        if node.allow_roots.is_empty() {
            bail!("node {} needs at least one allowRoot", node.id);
        }
        for root in &node.allow_roots {
            validate_allow_root(root)
                .map_err(|error| anyhow::anyhow!("node {}: {error}", node.id))?;
        }
    }
    if !identities.contains(manifest.initiator_node.as_str()) {
        bail!("initiatorNode does not reference a node");
    }
    Ok(manifest)
}

fn plan_fabric_manifest(path: &Path) -> Result<()> {
    let manifest = load_fabric_manifest(path)?;
    let remote_nodes: Vec<&FabricNode> = manifest
        .nodes
        .iter()
        .filter(|node| node.id != manifest.initiator_node)
        .collect();
    let mut links = Vec::new();
    for node in &remote_nodes {
        links.push(serde_json::json!({
            "dialer": manifest.initiator_node,
            "peer": node.id,
            "sshAlias": node.connection.as_ref().map(|connection| &connection.ssh_alias),
        }));
    }
    for (index, left) in remote_nodes.iter().enumerate() {
        for right in remote_nodes.iter().skip(index + 1) {
            let (dialer, peer) = if !matches!(left.platform, FabricPlatform::Windows)
                && matches!(right.platform, FabricPlatform::Windows)
            {
                (*left, *right)
            } else if matches!(left.platform, FabricPlatform::Windows)
                && !matches!(right.platform, FabricPlatform::Windows)
            {
                (*right, *left)
            } else if left.id <= right.id {
                (*left, *right)
            } else {
                (*right, *left)
            };
            links.push(serde_json::json!({
                "dialer": dialer.id,
                "peer": peer.id,
                "sshAlias": peer.connection.as_ref().map(|connection| &connection.ssh_alias),
            }));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "apiVersion": manifest.api_version,
            "kind": "FabricPlan",
            "initiatorNode": manifest.initiator_node,
            "nodes": manifest.nodes,
            "links": links,
        }))?
    );
    Ok(())
}

fn validate_identity(value: &str) -> Result<()> {
    if value.is_empty()
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'_' | b'-'))
    {
        bail!("invalid node identity or SSH alias: {value}");
    }
    Ok(())
}

fn validate_allow_root(value: &str) -> Result<()> {
    let trimmed = value.trim_end_matches(['/', '\\']);
    if value.is_empty()
        || matches!(value, "/" | "~" | "$HOME" | "${user.home}")
        || (trimmed.len() == 2
            && trimmed.as_bytes()[0].is_ascii_alphabetic()
            && trimmed.as_bytes()[1] == b':')
    {
        bail!("allowRoot must be a narrow non-root path: {value}");
    }
    if !(value.starts_with('/')
        || value.starts_with("${user.home}/")
        || (value.len() > 3
            && value.as_bytes()[0].is_ascii_alphabetic()
            && value.as_bytes()[1] == b':'
            && matches!(value.as_bytes()[2], b'\\' | b'/')))
    {
        bail!("allowRoot must be absolute or start with ${{user.home}}/: {value}");
    }
    Ok(())
}

fn print_response(response: machine_fabric_protocol::Response) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(&response)?);
    if !response.ok {
        bail!("request failed")
    }
    Ok(())
}

fn state_home() -> PathBuf {
    platform_state_home().join("machine-fabric")
}

fn default_log_dir() -> PathBuf {
    state_home().join("logs")
}

fn print_logs(
    component: Option<&str>,
    correlation_id: Option<&str>,
    request_id: Option<&str>,
    task_id: Option<&str>,
    connection_id: Option<&str>,
    since_ms: Option<u64>,
    tail: usize,
) -> Result<()> {
    let task_correlation = task_id.and_then(|task_id| {
        let state = std::fs::read_to_string(state_home().join("controller.json")).ok()?;
        let state: Value = serde_json::from_str(&state).ok()?;
        state
            .get("tasks")?
            .as_array()?
            .iter()
            .find(|task| task.get("id").and_then(Value::as_str) == Some(task_id))?
            .get("correlationId")?
            .as_str()
            .map(str::to_owned)
    });
    let components: Vec<&str> = component
        .map(|value| vec![value])
        .unwrap_or_else(|| vec!["controller", "executor", "peer"]);
    let mut records = Vec::new();
    for name in components {
        let path = default_log_dir().join(format!("{name}.jsonl"));
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        for line in BufReader::new(file).lines().map_while(Result::ok) {
            let Ok(value) = serde_json::from_str::<Value>(&line) else {
                continue;
            };
            if since_ms.is_some_and(|since| {
                value.get("timestamp").and_then(Value::as_u64).unwrap_or(0) < since
            }) {
                continue;
            }
            let text = value.to_string();
            if [correlation_id, request_id, connection_id]
                .into_iter()
                .flatten()
                .any(|needle| !text.contains(needle))
            {
                continue;
            }
            if task_id.is_some_and(|task_id| {
                !text.contains(task_id)
                    && task_correlation
                        .as_deref()
                        .is_none_or(|correlation_id| !text.contains(correlation_id))
            }) {
                continue;
            }
            records.push(value);
        }
    }
    records.sort_by_key(|value| value.get("timestamp").and_then(Value::as_u64).unwrap_or(0));
    let start = records.len().saturating_sub(tail);
    for record in &records[start..] {
        println!("{}", serde_json::to_string(record)?);
    }
    Ok(())
}

#[cfg(unix)]
fn platform_state_home() -> PathBuf {
    env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env::var_os("HOME").expect("HOME is required")).join(".local/state")
        })
}

#[cfg(windows)]
fn platform_state_home() -> PathBuf {
    env::var_os("LOCALAPPDATA")
        .or_else(|| env::var_os("PROGRAMDATA"))
        .map(PathBuf::from)
        .expect("LOCALAPPDATA or PROGRAMDATA is required")
}

fn default_controller_socket() -> PathBuf {
    state_home().join("controller.sock")
}

fn default_executor_socket() -> PathBuf {
    state_home().join("executor.sock")
}

fn default_controller_state() -> PathBuf {
    state_home().join("controller.json")
}

fn default_executor_state() -> PathBuf {
    state_home().join("executor-fences.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fabric_manifest_is_domain_neutral_and_strict() {
        let manifest: FabricManifest = serde_yaml::from_str(
            r#"
apiVersion: machine-fabric.dev/v1
kind: Fabric
initiatorNode: laptop
nodes:
  - id: laptop
    platform: macos
    architecture: aarch64
    allowRoots: ["${user.home}/Code"]
  - id: devbox-a
    platform: linux
    architecture: x86_64
    connection: {sshAlias: devbox-a}
    allowRoots: ["/srv/workspace"]
topology: {mode: full-mesh}
"#,
        )
        .unwrap();
        assert_eq!(manifest.nodes.len(), 2);
        assert!(
            serde_yaml::from_str::<FabricManifest>(
                r#"
apiVersion: machine-fabric.dev/v1
kind: Fabric
initiatorNode: laptop
nodes: []
topology: {mode: full-mesh}
domains: []
"#
            )
            .is_err()
        );
    }

    #[test]
    fn allow_roots_reject_broad_or_relative_paths() {
        for root in ["/", "~", "$HOME", "${user.home}", "C:\\", "relative/path"] {
            assert!(validate_allow_root(root).is_err(), "accepted {root}");
        }
        for root in ["/srv/workspace", "${user.home}/Code", "D:\\Workspace"] {
            validate_allow_root(root).unwrap();
        }
    }

    #[test]
    fn fabric_architecture_is_explicit() {
        assert!(
            serde_yaml::from_str::<FabricManifest>(
                r#"
apiVersion: machine-fabric.dev/v1
kind: Fabric
initiatorNode: laptop
nodes:
  - id: laptop
    platform: macos
    allowRoots: ["${user.home}/Code"]
topology: {mode: full-mesh}
"#,
            )
            .is_err()
        );
    }
}
