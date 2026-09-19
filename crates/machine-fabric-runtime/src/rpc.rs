use anyhow::{Context, Result, anyhow};
use interprocess::local_socket::{GenericFilePath, ListenerOptions, Stream, prelude::*};
use machine_fabric_protocol::{Request, Response, RpcError};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

pub struct RpcServer {
    socket: PathBuf,
}

impl RpcServer {
    pub fn new(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    pub fn serve<F>(&self, handler: F) -> Result<()>
    where
        F: Fn(Request) -> Response + Send + Sync + 'static,
    {
        if let Some(parent) = self.socket.parent() {
            fs::create_dir_all(parent)?;
        }
        let ipc_path = ipc_path(&self.socket);
        let name = ipc_path
            .as_os_str()
            .to_fs_name::<GenericFilePath>()
            .with_context(|| format!("map local IPC name {}", self.socket.display()))?;
        let options = ListenerOptions::new().name(name).try_overwrite(true);
        #[cfg(windows)]
        let options = windows_pipe_permissions(options)?;
        let listener = options
            .create_sync()
            .with_context(|| format!("bind local IPC {}", self.socket.display()))?;
        set_owner_only_permissions(&self.socket)?;
        let handler = Arc::new(handler);
        for stream in listener.incoming() {
            let handler = Arc::clone(&handler);
            match stream {
                Ok(stream) => {
                    thread::spawn(move || {
                        if let Err(error) = handle_stream(stream, handler) {
                            eprintln!("fabric RPC connection failed: {error:#}");
                        }
                    });
                }
                Err(error) => eprintln!("fabric RPC accept failed: {error}"),
            }
        }
        Ok(())
    }
}

fn handle_stream<F>(mut stream: Stream, handler: Arc<F>) -> Result<()>
where
    F: Fn(Request) -> Response,
{
    let mut line = String::new();
    BufReader::new(&mut stream)
        .read_line(&mut line)
        .context("read request")?;
    let response = match serde_json::from_str::<Request>(&line) {
        Ok(request) => handler(request),
        Err(error) => Response::failure(
            "unknown",
            RpcError::new("INVALID_REQUEST", format!("invalid JSON request: {error}")),
        ),
    };
    serde_json::to_writer(&mut stream, &response)?;
    stream.write_all(b"\n")?;
    Ok(())
}

pub fn call_unix(socket: impl AsRef<Path>, request: &Request) -> Result<Response> {
    let ipc_path = ipc_path(socket.as_ref());
    let name = ipc_path
        .as_os_str()
        .to_fs_name::<GenericFilePath>()
        .with_context(|| format!("map local IPC name {}", socket.as_ref().display()))?;
    let mut stream = Stream::connect(name)
        .with_context(|| format!("connect local IPC {}", socket.as_ref().display()))?;
    serde_json::to_writer(&mut stream, request)?;
    stream.write_all(b"\n")?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    if line.is_empty() {
        return Err(anyhow!("server closed connection without a response"));
    }
    Ok(serde_json::from_str(&line)?)
}

#[cfg(unix)]
fn ipc_path(path: &Path) -> PathBuf {
    path.to_path_buf()
}

#[cfg(windows)]
fn ipc_path(path: &Path) -> PathBuf {
    use machine_fabric_core::sha256_bytes;

    let digest = sha256_bytes(path.to_string_lossy().to_ascii_lowercase().as_bytes());
    PathBuf::from(format!(r"\\.\pipe\machine-fabric-{digest}"))
}

#[cfg(unix)]
fn set_owner_only_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(windows)]
fn set_owner_only_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(windows)]
fn windows_pipe_permissions(options: ListenerOptions<'_>) -> Result<ListenerOptions<'_>> {
    use interprocess::os::windows::{
        local_socket::ListenerOptionsExt, security_descriptor::SecurityDescriptor,
    };
    use widestring::U16CString;

    // LocalSystem, administrators, and authenticated local users may exchange
    // RPC frames. SSH still authenticates cross-node access; the pipe is never
    // exposed on the network.
    let sddl = U16CString::from_str("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GRGW;;;AU)")?;
    let descriptor = SecurityDescriptor::deserialize(&sddl)?;
    Ok(options.security_descriptor(descriptor))
}
