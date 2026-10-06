use anyhow::{Context, Result, anyhow};
use interprocess::local_socket::{
    GenericFilePath, Listener, ListenerNonblockingMode, ListenerOptions, Stream, prelude::*,
};
use machine_fabric_protocol::{Request, Response, RpcError};
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

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
        self.bind()?.serve(handler, None)
    }

    // Bind synchronously: a peer is not ready until both role endpoints exist.
    pub(crate) fn bind(&self) -> Result<BoundRpcServer> {
        if let Some(parent) = self.socket.parent() {
            fs::create_dir_all(parent)?;
        }
        #[cfg(unix)]
        let ownership = SocketOwnership::acquire(&self.socket)?;
        let ipc_path = ipc_path(&self.socket);
        let name = ipc_path
            .as_os_str()
            .to_fs_name::<GenericFilePath>()
            .with_context(|| format!("map local IPC name {}", self.socket.display()))?;
        let options = ListenerOptions::new()
            .name(name)
            .try_overwrite(true)
            .reclaim_name(false);
        #[cfg(windows)]
        let options = windows_pipe_permissions(options)?;
        let listener = options
            .create_sync()
            .with_context(|| format!("bind local IPC {}", self.socket.display()))?;
        #[cfg(unix)]
        let ownership = ownership.bound()?;
        let server = BoundRpcServer {
            listener,
            #[cfg(unix)]
            _ownership: ownership,
        };
        set_owner_only_permissions(&self.socket)?;
        Ok(server)
    }
}

pub(crate) struct BoundRpcServer {
    listener: Listener,
    #[cfg(unix)]
    _ownership: SocketOwnership,
}

impl BoundRpcServer {
    pub(crate) fn serve<F>(self, handler: F, stop: Option<Arc<AtomicBool>>) -> Result<()>
    where
        F: Fn(Request) -> Response + Send + Sync + 'static,
    {
        if stop.is_some() {
            self.listener
                .set_nonblocking(ListenerNonblockingMode::Accept)?;
        }
        let handler = Arc::new(handler);
        loop {
            if stop
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Acquire))
            {
                break;
            }
            match self.listener.accept() {
                Ok(stream) => {
                    let handler = Arc::clone(&handler);
                    thread::spawn(move || {
                        if let Err(error) = handle_stream(stream, handler) {
                            eprintln!("fabric RPC connection failed: {error:#}");
                        }
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(20));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error).context("fabric RPC accept failed"),
            }
        }
        Ok(())
    }
}

// Keep the lock file in place: removing it would let another generation acquire
// a different inode while this generation still owns the old lock.
#[cfg(unix)]
struct SocketOwnership {
    path: PathBuf,
    identity: Option<(u64, u64)>,
    _lock: fs::File,
}

#[cfg(unix)]
impl SocketOwnership {
    fn acquire(path: &Path) -> Result<Self> {
        use std::os::unix::{
            fs::{FileTypeExt, OpenOptionsExt},
            io::AsRawFd,
        };
        let mut lock_path = path.as_os_str().to_os_string();
        lock_path.push(".lock");
        let lock = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(lock_path)?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error()).context("IPC endpoint already owned");
        }
        let ownership = Self {
            path: path.to_path_buf(),
            identity: None,
            _lock: lock,
        };
        match fs::symlink_metadata(path) {
            Ok(metadata) if !metadata.file_type().is_socket() => {
                return Err(anyhow!(
                    "refusing to replace non-socket IPC endpoint {}",
                    path.display()
                ));
            }
            Ok(_) => {
                // Existing versions do not hold our lock. Do not unlink their
                // live listener during a rollout or a concurrent peer reconnect.
                let probe_path = path.to_path_buf();
                let (sender, receiver) = std::sync::mpsc::sync_channel(1);
                thread::spawn(move || {
                    let result = std::os::unix::net::UnixStream::connect(probe_path)
                        .map(|_| ())
                        .map_err(|error| error.kind());
                    let _ = sender.send(result);
                });
                match receiver.recv_timeout(Duration::from_millis(200)) {
                    Ok(Err(
                        std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound,
                    )) => {}
                    _ => {
                        return Err(anyhow!(
                            "IPC endpoint live or liveness unconfirmed: {}",
                            path.display()
                        ));
                    }
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(ownership)
    }

    fn bound(mut self) -> Result<Self> {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::symlink_metadata(&self.path)?;
        self.identity = Some((metadata.dev(), metadata.ino()));
        Ok(self)
    }
}

#[cfg(unix)]
impl Drop for SocketOwnership {
    fn drop(&mut self) {
        use std::os::unix::{fs::MetadataExt, io::AsRawFd};
        if let Some(identity) = self.identity
            && let Ok(metadata) = fs::symlink_metadata(&self.path)
            && (metadata.dev(), metadata.ino()) == identity
            && let Err(error) = fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            eprintln!("remove owned IPC endpoint failed: {error}");
        }
        // Explicit unlock also retires any brief fork-before-exec inheritance
        // of this open file description in unrelated process-starting threads.
        unsafe {
            libc::flock(self._lock.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

// Keep local IPC writes bounded and coalesced, without buffering a whole response.
fn write_json_frame(writer: &mut impl Write, frame: &impl serde::Serialize) -> Result<()> {
    let mut buffered = BufWriter::with_capacity(64 * 1024, writer);
    let result = (|| {
        serde_json::to_writer(&mut buffered, frame)?;
        buffered.write_all(b"\n")?;
        buffered.flush()?;
        Ok(())
    })();
    // Drop must not retry pending bytes after a partial write or failed flush.
    let _ = buffered.into_parts();
    result
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
    write_json_frame(&mut stream, &response)?;
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
    write_json_frame(&mut stream, request)?;
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn concurrent_bind_cannot_unlink_active_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rpc.sock");
        let server = RpcServer::new(&path).bind().unwrap();
        assert!(RpcServer::new(&path).bind().is_err());
        assert!(std::os::unix::net::UnixStream::connect(&path).is_ok());
        drop(server);
        assert!(!path.exists());
        assert!(RpcServer::new(&path).bind().is_ok());
    }

    #[test]
    fn old_generation_drop_preserves_replacement_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rpc.sock");
        let server = RpcServer::new(&path).bind().unwrap();
        fs::remove_file(&path).unwrap();
        let replacement = UnixListener::bind(&path).unwrap();
        drop(server);
        assert!(path.exists());
        assert!(std::os::unix::net::UnixStream::connect(&path).is_ok());
        drop(replacement);
    }

    #[test]
    fn rollout_preserves_unlocked_legacy_listener_and_recovers_stale_socket() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rpc.sock");
        let old = UnixListener::bind(&path).unwrap();
        assert!(RpcServer::new(&path).bind().is_err());
        assert!(path.exists());
        drop(old);
        // Parallel process tests can briefly inherit the legacy listener until
        // exec closes its CLOEXEC fd. Preserve it until it is actually dead.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let server = loop {
            match RpcServer::new(&path).bind() {
                Ok(server) => break server,
                Err(error) => {
                    assert!(std::time::Instant::now() < deadline, "{error:#}");
                    thread::sleep(Duration::from_millis(10));
                }
            }
        };
        drop(server);
        assert!(!path.exists());
    }

    #[test]
    fn bind_preserves_non_socket_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rpc.sock");
        fs::write(&path, "important").unwrap();
        assert!(RpcServer::new(&path).bind().is_err());
        assert_eq!(fs::read_to_string(path).unwrap(), "important");
    }
}

#[cfg(test)]
mod frame_write_tests {
    use super::*;
    #[derive(Default)]
    struct Probe {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
        limit: usize,
        fail_after: Option<usize>,
    }
    impl Write for Probe {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.writes += 1;
            if self.fail_after.is_some_and(|n| self.bytes.len() >= n) {
                return Err(std::io::Error::other("controlled write failure"));
            }
            let count = bytes.len().min(self.limit);
            self.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }
    fn large_response() -> Response {
        Response::success(
            "owned",
            serde_json::json!({"nodes":(0..12000).map(|i|
            serde_json::json!({"ref":format!("@e{i}"),"value":"Owned 中文🙂 with \"quotes\" and newline\n"})).collect::<Vec<_>>()}),
        )
    }
    #[test]
    fn large_frames_are_byte_exact_with_bounded_write_count() {
        let frame = large_response();
        let mut expected = serde_json::to_vec(&frame).unwrap();
        expected.push(b'\n');
        let mut p = Probe {
            limit: usize::MAX,
            ..Probe::default()
        };
        write_json_frame(&mut p, &frame).unwrap();
        assert_eq!(p.bytes, expected);
        assert!(p.writes <= expected.len().div_ceil(64 * 1024));
        assert_eq!(p.flushes, 1);
    }
    #[test]
    fn short_writes_preserve_unicode_and_one_complete_frame() {
        let frame = large_response();
        let mut expected = serde_json::to_vec(&frame).unwrap();
        expected.push(b'\n');
        let mut p = Probe {
            limit: 7,
            ..Probe::default()
        };
        write_json_frame(&mut p, &frame).unwrap();
        assert_eq!(p.bytes, expected);
    }
    #[test]
    fn partial_write_failure_never_retries_during_drop() {
        let frame = large_response();
        let expected = serde_json::to_vec(&frame).unwrap();
        let mut p = Probe {
            limit: 7,
            fail_after: Some(7),
            ..Probe::default()
        };
        assert!(write_json_frame(&mut p, &frame).is_err());
        assert_eq!(p.bytes, expected[..7]);
        assert_eq!(p.writes, 2);
        assert_eq!(p.flushes, 0);
    }
}
