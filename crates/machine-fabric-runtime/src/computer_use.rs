//! Transport/lifecycle adapter for the unmodified pi-computer-use extension.
//! OS automation and state-scoped UI refs belong entirely to the extension.
use machine_fabric_protocol::RpcError;
use serde_json::{Value, json};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Child;
#[cfg(not(windows))]
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const MAX_RESPONSE: u64 = 16 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(100);

#[derive(Default)]
pub(crate) struct ComputerUseService(Mutex<Option<Host>>);
struct Host {
    session: String,
    identity: Value,
    stream: BufReader<TcpStream>,
    child: Option<Child>,
}
impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.stream.get_ref().shutdown(std::net::Shutdown::Both);
        if let Some(child) = &mut self.child {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn failed(error: impl std::fmt::Display) -> RpcError {
    RpcError::new("COMPUTER_USE_UNAVAILABLE", error.to_string())
}

fn retire_exited_host(host: &mut Option<Host>) {
    // The host exits after its idle timeout. Retire a proven-dead local child
    // before submitting a new request, rather than treating its stale socket
    // as an ambiguous desktop operation. This never retries a sent request.
    if host
        .as_mut()
        .and_then(|h| h.child.as_mut())
        .is_some_and(|child| child.try_wait().ok().flatten().is_some())
    {
        *host = None;
    }
}
fn read_frame(reader: &mut BufReader<TcpStream>) -> Result<Value, RpcError> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_RESPONSE + 1)
        .read_until(b'\n', &mut bytes)
        .map_err(failed)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_RESPONSE || bytes.last() != Some(&b'\n') {
        return Err(failed(
            "host closed or response exceeded 16 MiB; operation outcome may be unknown",
        ));
    }
    serde_json::from_slice(&bytes).map_err(failed)
}
impl ComputerUseService {
    pub(crate) fn status(&self, state_root: &Path) -> Value {
        let state_root = selected_state_root(state_root);
        let selected = fs::read_to_string(state_root.join("host-root")).ok();
        let digest = selected
            .as_ref()
            .and_then(|s| Path::new(s.trim()).file_name())
            .and_then(|s| s.to_str());
        let mut status = json!({"selectedArtifactDigest":digest});
        match self.0.try_lock() {
            Ok(guard) => match guard.as_ref() {
                Some(host) => {
                    status["running"] = json!(true);
                    status["sessionId"] = json!(host.session);
                    status["identity"] = host.identity.clone();
                }
                None => {
                    status["running"] = json!(false);
                }
            },
            Err(_) => {
                status["busy"] = json!(true);
            }
        }
        status
    }
    pub(crate) fn close_existing(&self, state_root: &Path) -> Result<(), RpcError> {
        let state_root = selected_state_root(state_root);
        let session = self
            .0
            .lock()
            .map_err(failed)?
            .as_ref()
            .map(|h| h.session.clone());
        if let Some(session) = session {
            self.call(
                &state_root,
                &json!({"sessionId":session,"tool":"close"}),
                false,
            )?;
        }
        Ok(())
    }

    pub(crate) fn call(
        &self,
        state_root: &Path,
        params: &Value,
        tools_only: bool,
    ) -> Result<Value, RpcError> {
        let state_root = selected_state_root(state_root);
        let session = params["sessionId"].as_str().unwrap_or("discovery");
        if session.is_empty() || session.len() > 256 {
            return Err(RpcError::new(
                "INVALID_PARAMS",
                "sessionId must be 1..256 bytes",
            ));
        }
        let method = if tools_only {
            "tools"
        } else {
            params["tool"]
                .as_str()
                .ok_or_else(|| RpcError::new("INVALID_PARAMS", "tool is required"))?
        };
        // Even observations may initialize/focus native helpers. All tool calls
        // require the same executor-wide desktop lease, including browser tools.
        let mut guard = self.0.lock().map_err(failed)?;
        retire_exited_host(&mut guard);
        if !tools_only && guard.as_ref().is_some_and(|host| host.session != session) {
            *guard = None; // new owner/session invalidates every prior UI ref
        }
        if guard.is_none() {
            *guard = Some(Host::start(&state_root, session)?);
        }
        let host = guard.as_mut().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        let request = json!({"id":id,"method":method,"args":params.get("arguments").cloned().unwrap_or(json!({}))});
        let result = (|| {
            let mut bytes = serde_json::to_vec(&request).map_err(failed)?;
            if bytes.len() > 1024 * 1024 {
                return Err(RpcError::new("INVALID_PARAMS", "request exceeds 1 MiB"));
            }
            bytes.push(b'\n');
            host.stream.get_mut().write_all(&bytes).map_err(failed)?;
            let response = read_frame(&mut host.stream)?;
            if let Some(identity) = response.get("identity") {
                host.identity = identity.clone();
            }
            if response["id"] != id {
                return Err(failed(
                    response["error"]
                        .as_str()
                        .unwrap_or("host response ID mismatch"),
                ));
            }
            if response["ok"] != true {
                return Err(RpcError::new(
                    "COMPUTER_USE_TOOL_FAILED",
                    response["error"]
                        .as_str()
                        .unwrap_or("extension tool failed"),
                ));
            }
            Ok(if tools_only {
                json!({"tools":response["result"], "hostIdentity":host.identity})
            } else if method == "close" {
                json!({"closed":true})
            } else {
                response["result"].clone()
            })
        })();
        // Never replay actions after a timeout, disconnect, or ambiguous result.
        if method == "close"
            || result
                .as_ref()
                .is_err_and(|e| e.code == "COMPUTER_USE_UNAVAILABLE")
        {
            *guard = None;
        }
        result
    }
}

fn selected_state_root(state_root: &Path) -> PathBuf {
    if state_root.join("runtime-root").is_file() {
        return state_root.to_path_buf();
    }
    #[cfg(windows)]
    if let Ok(candidate) = crate::windows_computer_use::interactive_computer_use_state_root()
        && candidate.join("runtime-root").is_file()
    {
        return candidate;
    }
    state_root.to_path_buf()
}

fn node_executable(root: &Path) -> Result<PathBuf, RpcError> {
    match fs::read_to_string(root.join("node-path")) {
        Ok(value) => {
            let selected = PathBuf::from(value.trim());
            if !selected.is_absolute() {
                return Err(failed("computer-use node-path must be absolute"));
            }
            Ok(selected)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Legacy bundled runtimes remain usable during gradual upgrades.
            Ok(root.join(if cfg!(windows) { "node.exe" } else { "node" }))
        }
        Err(error) => Err(failed(error)),
    }
}

impl Host {
    fn start(state_root: &Path, session: &str) -> Result<Self, RpcError> {
        let root = std::env::var_os("WORKBENCH_COMPUTER_USE_ROOT")
            .map(PathBuf::from)
            .or_else(|| {
                fs::read_to_string(state_root.join("runtime-root"))
                    .ok()
                    .map(|value| PathBuf::from(value.trim()))
            })
            .or_else(|| {
                std::env::current_exe()
                    .ok()
                    .and_then(|p| p.parent()?.parent().map(|p| p.join("computer-use")))
            })
            .ok_or_else(|| failed("cannot locate computer-use package"))?;
        let node = node_executable(&root)?;
        // Read the host selection only when starting a new session. An existing
        // session retains its process and immutable host until acknowledged close.
        let host_root = match fs::read_to_string(state_root.join("host-root")) {
            Ok(value) => {
                let selected = PathBuf::from(value.trim());
                if !selected.is_absolute() {
                    return Err(failed("computer-use host-root must be absolute"));
                }
                selected
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => root.clone(),
            Err(error) => return Err(failed(error)),
        };
        let script = host_root.join("host.mjs");
        if !node.is_file()
            || !script.is_file()
            || !root
                .join("node_modules/@injaneity/pi-computer-use/package.json")
                .is_file()
        {
            return Err(failed(
                "managed pi-computer-use runtime is missing; run scripts/install-computer-use.mjs for this release",
            ));
        }
        fs::create_dir_all(state_root).map_err(failed)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(state_root, fs::Permissions::from_mode(0o700)).map_err(failed)?;
        }
        let listener = TcpListener::bind("127.0.0.1:0").map_err(failed)?;
        listener.set_nonblocking(true).map_err(failed)?;
        let token = uuid::Uuid::new_v4().to_string();
        let handshake = state_root.join(format!("handshake-{}.json", uuid::Uuid::new_v4()));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options
            .open(&handshake)
            .map_err(failed)?
            .write_all(token.as_bytes())
            .map_err(failed)?;
        let args = vec![
            script.to_string_lossy().into_owned(),
            listener.local_addr().map_err(failed)?.to_string(),
            handshake.to_string_lossy().into_owned(),
            state_root.to_string_lossy().into_owned(),
            root.to_string_lossy().into_owned(),
        ];
        #[cfg(windows)]
        let spawned: Result<Option<Child>, RpcError> =
            crate::windows_computer_use::spawn_hidden_in_active_session(&node, &args, &root)
                .map(|_| None);
        #[cfg(not(windows))]
        let spawned = Command::new(&node)
            .args(&args)
            .current_dir(&root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map(Some)
            .map_err(failed);
        let mut child = match spawned {
            Ok(child) => child,
            Err(error) => {
                let _ = fs::remove_file(&handshake);
                return Err(error);
            }
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let result = (|| {
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        stream.set_nonblocking(false).map_err(failed)?;
                        stream
                            .set_read_timeout(Some(Duration::from_secs(2)))
                            .map_err(failed)?;
                        stream.set_write_timeout(Some(TIMEOUT)).map_err(failed)?;
                        let mut reader = BufReader::new(stream);
                        if read_frame(&mut reader).is_ok_and(|hello| hello["token"] == token) {
                            reader
                                .get_ref()
                                .set_read_timeout(Some(TIMEOUT))
                                .map_err(failed)?;
                            return Ok(reader);
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(error) => return Err(failed(error)),
                }
                if Instant::now() >= deadline
                    || child
                        .as_mut()
                        .is_some_and(|c| c.try_wait().ok().flatten().is_some())
                {
                    return Err(failed(
                        "computer-use host did not connect; check Node installation and interactive desktop session",
                    ));
                }
                std::thread::sleep(Duration::from_millis(25));
            }
        })();
        let _ = fs::remove_file(&handshake);
        match result {
            Ok(stream) => Ok(Self {
                session: session.into(),
                identity: Value::Null,
                stream,
                child,
            }),
            Err(error) => {
                if let Some(mut child) = child {
                    let _ = child.kill();
                    let _ = child.wait();
                }
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[cfg(unix)]
    fn retires_a_confirmed_exited_host_before_another_request() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (_server, _) = listener.accept().unwrap();
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        child.wait().unwrap();
        let mut host = Some(Host {
            session: "discovery".into(),
            identity: Value::Null,
            stream: BufReader::new(client),
            child: Some(child),
        });
        retire_exited_host(&mut host);
        assert!(host.is_none());
    }
    #[test]
    fn external_node_selection_is_absolute_and_legacy_fallback_is_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        assert_eq!(
            node_executable(root).unwrap(),
            root.join(if cfg!(windows) { "node.exe" } else { "node" })
        );
        let selected = root.join("manager-version").join("node");
        fs::write(root.join("node-path"), selected.to_str().unwrap()).unwrap();
        assert_eq!(node_executable(root).unwrap(), selected);
        fs::write(root.join("node-path"), "relative/node").unwrap();
        assert!(node_executable(root).is_err());
    }
    fn frame(bytes: &'static [u8]) -> Result<Value, RpcError> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (mut server, _) = listener.accept().unwrap();
        std::thread::spawn(move || {
            server.write_all(bytes).unwrap();
        });
        read_frame(&mut BufReader::new(client))
    }
    #[test]
    fn rejects_truncated_or_invalid_frames() {
        assert!(frame(b"{\"ok\":true}").is_err());
        assert!(frame(b"not json\n").is_err());
        assert!(frame(b"").is_err());
        assert_eq!(frame(b"{\"ok\":true}\n").unwrap()["ok"], true);
    }
}
