//! One-shot commands have a real deadline, including output collection.
//! Use process.start for persistent processes. A timeout never authorizes replay.
use machine_fabric_protocol::RpcError;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

pub fn timeout_ms(value: Option<&Value>, default: u64) -> Result<u64, RpcError> {
    match value {
        None => Ok(default),
        Some(value) => value
            .as_u64()
            .filter(|value| (1..=3_600_000).contains(value))
            .ok_or_else(|| {
                RpcError::new(
                    "INVALID_PARAMS",
                    "timeoutMs must be an integer from 1 to 3600000",
                )
            }),
    }
}

fn failed(error: impl std::fmt::Display) -> RpcError {
    RpcError::new("COMMAND_FAILED", error.to_string())
}

pub const OUTPUT_LIMIT: usize = 64 * 1024;

#[derive(Debug)]
pub struct CapturedOutput {
    pub bytes: Vec<u8>,
    pub original_bytes: u64,
}

impl CapturedOutput {
    pub fn truncated(&self) -> bool {
        self.original_bytes > self.bytes.len() as u64
            || String::from_utf8_lossy(&self.bytes).len() > OUTPUT_LIMIT
    }

    pub fn text(&self) -> String {
        let notice = if self.truncated() {
            format!(
                "[machine-fabric: output truncated; showing tail of {} original bytes]\n",
                self.original_bytes
            )
        } else {
            String::new()
        };
        let budget = OUTPUT_LIMIT - notice.len();
        let start = self.bytes.len().saturating_sub(budget);
        let tail = String::from_utf8_lossy(&self.bytes[start..]);
        // Lossy decoding can expand a cut/invalid UTF-8 sequence. Keep the
        // response byte bound and a valid UTF-8 suffix even for binary output.
        let mut offset = tail.len().saturating_sub(budget);
        while !tail.is_char_boundary(offset) {
            offset += 1;
        }
        notice + &tail[offset..]
    }
}

#[derive(Debug)]
pub struct Output {
    pub status: ExitStatus,
    pub stdout: CapturedOutput,
    pub stderr: CapturedOutput,
}

fn tail(file: &mut File) -> Result<CapturedOutput, RpcError> {
    // Private anonymous files avoid an unbounded in-memory output buffer and
    // pipe EOF waits when a descendant inherits stdout/stderr.
    let len = file.metadata().map_err(failed)?.len();
    let start = len.saturating_sub(OUTPUT_LIMIT as u64);
    file.seek(SeekFrom::Start(start)).map_err(failed)?;
    let mut bytes = Vec::new();
    // A descendant may still append after the root exits. Read the measured
    // snapshot only, so its original byte count describes these same bytes.
    file.take(len - start)
        .read_to_end(&mut bytes)
        .map_err(failed)?;
    Ok(CapturedOutput {
        bytes,
        original_bytes: len,
    })
}

#[cfg(unix)]
struct Tree(u32);

#[cfg(unix)]
impl Tree {
    fn attach(child: &Child) -> Result<Self, RpcError> {
        Ok(Self(child.id()))
    }

    fn terminate(&self) -> Result<(), RpcError> {
        // Only the process group created for this invocation is signalled.
        if unsafe { libc::kill(-(self.0 as i32), libc::SIGKILL) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(failed(error))
        }
    }
}

#[cfg(windows)]
struct Tree(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
impl Tree {
    fn attach(child: &Child) -> Result<Self, RpcError> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::JobObjects::*;
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(failed(std::io::Error::last_os_error()));
        }
        let tree = Self(handle);
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        } == 0
            || unsafe { AssignProcessToJobObject(handle, child.as_raw_handle().cast()) } == 0
        {
            return Err(failed(std::io::Error::last_os_error()));
        }
        Ok(tree)
    }

    fn terminate(&self) -> Result<(), RpcError> {
        if unsafe { windows_sys::Win32::System::JobObjects::TerminateJobObject(self.0, 1) } == 0 {
            Err(failed(std::io::Error::last_os_error()))
        } else {
            Ok(())
        }
    }
}

#[cfg(windows)]
impl Drop for Tree {
    fn drop(&mut self) {
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

pub fn run(
    argv: &[String],
    cwd: &Path,
    env: &BTreeMap<String, String>,
    timeout: u64,
) -> Result<Output, RpcError> {
    let executable = argv
        .first()
        .ok_or_else(|| RpcError::new("INVALID_PARAMS", "argv must not be empty"))?;
    let mut stdout = tempfile::tempfile().map_err(failed)?;
    let mut stderr = tempfile::tempfile().map_err(failed)?;
    let mut command = Command::new(executable);
    command
        .args(&argv[1..])
        .current_dir(cwd)
        .envs(env)
        .stdin(Stdio::null())
        .stdout(stdout.try_clone().map_err(failed)?)
        .stderr(stderr.try_clone().map_err(failed)?);
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let started = Instant::now();
    let mut child = command.spawn().map_err(failed)?;
    let tree = match Tree::attach(&child) {
        Ok(tree) => Some(tree),
        Err(error) => {
            // A short command can exit before Windows assigns its job.
            if child.try_wait().map_err(failed)?.is_some() {
                None
            } else {
                let _ = child.kill();
                return Err(error);
            }
        }
    };
    loop {
        let status = child.try_wait().map_err(failed)?;
        if started.elapsed() >= Duration::from_millis(timeout) {
            // Do not signal a Unix group whose root was already reaped: its
            // numeric identity could subsequently be reused.
            let termination = if status.is_none() {
                tree.as_ref().map(Tree::terminate).transpose()
            } else {
                Ok(None)
            };
            let reap_deadline = Instant::now() + Duration::from_secs(2);
            let mut stopped = status.is_some();
            while !stopped && Instant::now() < reap_deadline {
                if child.try_wait().map_err(failed)?.is_some() {
                    stopped = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            let mut error = RpcError::new(
                "COMMAND_TIMED_OUT",
                "Command deadline exceeded; side effects may have occurred; do not replay automatically",
            );
            let stdout = tail(&mut stdout)?;
            let stderr = tail(&mut stderr)?;
            error.details = json!({"timeoutMs": timeout, "pid": child.id(),
                "terminationRequested": status.is_none() && termination.is_ok(), "rootStopped": stopped,
                "outcome": "unknown", "stdout": stdout.text(), "stderr": stderr.text(),
                "stdoutBytes": stdout.original_bytes, "stderrBytes": stderr.original_bytes,
                "stdoutTruncated": stdout.truncated(), "stderrTruncated": stderr.truncated(),
                "outputLimitBytes": OUTPUT_LIMIT });
            return Err(error);
        }
        if let Some(status) = status {
            return Ok(Output {
                status,
                stdout: tail(&mut stdout)?,
                stderr: tail(&mut stderr)?,
            });
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn shell(script: &str) -> Vec<String> {
        #[cfg(unix)]
        {
            vec!["/bin/sh".into(), "-c".into(), script.into()]
        }
        #[cfg(windows)]
        {
            vec!["cmd.exe".into(), "/D".into(), "/C".into(), script.into()]
        }
    }
    #[test]
    fn validates_deadline_before_spawn() {
        assert_eq!(timeout_ms(None, 300_000).unwrap(), 300_000);
        assert_eq!(timeout_ms(Some(&json!(12)), 300_000).unwrap(), 12);
        for value in [
            json!(0),
            json!(-1),
            json!(3_600_001),
            json!("12"),
            Value::Null,
        ] {
            assert_eq!(
                timeout_ms(Some(&value), 300_000).unwrap_err().code,
                "INVALID_PARAMS"
            );
        }
    }
    #[test]
    fn retains_status_and_output() {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        let script = "echo output; echo diagnostic >&2; exit 7";
        #[cfg(windows)]
        let script = "echo output & echo diagnostic >&2 & exit 7";
        let output = run(&shell(script), directory.path(), &BTreeMap::new(), 2_000).unwrap();
        assert_eq!(output.status.code(), Some(7));
        assert!(output.stdout.text().contains("output"));
        assert!(output.stderr.text().contains("diagnostic"));
    }
    #[test]
    fn hanging_command_stops_with_partial_output_without_replay() {
        let directory = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        let script = "echo once >> receipt; echo before; sleep 30";
        #[cfg(windows)]
        let script = "echo once >> receipt & echo before & ping -n 30 127.0.0.1 >NUL";
        let started = Instant::now();
        let error = run(&shell(script), directory.path(), &BTreeMap::new(), 500).unwrap_err();
        assert_eq!(error.code, "COMMAND_TIMED_OUT");
        assert_eq!(error.details["rootStopped"], true);
        assert!(error.details["stdout"].as_str().unwrap().contains("before"));
        assert!(started.elapsed() < Duration::from_secs(4));
        assert_eq!(
            std::fs::read_to_string(directory.path().join("receipt"))
                .unwrap()
                .lines()
                .count(),
            1
        );
    }
    #[test]
    fn deadline_keeps_original_counts_and_truncation_for_both_streams() {
        let directory = tempfile::tempdir().unwrap();
        let raw = vec![b'x'; 200_000];
        std::fs::write(directory.path().join("stream"), &raw).unwrap();
        #[cfg(unix)]
        let script = "cat stream; cat stream >&2; sleep 30";
        #[cfg(windows)]
        let script = "type stream & type stream 1>&2 & ping -n 30 127.0.0.1 >NUL";
        let error = run(&shell(script), directory.path(), &BTreeMap::new(), 2_000).unwrap_err();
        assert_eq!(error.code, "COMMAND_TIMED_OUT");
        assert_eq!(error.details["outcome"], "unknown");
        for stream in ["stdout", "stderr"] {
            assert_eq!(error.details[format!("{stream}Bytes")], raw.len());
            assert_eq!(error.details[format!("{stream}Truncated")], true);
            let text = error.details[stream].as_str().unwrap();
            assert!(text.starts_with("[machine-fabric: output truncated;"));
            assert!(text.len() <= OUTPUT_LIMIT);
        }
    }

    #[cfg(unix)]
    #[test]
    fn inherited_output_descriptor_does_not_hold_the_rpc_open() {
        let directory = tempfile::tempdir().unwrap();
        let started = Instant::now();
        let output = run(
            &shell("sleep 1 & echo parent"),
            directory.path(),
            &BTreeMap::new(),
            100,
        )
        .unwrap();
        assert!(output.status.success());
        assert!(started.elapsed() < Duration::from_millis(500));
    }
    #[test]
    fn output_tail_has_a_fixed_memory_bound() {
        let mut file = tempfile::tempfile().unwrap();
        use std::io::Write;
        file.write_all(&vec![b'x'; 200_000]).unwrap();
        file.write_all(b"last").unwrap();
        let output = tail(&mut file).unwrap();
        assert_eq!(output.bytes.len(), 65536);
        assert_eq!(output.original_bytes, 200_004);
        assert!(output.truncated());
        assert!(
            output
                .text()
                .starts_with("[machine-fabric: output truncated;")
        );
        assert!(output.text().ends_with("last"));
        assert!(output.text().len() <= OUTPUT_LIMIT);
    }
    #[test]
    fn exact_limit_is_complete_and_invalid_utf8_expansion_is_explicit() {
        let exact = CapturedOutput {
            bytes: vec![b'x'; OUTPUT_LIMIT],
            original_bytes: OUTPUT_LIMIT as u64,
        };
        assert!(!exact.truncated());
        assert_eq!(exact.text(), "x".repeat(OUTPUT_LIMIT));
        let binary = CapturedOutput {
            bytes: vec![0xff; OUTPUT_LIMIT],
            original_bytes: OUTPUT_LIMIT as u64,
        };
        assert!(binary.truncated());
        assert!(
            binary
                .text()
                .starts_with("[machine-fabric: output truncated;")
        );
        assert!(binary.text().len() <= OUTPUT_LIMIT);
        let small = CapturedOutput {
            bytes: "中文🙂".as_bytes().to_vec(),
            original_bytes: "中文🙂".len() as u64,
        };
        assert!(!small.truncated());
        assert_eq!(small.text(), "中文🙂");
    }
}
