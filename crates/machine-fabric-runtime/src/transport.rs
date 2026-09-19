use anyhow::{Context, Result, anyhow};
use machine_fabric_protocol::{Request, Response};
use machine_fabric_schema::ExecutorEndpoint;
use std::io::Write;
use std::process::{Command, Stdio};

use crate::call_unix;

pub fn call_executor(endpoint: &ExecutorEndpoint, request: &Request) -> Result<Response> {
    match endpoint {
        ExecutorEndpoint::Local { socket } => call_unix(socket, request),
        ExecutorEndpoint::Ssh {
            host,
            socket,
            control_path,
        } => {
            let mut command = Command::new("ssh");
            command.args(["-o", "ClearAllForwardings=yes", "-o", "BatchMode=yes"]);
            if let Some(path) = control_path {
                command.args(["-S", path]);
            }
            let remote_command = format!(
                ".local/bin/machine-fabric --socket '{}' call '{}' '{}'",
                shell_single_quote(socket),
                shell_single_quote(&request.action),
                shell_single_quote(&serde_json::to_string(&request.params)?),
            );
            command.args([host, &remote_command]);
            let output = command
                .stdin(Stdio::null())
                .output()
                .with_context(|| format!("invoke executor through ssh host {host}"))?;
            if !output.status.success() {
                return Err(anyhow!(
                    "ssh executor failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ));
            }
            serde_json::from_slice(&output.stdout).context("decode ssh executor response")
        }
        ExecutorEndpoint::Command {
            executable,
            args,
            cwd,
        } => {
            let mut command = Command::new(executable);
            command
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped());
            if let Some(cwd) = cwd {
                command.current_dir(cwd);
            }
            let mut child = command
                .spawn()
                .with_context(|| format!("start command provider {executable}"))?;
            child
                .stdin
                .take()
                .expect("provider stdin is piped")
                .write_all(&serde_json::to_vec(request)?)?;
            let output = child
                .wait_with_output()
                .with_context(|| format!("wait for command provider {executable}"))?;
            if !output.status.success() && output.stdout.is_empty() {
                return Err(anyhow!(
                    "command provider failed: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ));
            }
            serde_json::from_slice(&output.stdout).context("decode command provider response")
        }
    }
}

pub fn deploy_binary_over_ssh(host: &str, bytes: &[u8], target: &str) -> Result<()> {
    let temporary = format!("{target}.fabric.tmp");
    let mut child = Command::new("ssh")
        .args([
            "-o",
            "ClearAllForwardings=yes",
            "-o",
            "BatchMode=yes",
            host,
            "sh",
            "-c",
            &format!(
                "umask 077 && mkdir -p \"$(dirname '{}')\" && dd of='{}' status=none && chmod 755 '{}' && mv '{}' '{}'",
                shell_single_quote(target),
                shell_single_quote(&temporary),
                shell_single_quote(&temporary),
                shell_single_quote(&temporary),
                shell_single_quote(target),
            ),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .spawn()
        .with_context(|| format!("deploy fabric binary to {host}"))?;
    child.stdin.take().expect("piped stdin").write_all(bytes)?;
    let status = child.wait()?;
    if !status.success() {
        return Err(anyhow!("remote binary deployment failed"));
    }
    Ok(())
}

fn shell_single_quote(value: &str) -> String {
    value.replace('\'', "'\\''")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use serde_json::json;

    #[test]
    #[cfg(unix)]
    fn command_provider_uses_json_request_response_protocol() {
        let endpoint = ExecutorEndpoint::Command {
            executable: "/bin/sh".to_owned(),
            args: vec![
                "-c".to_owned(),
                "read request; printf '%s\\n' '{\"ok\":true,\"apiVersion\":\"machine-fabric.dev/v1\",\"requestId\":\"provider\",\"result\":{\"status\":\"ready\"}}'".to_owned(),
            ],
            cwd: None,
        };
        let response = call_executor(&endpoint, &Request::new("status", json!({}))).unwrap();
        assert!(response.ok);
        assert_eq!(response.result.unwrap()["status"], "ready");
    }

    #[test]
    fn ssh_arguments_are_safe_inside_single_quotes() {
        assert_eq!(shell_single_quote("a'b"), "a'\\''b");
    }
}
