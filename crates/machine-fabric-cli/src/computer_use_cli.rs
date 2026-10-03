//! Thin CLI over an Executor's pinned Pi extension. All calls use the local Controller.
use anyhow::{Result, bail};
use clap::Subcommand;
use machine_fabric_protocol::{Request, RpcError};
use machine_fabric_runtime::call_unix;
use serde_json::{Value, json};
use std::path::Path;

#[derive(Debug, Subcommand)]
pub enum ComputerUseCommand {
    /// List original plugin tool descriptions and JSON schemas.
    Tools {
        #[arg(long)]
        executor: String,
    },
    /// List the target desktop FIFO (credentials are redacted).
    Queue {
        #[arg(long)]
        executor: String,
    },
    /// Query a submitted session; only state=active permits tool calls.
    Status {
        #[arg(long)]
        executor: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        token: String,
    },
    /// Cancel a queued session or drain an active one.
    Cancel {
        #[arg(long)]
        executor: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        token: String,
    },
    /// Submit to the target desktop's durable FIFO; retain the returned token.
    Open {
        #[arg(long)]
        executor: String,
        #[arg(long)]
        owner: String,
        #[arg(long, default_value_t = 900000)]
        ttl_ms: u64,
        /// Stable per-submission key for safe retries after a lost response.
        #[arg(long)]
        request_key: Option<String>,
    },
    Renew {
        #[arg(long)]
        executor: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        token: String,
        #[arg(long, default_value_t = 900000)]
        ttl_ms: u64,
    },
    /// Invoke an original plugin tool; pass '-' to read JSON arguments from stdin.
    Call {
        #[arg(long)]
        executor: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        token: String,
        tool: String,
        #[arg(default_value = "{}")]
        arguments: String,
    },
    /// Dispose plugin state/native children, then release desktop control.
    Close {
        #[arg(long)]
        executor: String,
        #[arg(long)]
        owner: String,
        #[arg(long)]
        token: String,
    },
}
// Only expose bounded transport metadata, never extension payloads or credentials.
fn format_rpc_error(error: &RpcError) -> String {
    let mut diagnostic = serde_json::Map::new();
    for key in ["transportReason", "operationOutcome"] {
        if let Some(value) = error.details[key].as_str().filter(|v| v.len() <= 64) {
            diagnostic.insert(key.into(), json!(value));
        }
    }
    for key in ["receivedBytes", "maxResponseBytes"] {
        if let Some(value) = error.details[key].as_u64() {
            diagnostic.insert(key.into(), json!(value));
        }
    }
    let host = &error.details["hostExit"];
    if let Some(state) = host["state"]
        .as_str()
        .filter(|v| matches!(*v, "running" | "exited" | "unavailable" | "untracked"))
    {
        let mut exit = json!({"state": state});
        for key in ["exitCode", "signal"] {
            if let Some(value) = host[key].as_i64() {
                exit[key] = json!(value);
            }
        }
        diagnostic.insert("hostExit".into(), exit);
    }
    let base = format!("{}: {}", error.code, error.message);
    if diagnostic.is_empty() {
        base
    } else {
        format!("{base}; diagnostics={}", Value::Object(diagnostic))
    }
}

fn rpc(socket: &Path, action: &str, params: Value) -> Result<Value> {
    let response = call_unix(socket, &Request::new(action, params))?;
    if !response.ok {
        let error = response
            .error
            .ok_or_else(|| anyhow::anyhow!("missing RPC error"))?;
        bail!("{}", format_rpc_error(&error));
    }
    Ok(response.result.unwrap_or(Value::Null))
}
fn invoke(
    socket: &Path,
    executor: &str,
    owner: &str,
    token: &str,
    tool: &str,
    arguments: Value,
) -> Result<Value> {
    if !arguments.is_object() {
        bail!("tool arguments must be a JSON object");
    }
    rpc(
        socket,
        "executor.call",
        json!({"executorId":executor,"action":"computer-use.call",
        "params":{"_desktop":{"owner":owner,"token":token},"tool":tool,"arguments":arguments}}),
    )
}
pub fn run(socket: &Path, command: ComputerUseCommand) -> Result<()> {
    let result = match command {
        ComputerUseCommand::Queue { executor } => {
            rpc(socket, "desktop.list", json!({"executorId":executor}))
        }
        ComputerUseCommand::Status {
            executor,
            owner,
            token,
        } => rpc(
            socket,
            "desktop.get",
            json!({"executorId":executor,"owner":owner,"token":token}),
        ),
        ComputerUseCommand::Cancel {
            executor,
            owner,
            token,
        } => rpc(
            socket,
            "desktop.cancel",
            json!({"executorId":executor,"owner":owner,"token":token}),
        ),
        ComputerUseCommand::Tools { executor } => rpc(
            socket,
            "executor.call",
            json!({"executorId":executor,"action":"computer-use.tools","params":{}}),
        ),
        ComputerUseCommand::Open {
            executor,
            owner,
            ttl_ms,
            request_key,
        } => rpc(
            socket,
            "desktop.submit",
            json!({"executorId":executor,"requestKey":request_key.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),"owner":owner,"ttlMs":ttl_ms}),
        ),
        ComputerUseCommand::Renew {
            executor,
            owner,
            token,
            ttl_ms,
        } => rpc(
            socket,
            "desktop.renew",
            json!({"executorId":executor,"owner":owner,"token":token,"ttlMs":ttl_ms}),
        ),
        ComputerUseCommand::Call {
            executor,
            owner,
            token,
            tool,
            arguments,
        } => {
            let args = if arguments == "-" {
                serde_json::from_reader(std::io::stdin())?
            } else {
                serde_json::from_str(&arguments)?
            };
            invoke(socket, &executor, &owner, &token, &tool, args)
        }
        ComputerUseCommand::Close {
            executor,
            owner,
            token,
        } => rpc(
            socket,
            "desktop.finish",
            json!({"executorId":executor,"owner":owner,"token":token}),
        ),
    }?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn preserves_transport_metadata_without_private_payload() {
        let mut error = RpcError::new("COMPUTER_USE_UNAVAILABLE", "host unavailable");
        error.details = json!({"transportReason":"truncated_response",
            "operationOutcome":"unknown", "receivedBytes":12, "maxResponseBytes":16777216,
            "hostExit":{"state":"exited","exitCode":7,"signal":null,"token":"secret"},
            "payload":"private", "token":"secret"});
        let output = format_rpc_error(&error);
        assert!(output.contains("truncated_response"));
        assert!(output.contains("unknown"));
        assert!(output.contains("\"exitCode\":7"));
        assert!(!output.contains("private"));
        assert!(!output.contains("secret"));
        assert!(!output.contains("token"));
    }

    #[test]
    fn rejects_unbounded_and_malformed_diagnostics() {
        let mut error = RpcError::new("FAILED", "unchanged");
        error.details = json!({"transportReason":"x".repeat(65),
            "receivedBytes":-1,"hostExit":{"state":"secret"}});
        assert_eq!(format_rpc_error(&error), "FAILED: unchanged");
        error.details = Value::Null;
        assert_eq!(format_rpc_error(&error), "FAILED: unchanged");
    }
}
