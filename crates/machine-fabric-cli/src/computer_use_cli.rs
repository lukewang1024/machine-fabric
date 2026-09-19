//! Thin CLI over an Executor's pinned Pi extension. All calls use the local Controller.
use anyhow::{Result, bail};
use clap::Subcommand;
use machine_fabric_protocol::Request;
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
fn rpc(socket: &Path, action: &str, params: Value) -> Result<Value> {
    let response = call_unix(socket, &Request::new(action, params))?;
    if !response.ok {
        let error = response
            .error
            .ok_or_else(|| anyhow::anyhow!("missing RPC error"))?;
        bail!("{}: {}", error.code, error.message);
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
