//! One-shot image transfer over the existing Controller/Executor connection.
use anyhow::{Result, anyhow, bail};
use machine_fabric_protocol::Request;
use machine_fabric_runtime::{
    call_unix,
    clipboard::{ClipboardService, DEFAULT_MAX_BYTES},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::Path;
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

// Opt-in line-delimited metadata for desktop progress UI. Never emit pixels or
// estimate transfer percentages: this protocol acknowledges the whole image.
fn progress(stage: &str, bytes: Option<u64>) {
    if std::env::var("MACHINE_FABRIC_CLIPBOARD_PROGRESS").as_deref() == Ok("1") {
        eprintln!(
            "{}",
            json!({"event":"clipboard.progress","stage":stage,"bytes":bytes})
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Target {
    node_id: String,
    executor_id: Option<String>,
    ready: bool,
    reason: Option<String>,
    display: Option<String>,
}

// Bound each probe so one unreachable peer does not hang the menu indefinitely.
fn rpc(socket: &Path, action: &str, params: Value, timeout: Duration) -> Result<Value> {
    let socket = socket.to_owned();
    let request = Request::new(action, params);
    let (sender, receiver) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = sender.send(call_unix(socket, &request));
    });
    let response = receiver
        .recv_timeout(timeout)
        .map_err(|_| anyhow!("RPC_TIMEOUT: no response; operation outcome is unknown"))??;
    if !response.ok {
        let error = response
            .error
            .ok_or_else(|| anyhow!("RPC_FAILED: missing error"))?;
        bail!("{}: {}", error.code, error.message);
    }
    Ok(response.result.unwrap_or(Value::Null))
}
fn executor_call(
    socket: &Path,
    id: &str,
    action: &str,
    params: Value,
    timeout: Duration,
) -> Result<Value> {
    rpc(
        socket,
        "executor.call",
        json!({"executorId":id,"action":action,"params":params}),
        timeout,
    )
}

// Peer Controller and Executor proxies share a directory. Do not guess node IDs
// from '-rust' / '-native' suffixes or expose command-based product adapters.
fn same_peer(controller: &Value, executor: &Value) -> bool {
    if controller["transport"] != "local" || executor["transport"] != "local" {
        return false;
    }
    match (controller["socket"].as_str(), executor["socket"].as_str()) {
        (Some(a), Some(b)) => Path::new(a).parent() == Path::new(b).parent(),
        _ => false,
    }
}
fn candidates(snapshot: &Value) -> Vec<Target> {
    let mut targets = Vec::new();
    for controller in snapshot["controllers"].as_array().into_iter().flatten() {
        if controller["health"] != "ready" {
            continue;
        }
        let Some(node_id) = controller["id"]
            .as_str()
            .or_else(|| controller["metadata"]["id"].as_str())
        else {
            continue;
        };
        let executors: Vec<_> = snapshot["executors"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|e| {
                e["health"] == "ready" && same_peer(&controller["endpoint"], &e["endpoint"])
            })
            .collect();
        let executor_id = if executors.len() == 1 {
            executors[0]["id"]
                .as_str()
                .or_else(|| executors[0]["metadata"]["id"].as_str())
                .map(str::to_owned)
        } else {
            None
        };
        targets.push(Target {
            node_id: node_id.into(),
            executor_id,
            ready: false,
            reason: Some(
                if executors.len() > 1 {
                    "AMBIGUOUS_EXECUTOR"
                } else {
                    "EXECUTOR_UNAVAILABLE"
                }
                .into(),
            ),
            display: None,
        });
    }
    targets.sort_by(|a, b| a.node_id.cmp(&b.node_id));
    targets
}
fn discover(socket: &Path) -> Result<Vec<Target>> {
    discover_for(socket, None, Duration::from_secs(3))
}
fn discover_for(socket: &Path, requested: Option<&str>, timeout: Duration) -> Result<Vec<Target>> {
    let snapshot = rpc(socket, "status", json!({}), timeout)?;
    let targets = candidates(&snapshot).into_iter().filter(|target| {
        requested.is_none_or(|id| target.node_id == id || target.executor_id.as_deref() == Some(id))
    });
    // Probe only metadata/backend readiness, never clipboard contents.
    let handles: Vec<_> = targets.map(|mut target| {
        let socket=socket.to_owned();
        std::thread::spawn(move || {
            if let Some(id)=target.executor_id.as_deref() {
                let probe=executor_call(&socket,id,"capability.list",json!({}),timeout).and_then(|caps| {
                    let has = |name| caps.as_array().is_some_and(|v| v.iter().any(|c| c["name"]==name));
                    if !has("clipboard.status") || !has("clipboard.write") { bail!("CLIPBOARD_UNSUPPORTED: install image clipboard support on this node"); }
                    executor_call(&socket,id,"clipboard.status",json!({}),timeout)
                });
                match probe {
                    Ok(status) if status["ready"]==true => { target.ready=true; target.reason=None;
                        target.display=status["display"].as_str().map(str::to_owned); }
                    Ok(_) => target.reason=Some("CLIPBOARD_UNAVAILABLE".into()),
                    Err(error) => target.reason=Some(error.to_string()),
                }
            }
            target
        })
    }).collect();
    handles
        .into_iter()
        .map(|handle| handle.join().map_err(|_| anyhow!("target probe failed")))
        .collect()
}
fn resolve<'a>(targets: &'a [Target], requested: &str) -> Result<&'a Target> {
    let matches: Vec<_> = targets
        .iter()
        .filter(|t| t.node_id == requested || t.executor_id.as_deref() == Some(requested))
        .collect();
    if matches.len() != 1 {
        bail!("TARGET_UNAVAILABLE: target is not uniquely connected: {requested}");
    }
    let target = matches[0];
    if !target.ready {
        bail!(
            "{}",
            target.reason.as_deref().unwrap_or("CLIPBOARD_UNAVAILABLE")
        );
    }
    Ok(target)
}
fn report(result: Result<Value>, as_json: bool) -> Result<()> {
    match result {
        Ok(value) => {
            if as_json {
                println!("{}", json!({"ok":true,"result":value}));
            } else {
                println!("{}", serde_json::to_string_pretty(&value)?);
            }
            Ok(())
        }
        Err(error) => {
            if as_json {
                let message = error.to_string();
                let code = message
                    .split_once(':')
                    .map(|(code, _)| code)
                    .filter(|c| c.chars().all(|ch| ch.is_ascii_uppercase() || ch == '_'))
                    .unwrap_or("CLIPBOARD_FAILED");
                println!(
                    "{}",
                    json!({"ok":false,"error":{"code":code,"message":message}})
                );
            }
            Err(error)
        }
    }
}
pub fn targets(socket: &Path, as_json: bool) -> Result<()> {
    report(
        discover(socket).map(|targets| json!({"targets":targets})),
        as_json,
    )
}
pub fn push(socket: &Path, requested: &str, as_json: bool) -> Result<()> {
    report(push_image(socket, requested), as_json)
}
fn preflight_error(error: anyhow::Error) -> anyhow::Error {
    if error.to_string().contains("RPC_TIMEOUT") {
        anyhow!("TARGET_CHECK_TIMEOUT: target check timed out; image was not sent")
    } else {
        error
    }
}
fn push_image(socket: &Path, requested: &str) -> Result<Value> {
    progress("reading", None);
    // Snapshot at invocation, before network probes: another copy while discovery
    // is in flight must not change which image the user asked to send.
    // Local native read occurs on the invoking thread (NSPasteboard on macOS).
    // No local Executor or Hammerspoon clipboard API is needed.
    let image = ClipboardService::default()
        .read(DEFAULT_MAX_BYTES)
        .map_err(|e| anyhow!("{}: {}", e.code, e.message))?;
    let bytes = image["size"].as_u64();
    progress("checking", bytes);
    // Explicit sends tolerate shared-connection traffic and probe only the
    // selected destination. Menu refreshes retain their short read-only budget.
    let targets =
        discover_for(socket, Some(requested), Duration::from_secs(15)).map_err(preflight_error)?;
    let target = resolve(&targets, requested).map_err(preflight_error)?;
    let id = target
        .executor_id
        .as_deref()
        .ok_or_else(|| anyhow!("EXECUTOR_UNAVAILABLE"))?;
    let resource = format!("clipboard:{id}");
    let owner = format!("clipboard-{}", uuid::Uuid::new_v4());
    progress("preparing", bytes);
    let lease = rpc(
        socket,
        "lease.acquire",
        json!({"resource":resource,"owner":owner,"ttlMs":45000}),
        Duration::from_secs(3),
    )?;
    let result = transfer(target, image, |id, params| {
        progress("transferring", bytes);
        rpc(
            socket,
            "executor.call",
            json!({"executorId":id,"action":"clipboard.write","params":params,
            "leaseResource":resource,"owner":owner,"token":lease["token"]}),
            Duration::from_secs(35),
        )
    });
    if result.is_ok() {
        progress("confirmed", bytes);
    }
    // Releasing a lease never retries the image write. If release fails it expires.
    let _ = rpc(
        socket,
        "lease.release",
        json!({"resource":resource,"owner":owner,"token":lease["token"]}),
        Duration::from_secs(3),
    );
    result
}
fn transfer(
    target: &Target,
    image: Value,
    mut send: impl FnMut(&str, Value) -> Result<Value>,
) -> Result<Value> {
    if image["content"]["kind"] != "image" {
        bail!("NO_IMAGE: clipboard contains no image");
    }
    let expires = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64 + 30_000;
    let response = send(
        target
            .executor_id
            .as_deref()
            .ok_or_else(|| anyhow!("EXECUTOR_UNAVAILABLE"))?,
        json!({"executorId":target.executor_id,"content":image["content"],"semanticDigest":image["semanticDigest"],"maxBytes":DEFAULT_MAX_BYTES,"expiresAtMs":expires}),
    )?;
    if response["applied"] != true || response["semanticDigest"] != image["semanticDigest"] {
        bail!("CLIPBOARD_UNCONFIRMED: destination did not confirm the image write");
    }
    // Never print pixel data, including in JSON mode.
    Ok(
        json!({"target":target.node_id,"executorId":target.executor_id,"width":image["content"]["width"],
        "height":image["content"]["height"],"bytes":image["size"],"applied":true,"display":target.display}),
    )
}

/// The installer owns this two-line configuration. Parse as data, never eval a shell file.
pub fn exec(args: &[String]) -> Result<()> {
    let config_root = std::env::var_os("XDG_CONFIG_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".config"))
        })
        .ok_or_else(|| anyhow!("HOME or XDG_CONFIG_HOME is required"))?;
    let mut config = config_root.join("machine-fabric/clipboard-env");
    if !config.is_file() {
        let legacy = config_root.join("distributed-workbench/clipboard-env");
        if legacy.is_file() {
            config = legacy;
        }
    }
    let text = std::fs::read_to_string(&config).map_err(|_| {
        anyhow!(
            "CLIPBOARD_UNAVAILABLE: managed display is not configured ({})",
            config.display()
        )
    })?;
    let mut command = std::process::Command::new(&args[0]);
    command.args(&args[1..]);
    let mut display = false;
    let mut authority = false;
    for line in text.lines() {
        if let Some((key, value)) = line.split_once('=') {
            match key {
                "DISPLAY" => {
                    display = true;
                    command.env(key, value);
                }
                "XAUTHORITY" => {
                    authority = true;
                    command.env(key, value);
                }
                _ => bail!("invalid managed clipboard environment"),
            }
        }
    }
    if !display || !authority {
        bail!("managed clipboard environment is incomplete");
    }
    command.env_remove("WAYLAND_DISPLAY");
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        Err(command.exec().into())
    }
    #[cfg(not(unix))]
    {
        std::process::exit(command.status()?.code().unwrap_or(1));
    }
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    fn snapshot() -> Value {
        json!({"controllers":[
            {"metadata":{"id":"dev"},"health":"ready","endpoint":{"transport":"local","socket":"/state/peers/dev/controller.sock"}},
            {"metadata":{"id":"offline"},"health":"offline","endpoint":{"transport":"local","socket":"/state/peers/offline/controller.sock"}}
        ],"executors":[
            {"metadata":{"id":"arbitrary-executor-name"},"health":"ready","endpoint":{"transport":"local","socket":"/state/peers/dev/executor.sock"}},
            {"metadata":{"id":"local"},"health":"ready","endpoint":{"transport":"local","socket":"/state/executor.sock"}},
            {"metadata":{"id":"adapter"},"health":"ready","endpoint":{"transport":"command","executable":"adapter"}}
        ]})
    }
    fn target() -> Target {
        Target {
            node_id: "dev".into(),
            executor_id: Some("arbitrary-executor-name".into()),
            ready: true,
            reason: None,
            display: Some(":98".into()),
        }
    }
    #[test]
    fn discovers_only_connected_nodes_without_id_suffix_assumptions() {
        let targets = candidates(&snapshot());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].node_id, "dev");
        assert_eq!(
            targets[0].executor_id.as_deref(),
            Some("arbitrary-executor-name")
        );
        assert!(!targets[0].ready); // an actual native backend probe is required
    }
    #[test]
    fn compact_controller_snapshot_maps_node_and_executor_ids() {
        let mut state = snapshot();
        for key in ["controllers", "executors"] {
            for item in state[key].as_array_mut().unwrap() {
                item["id"] = item["metadata"]["id"].clone();
                item.as_object_mut().unwrap().remove("metadata");
            }
        }
        let targets = candidates(&state);
        assert_eq!(targets[0].node_id, "dev");
        assert_eq!(
            targets[0].executor_id.as_deref(),
            Some("arbitrary-executor-name")
        );
    }

    #[test]
    fn rejects_disconnected_or_ambiguous_targets_instead_of_falling_back() {
        assert!(resolve(&[], "dev").is_err());
        assert!(resolve(&[target(), target()], "dev").is_err());
        assert!(resolve(&[target()], "another-node").is_err());
        let mut state = snapshot();
        state["executors"][0]["health"] = json!("offline");
        assert!(candidates(&state)[0].executor_id.is_none());
    }
    #[test]
    fn no_text_no_retry_and_success_requires_matching_ack() {
        let mut calls = 0;
        assert!(
            transfer(&target(), json!({"content":{"kind":"text"}}), |_, _| {
                calls += 1;
                Ok(Value::Null)
            })
            .is_err()
        );
        assert_eq!(calls, 0);
        let image = json!({"content":{"kind":"image","width":1,"height":1,"rgbaBase64":"private"},"semanticDigest":"abc","size":4});
        assert!(
            transfer(&target(), image.clone(), |_, _| {
                calls += 1;
                bail!("offline")
            })
            .is_err()
        );
        assert_eq!(calls, 1);
        assert!(
            transfer(&target(), image.clone(), |_, _| Ok(
                json!({"applied":true,"semanticDigest":"wrong"})
            ))
            .is_err()
        );
        let result = transfer(&target(), image, |_, request| {
            assert!(request["expiresAtMs"].is_u64());
            assert_eq!(request["executorId"], "arbitrary-executor-name");
            Ok(json!({"applied":true,"semanticDigest":"abc"}))
        })
        .unwrap();
        assert_eq!(result["target"], "dev");
        assert!(!result.to_string().contains("private"));
    }
}
