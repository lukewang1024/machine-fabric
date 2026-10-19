//! Cheap Controller-side consumer for the Executor-owned durable FIFO.
//! The command starts only after a worker grant. All GUI work still uses typed
//! Controller calls and the Executor rechecks availability on every dispatch.
use crate::computer_use_cli::rpc;
use anyhow::{Context, Result, bail};
use machine_fabric_core::{atomic_replace, now_ms};
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    path::Path,
    process::Command,
    thread,
    time::{Duration, Instant},
};

fn store(path: &Path, value: &Value) -> Result<()> {
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    atomic_replace(&temp, path)?;
    Ok(())
}

pub(crate) struct Run<'a> {
    pub socket: &'a Path,
    pub executor: &'a str,
    pub owner: &'a str,
    pub request_key: &'a str,
    pub receipt: &'a Path,
    pub deadline_at: u64,
    pub argv: &'a [String],
}
impl Run<'_> {
    pub(crate) fn execute(&self) -> Result<Value> {
        self.execute_with(rpc)
    }
    fn execute_with(
        &self,
        mut rpc: impl FnMut(&Path, &str, Value) -> Result<Value>,
    ) -> Result<Value> {
        if self.argv.is_empty() {
            bail!("a consumer command is required after --");
        }
        if let Some(parent) = self.receipt.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let lock = options.open(self.receipt.with_extension("lock"))?;
        lock.try_lock()
            .context("another consumer owns this receipt")?;
        let mut record = if self.receipt.exists() {
            serde_json::from_slice::<Value>(&fs::read(self.receipt)?)?
        } else {
            json!({"state":"submitting","executorId":self.executor,"owner":self.owner,
                "requestKey":self.request_key,"argv":self.argv,"deadlineAt":self.deadline_at,"consumerId":uuid::Uuid::new_v4().to_string()})
        };
        if record["executorId"] != self.executor
            || record["owner"] != self.owner
            || record["requestKey"] != self.request_key
            || record["argv"] != json!(self.argv)
            || record["deadlineAt"] != self.deadline_at
        {
            bail!("receipt does not match this request");
        }
        match record["state"].as_str() {
            Some("submitting" | "queued") => {}
            Some("completed") => {
                if record["exitCode"] != 0 {
                    bail!("previous consumer failed; automatic replay refused");
                }
                return Ok(
                    json!({"state":"completed","alreadyConsumed":true,"exitCode":record["exitCode"]}),
                );
            }
            _ => bail!(
                "consumer may already have started; inspect its receipt and process; automatic replay refused"
            ),
        }
        store(self.receipt, &record)?;
        let job = if record.get("job").is_some() {
            record["job"].clone()
        } else {
            let job = rpc(
                self.socket,
                "desktop.submit",
                json!({"executorId":self.executor,
                "owner":self.owner,"requestKey":self.request_key,"requiresAvailability":true,
                "deadlineAt":self.deadline_at,"ttlMs":900000}),
            )?;
            record["job"] = job.clone();
            record["state"] = json!("queued");
            store(self.receipt, &record)?;
            job
        };
        let credentials =
            json!({"executorId":self.executor,"owner":self.owner,"token":job["token"]});
        loop {
            if now_ms() >= self.deadline_at {
                rpc(self.socket, "desktop.cancel", credentials.clone())?;
                record["state"] = json!("expired");
                store(self.receipt, &record)?;
                bail!("desktop request expired before consumer launch");
            }
            // Read-only status failures never cause a second submit or launch.
            let status = match rpc(self.socket, "desktop.get", credentials.clone()) {
                Ok(status) => status,
                Err(_) => {
                    thread::sleep(Duration::from_secs(3));
                    continue;
                }
            };
            match status["state"].as_str() {
                Some("queued") => thread::sleep(Duration::from_secs(1)),
                Some("active") if status["inputAllowed"] == true => break,
                Some("active") => thread::sleep(Duration::from_secs(1)),
                _ => bail!("desktop request is no longer runnable: {}", status["state"]),
            }
        }
        let mut claim = credentials.clone();
        claim["consumerId"] = record["consumerId"].clone();
        rpc(self.socket, "desktop.claim", claim)?;
        // Persist before starting; a crash in this interval is unknown, never a
        // reason to run a consumer twice (even if the child PID was not saved).
        record["state"] = json!("starting");
        store(self.receipt, &record)?;
        let mut child = match Command::new(&self.argv[0])
            .args(&self.argv[1..])
            .env("MACHINE_FABRIC_DESKTOP_EXECUTOR", self.executor)
            .env("MACHINE_FABRIC_DESKTOP_OWNER", self.owner)
            .env(
                "MACHINE_FABRIC_DESKTOP_TOKEN",
                job["token"].as_str().context("missing job token")?,
            )
            .env("MACHINE_FABRIC_CONTROLLER_SOCKET", self.socket)
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                record["state"] = json!("spawn_failed");
                store(self.receipt, &record)?;
                rpc(self.socket, "desktop.finish", credentials)?;
                return Err(error.into());
            }
        };
        record["state"] = json!("running");
        record["pid"] = json!(child.id());
        store(self.receipt, &record)?;
        let mut renewed = Instant::now();
        let exit = loop {
            if let Some(exit) = child.try_wait()? {
                break exit;
            }
            // The process can compute while unavailable; native GUI dispatch is
            // revoked at the Executor and cannot be restored by this consumer.
            if renewed.elapsed() >= Duration::from_secs(30) {
                rpc(self.socket, "desktop.renew", credentials.clone())?;
                renewed = Instant::now();
            }
            thread::sleep(Duration::from_millis(250));
        };
        record["state"] = json!("exited");
        record["exitCode"] = json!(exit.code());
        store(self.receipt, &record)?;
        rpc(self.socket, "desktop.finish", credentials)?;
        record["state"] = json!("completed");
        store(self.receipt, &record)?;
        if !exit.success() {
            bail!(
                "desktop consumer failed; exitCode={:?}; receipt retained",
                exit.code()
            );
        }
        Ok(json!({"state":"completed","exitCode":exit.code()}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn consumer_fixture() {
        if std::env::var("MACHINE_FABRIC_DESKTOP_EXECUTOR").as_deref() != Ok("desktop-fixture") {
            return;
        }
        assert_eq!(
            std::env::var("MACHINE_FABRIC_DESKTOP_TOKEN").unwrap(),
            "private-test-token"
        );
        fs::write(
            std::env::var("MACHINE_FABRIC_DESKTOP_OWNER").unwrap(),
            b"consumed-once",
        )
        .unwrap();
    }
    #[test]
    fn consumer_waits_claims_once_cleans_up_and_never_replays() {
        let dir = tempfile::tempdir().unwrap();
        let receipt = dir.path().join("receipt.json");
        let output = dir.path().join("output");
        let owner = output.to_str().unwrap();
        let command = vec![
            std::env::current_exe()
                .unwrap()
                .to_string_lossy()
                .into_owned(),
            "--exact".into(),
            "desktop_run::tests::consumer_fixture".into(),
        ];
        let run = Run {
            socket: Path::new("unused"),
            executor: "desktop-fixture",
            owner,
            request_key: "one",
            receipt: &receipt,
            deadline_at: now_ms() + 60000,
            argv: &command,
        };
        let mut calls = Vec::new();
        let result = run
            .execute_with(|_, action, params| {
                calls.push(action.to_string());
                match action {
                    "desktop.submit" => {
                        assert_eq!(params["requiresAvailability"], true);
                        Ok(json!({"token":"private-test-token"}))
                    }
                    "desktop.get" => {
                        if calls.iter().filter(|a| *a == "desktop.get").count() == 1 {
                            assert!(!output.exists());
                            Ok(json!({"state":"queued"}))
                        } else {
                            Ok(json!({"state":"active","inputAllowed":true}))
                        }
                    }
                    "desktop.claim" => {
                        assert!(params["consumerId"].is_string());
                        assert!(!output.exists());
                        Ok(json!({"claimed":true}))
                    }
                    "desktop.finish" => {
                        assert_eq!(fs::read(&output).unwrap(), b"consumed-once");
                        Ok(json!({"state":"completed"}))
                    }
                    other => panic!("unexpected call: {other}"),
                }
            })
            .unwrap();
        assert_eq!(result["state"], "completed");
        assert_eq!(calls.iter().filter(|a| *a == "desktop.claim").count(), 1);
        assert_eq!(
            run.execute_with(|_, _, _| panic!("completed request must not launch again"))
                .unwrap()["alreadyConsumed"],
            true
        );
        let mut record: Value = serde_json::from_slice(&fs::read(&receipt).unwrap()).unwrap();
        record["state"] = json!("starting");
        store(&receipt, &record).unwrap();
        assert!(
            run.execute_with(|_, _, _| panic!("unknown launch must not replay"))
                .unwrap_err()
                .to_string()
                .contains("automatic replay refused")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&receipt).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
}
