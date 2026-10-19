# Unattended desktop requests

The destination Executor owns the durable FIFO. Controllers submit requests and
wait for a worker grant; they cannot publish availability through RPC or choose
a different provider path. All times of day are eligible when the local provider
permits unattended use. No provider is required on unconfigured nodes. Once a local provider snapshot is installed, the worker durably enforces availability for all desktop sessions, including legacy callers; caller flags cannot opt out. A missing provider after configuration remains unavailable across Executor restarts.

Submit an unattended session with `computer-use open --requires-availability`,
optionally setting `--deadline-at` to an epoch-millisecond queue deadline. Keep
its owner, token and request key. Repeated submissions with the same owner/key
return the same request. A waiting head cannot be bypassed. Cancellation and
maintenance continue to use the existing desktop controls.

`computer-use run --executor ID --owner OWNER --request-key KEY --receipt PATH
--deadline-at EPOCH_MS -- COMMAND ARG...` is a lightweight consumer: it waits
without starting a model, claims the worker grant once, then starts COMMAND on
the submitting Controller machine. Requests and arbitration remain on the worker;
Agent task ownership remains on the originating Controller. The command inherits
the caller's environment (including configured proxy variables), plus:

- `MACHINE_FABRIC_DESKTOP_EXECUTOR`
- `MACHINE_FABRIC_DESKTOP_OWNER`
- `MACHINE_FABRIC_DESKTOP_TOKEN`
- `MACHINE_FABRIC_CONTROLLER_SOCKET`

The command must adopt this session instead of opening another desktop session.
It must use the original typed Controller capabilities and existing workspace
driver/approval rules, use fresh observations, and close only its task-owned
windows after verifying saves. The runner renews the lease while the command
runs, disposes owned native helpers and releases its session on command exit.
A GUI call may still be rejected if availability changes after the grant.

The receipt is private and durable. Restarting the same invocation resumes a
queued wait. Once launching may have happened, it refuses automatic replay.
A worker-side consumer claim prevents a second receipt from consuming the same
request. Completed invocations do not run again. Lost transport responses are
not evidence that an input failed. Existing uncertain-outcome quarantine remains
in force, and availability never clears it.

## Local provider contract

The Executor reads `desktop-availability.json` beside `desktop-queue.json`.
The provider atomically replaces it; malformed, absent, future-dated or stale
snapshots deny admission. Minimal version 1 snapshot:

```json
{"schemaVersion":1,"available":true,"observedAtMs":1792400000000,"expiresAtMs":1792400001500}
```

Both observation age and remaining validity must be at most 1500 milliseconds.
The worker reconciles queued requests every 500 ms. On macOS it also listens for
`dev.machine-fabric.desktop-availability-changed` Darwin notifications (the Away Guard adapter also broadcasts distributed notifications for UI consumers);
notifications only wake reconciliation. A spoofed, duplicate or delayed event
cannot grant permission without a fresh local snapshot. Periodic reconciliation
covers missed notifications, reconnects and provider death without model calls.

The Away Guard adapter publishes only `armed`, not true OS lock, as available.
It refreshes the snapshot after guard checks and broadcasts state/availability
transitions only. It requires a fresh native monitor heartbeat, live monitor and
keepawake processes, readable unlocked session properties, and an unstopped guard.
Other states revoke availability. Missing `CGSSessionScreenIsLocked` in readable
macOS session properties is the normal unlocked representation.

Worker promotion and every protected desktop dispatch recheck the snapshot.
A gated active session reports `inputAllowed`; `active` alone is ownership, not
permission to input. Availability loss stops new desktop dispatch, preserves
ownership until completion/expiry, and prevents consumption of successors.
Inputs already dispatched cannot be undone. A later availability event never
replays previous input or clears an uncertain outcome.

Normal Linux/Windows sessions and exact-window, foreground, hit-test, credential
path, driver, approval and deployment checks are unchanged. An unattended request
without a configured live provider stays queued until canceled or expired.
