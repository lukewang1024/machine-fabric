# Machine Fabric

Machine Fabric connects a small set of machines so an Agent can use their
local operating systems, tools, and compute as one working fabric. It is a
node-to-node runtime, not a cluster scheduler.

Every machine normally runs a Controller and an Executor:

- An Agent talks to the Controller on its own machine. That Controller owns
  the Agent's task records, retries, and task lifecycle—even when work runs
  elsewhere.
- The target Executor is authoritative for that machine's availability,
  bounded admission queue, per-resource serialization, and execution fences.
  Controllers route requests; they do not reserve remote resources in their
  own local lease tables.
- SSH authenticates and connects peers. One persistent framed connection per
  node pair carries calls to remote Executors in either logical direction.

Local RPC requests and responses use 64 KiB write buffers, so large Computer
Use outlines do not issue a socket write for every JSON token. Frames remain
newline-delimited JSON; a failed write or flush discards pending buffer bytes
without retrying or replaying the operation.

If a Controller goes down, task control for Agents that depend on that
Controller is unavailable; tasks are not silently adopted by another node.
Executor admission remains node-local and does not require a central service.

## Agent entry points

Use the compact context first, then ask for one capability contract only when
needed. The full schemas are deliberately not repeated in the context:

```sh
machine-fabric context
machine-fabric context --capability command.run
machine-fabric --socket "$XDG_STATE_HOME/machine-fabric/controller.sock" \
  call capability.describe '{"executorId":"linux-build","capability":"command.run"}'
```

`context` reports which Executors are reachable, their current active/queued
work, and supported capability names. Filter by Executor, capability, or
resource to keep the response small. `capability.describe` returns the
selected input/output contract. The normal `capability.invoke` API creates a
task on the local Controller and routes the call to the chosen Executor.

For disk-pressure diagnosis, discover `filesystem.capacity` and supply an
existing file or directory in the Executor's allowed roots. It returns
`availableBytes`, `freeBytes`, and `totalBytes` from native filesystem
statistics without starting a process or writing an execution fence. Available
bytes describe space accessible to the Executor user at collection time;
they do not reserve space for an installation. This read remains available
when persistence errors reject mutating operations. It does not permit those
operations to bypass their durable-write gate.

If an availability probe fails, `availability.probe` supplies a bounded
`errorCode` and `scope: executor-route`. Missing local endpoints, unavailable
connections, closed connections, timeouts and malformed replies are distinct;
valid remote RPC error codes are preserved. Raw transport errors, command
arguments and error payloads are omitted. `health: offline` describes this
Executor route, not proof that the physical machine or its desktop is off.
An Executor that responds with `available: false` because capacity is full
remains reachable; capacity exhaustion is not reported as a transport failure.

## A common three-machine arrangement

`examples/mac-linux-windows.yaml` describes a Mac Agent node, a Linux build
node, and a Windows verification node. For example, the Agent can develop on
the Mac, run Linux-specific builds on Linux, then run Windows compatibility
checks on Windows. All three are ordinary peers; the manifest does not assign
product workflows or Agent skills to a particular machine.

The runtime uses native platform services in production: launchd on macOS,
systemd user services on Linux, Windows Services on Windows, and
`termux-services` on Android aarch64. SSH aliases and keys remain user-managed
through the normal SSH configuration.

## Desktop Executor paths

`machine-fabric computer-use queue --executor <id>` lists current sessions and
history counts without returning completed sessions. Add `--history` when
investigating past sessions. Active, queued, draining, and unknown states remain
visible; blocked, in-flight, and maintenance status are reported in either mode.
This changes only the response, not durable history or desktop admission.
Raw `desktop.list` retains its full-history default for existing callers; pass
`includeTerminal: false` for the compact response. Session tokens remain redacted.
`desktop.maintenance` returns the compact current-state receipt, including
maintenance ownership, safe-point status and history counts. Read `desktop.list`
explicitly when audit history is needed; maintenance does not remove past jobs.

One-shot command stdout and stderr retain at most 64 KiB each. When clipped,
the returned text starts with an explicit truncation notice; `stdoutTruncated`
and `stderrTruncated` indicate incomplete output, and `stdoutBytes` and
`stderrBytes` report the measured original byte counts. These fields also
appear in failed-command and deadline error details. Never infer that a process,
window or error is absent from truncated output. Filter the inventory at its
source or query the exact identity, then verify lifecycle changes independently.

The macOS and Windows installers start their Executors with `--path-policy
desktop` and an explicit `--policy-home`. Direct file reads and application
paths are open by default. The read deny list covers common credential
directories and sensitive OS data, while leaving application locations such as
`/Applications`, `/System/Applications`, and `Program Files` readable. A read
deny also overrides a write allow. Directory listings and searches omit denied
children; artifact tree operations refuse a tree containing one.

Writes are admitted only below the selected user's `Code`, `Workspace`,
`Downloads`, `Documents`, `Desktop`, `Pictures`, `.cache`, `.config`,
`.local/state`, and `.local/share`, plus absolute XDG cache, state, and config
roots beneath that user's home when set. macOS also admits user `Applications`, `Library/Caches`,
`Library/Application Support`, `Library/Preferences`, and `Library/Logs`; Windows also admits
user `AppData/Local` and `AppData/Roaming`. The Windows installer can resolve
`-PolicyUser` to that account's profile instead of using the installer's
profile. `allowRoots` remains the legacy Linux path gate and provides an
internal relay root; it does not expand the desktop write allow list.

Administrators may explicitly register relocated desktop directories with the
repeatable Executor startup option `--managed-path-mapping LOGICAL=PHYSICAL`.
The Windows installers accept the corresponding `-ManagedPathMapping` string
array. Both directories must exist and resolve to the same location, and the
logical directory must be inside an existing desktop write root. Overlapping
registrations are rejected. Configuration is verified before serving RPCs;
`status.pathPolicy.managedPathMappings` reports the active registrations.
This is startup configuration, not an RPC permission grant. Do not infer a
trusted mapping from a junction's presence or from `allowRoots`.
The installer validates the candidate policy before stopping services and
persists the intended mappings in an administrator-owned configuration beside
the installed binary. Upgrades inherit that configuration when the mapping
parameter is omitted; an explicitly empty array clears it. Redirected
configuration, or configuration writable by non-administrators, is rejected. The standalone
`executor validate-path-policy` command performs the same policy validation
without starting services or creating runtime state.

Windows upgrades can use `-SideBySide` on either installer when unrelated
peer processes still hold the legacy executable. This opt-in mode requires
an existing installation, verifies a candidate in an immutable SHA-256
directory before stopping services, and points the Controller and Executor
services at that candidate. Existing identical candidates are reused without
overwriting an open file; mismatched files or redirects are rejected.
Only processes captured as belonging to these two services are awaited.
Other peer processes and their legacy executable are retained. The installer
returns the active service binary path and records it, its digest, and retained
process IDs in `installation.json` beside the legacy binary. The legacy CLI
and peer executable are not upgraded in this mode; their owners must refresh
their launch paths separately. Without this option, a foreign holder causes
failure before services are stopped rather than an attempted overwrite.

Writes through a registration must still resolve to the exact registered
target plus the logical suffix. A changed mapping or an additional redirect
inside it is rejected; credential deny rules apply to logical and resolved
paths. Unregistered paths retain the existing physical write-root checks.
Desktop path normalization collapses parent components without crossing a
filesystem root and rejects Windows device paths, alternate data streams,
and ambiguous trailing spaces or dots. File operations use the checked
resolved destination rather than following the logical alias a second time.

This is admission control for Executor path arguments. Commands and launched
applications run with their normal OS privileges and are not filesystem
sandboxed by this policy. macOS TCC and Windows ACL checks still apply.

## Image clipboard transfer

Send the current Mac clipboard image to one connected machine explicitly:

```sh
machine-fabric clipboard targets --json
machine-fabric clipboard push --target devbox --image-only --json
```

The transfer accepts images only, is limited to 16 MiB of decoded RGBA pixels,
expires after 30 seconds, and succeeds only after the destination confirms the
image digest. It does not watch the clipboard, forward text, or retry writes.

On a headless Linux Executor, install with a private authenticated X11 display:

```sh
MACHINE_FABRIC_CLIPBOARD_DISPLAY=:98 scripts/install-linux-user.sh \
  target/release/machine-fabric devbox
```

Launch tools that need the managed clipboard with `machine-fabric clipboard
exec -- codex`. Existing `distributed-workbench/clipboard-env` files remain a
supported migration fallback.

## Local acceptance

The repeatable three-node Docker acceptance is local-only:

```sh
scripts/acceptance/docker-three-node.sh
```

It builds the CLI, creates an isolated Docker network and three disposable
Linux containers, connects them over test-only SSH keys, then checks routing
from two Controllers to a shared remote Executor and verifies that its
resource queue prevents overlapping writes. Container names stand in for the
Mac/Linux/Windows roles; this does **not** emulate or certify native macOS or
Windows behavior. The script removes only the containers/network it created.

By default the script builds its node image from `debian:bookworm-slim`, so
Docker Hub access is needed the first time. Offline environments can point it
at an already-built compatible image with `MACHINE_FABRIC_DOCKER_NODE_IMAGE`
and optionally select its SSH account with `MACHINE_FABRIC_DOCKER_USER` (`fabric`
by default, or `root` for minimal test images). Set `MACHINE_FABRIC_DOCKER_INIT=no`
only when using a Docker daemon without its optional init binary.

## Build and test

```sh
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release --bin machine-fabric
```

## Pinned installs

Machine Fabric releases are exact-version packages. There is no mutable
`latest` endpoint: set `MACHINE_FABRIC_RELEASE_BASE_URL` to the internal CDN
root, then pass the exact version to the installer. Each installer verifies
the archive against the release's `SHA256SUMS` before extraction.

```sh
MACHINE_FABRIC_RELEASE_BASE_URL=https://<internal-cdn-root> \
  scripts/install-from-release.sh 0.1.12
```

Termux on Android aarch64 uses the `aarch64-linux-android` release archive and
requires `termux-services` before installation:

```sh
pkg install termux-services openssh
MACHINE_FABRIC_RELEASE_BASE_URL=https://<internal-cdn-root> \
  scripts/install-from-release.sh 0.1.12
```

After the local Controller and Executor are ready, connect the phone to a
configured SSH peer with:

```sh
scripts/connect-termux-peer.sh cndevbox
```

The package is also published to its private GitHub repository for source and
release management. Runtime installation uses the internal CDN only.

The local CLI communicates with the node Controller over Unix sockets or
Windows Named Pipes. State and logs are stored under the platform's normal
XDG/application data locations.

Peer frame serialization uses a bounded 64 KiB write buffer under the existing
whole-frame lock. Each completed JSON frame retains its newline and explicit
flush. A write failure discards remaining buffered bytes without an implicit
retry; partial delivery remains unknown and must not replay an operation.

Peers negotiate independent gzip responses during the existing v1 handshake.
Only successful responses from 64 KiB through 32 MiB are eligible, and only
when the encoded frame is smaller. Each response uses a fresh dictionary;
requests remain ordinary JSON. A peer without the capability receives the
original format. Decoding checks negotiation, bounded expansion, exact length
and gzip integrity before routing the unchanged response. Invalid frames close
the connection; they never trigger a retry or relax desktop input guards.

Read-only peer health, desktop queue and managed process status requests have a
10-second response deadline. This bounds diagnostics when a connected peer
stops responding; cached registration health alone is not live confirmation.
Builds, Computer Use calls and desktop/process mutations retain their existing
completion waits. A peer response timeout does not cancel remote execution or
prove that an input was not sent. Keep the original request identity, reconcile
its effect and desktop admission state, and never automatically replay input.

Unix RPC endpoints are held by one listener generation using an exclusive,
owner-only lock file. A reconnect cannot replace a live endpoint. Peer role
listeners bind before the bridge is announced and stop before the disconnected
bridge returns. Cleanup removes only the socket inode owned by that generation;
an older generation cannot remove a replacement endpoint. Persistent `.lock`
files are intentional and must not be removed while a listener is running.
Disconnected bridges reject new requests and do not replay pending requests.
