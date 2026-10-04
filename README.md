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

Read-only peer health, desktop queue and managed process status requests have a
10-second response deadline. This bounds diagnostics when a connected peer
stops responding; cached registration health alone is not live confirmation.
Builds, Computer Use calls and desktop/process mutations retain their existing
completion waits. A peer response timeout does not cancel remote execution or
prove that an input was not sent. Keep the original request identity, reconcile
its effect and desktop admission state, and never automatically replay input.
