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
systemd user services on Linux, and Windows Services on Windows. SSH aliases
and keys remain user-managed through the normal SSH configuration.

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
  scripts/install-from-release.sh 0.1.3
```

The package is also published to its private GitHub repository for source and
release management. Runtime installation uses the internal CDN only.

The local CLI communicates with the node Controller over Unix sockets or
Windows Named Pipes. State and logs are stored under the platform's normal
XDG/application data locations.
