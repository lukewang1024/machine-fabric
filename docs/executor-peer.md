# Embedding duplex channels in executor daemons

`machine-fabric-peer` provides bounded duplex request/response channels without
a Fabric Controller, task store, or Fabric Executor. Either endpoint can initiate
requests over one authenticated byte stream. The existing Fabric runtime uses the
same core through its legacy v1 adapter.

There are two integration paths:

| Consumer | Entry point | Owned by the consumer |
| --- | --- | --- |
| Rust executor | `machine_fabric_peer::{PeerConnection, ManagedSshPeer}` | Authenticated IO, handlers, authorization, lease |
| Executor in another language | Supervised `machine-fabric peer channel-connect/channel-accept` process | Private config, local IPC handlers, process supervisor |

The Coding Agent sees the executor's authorized tool API. SSH endpoints, keys,
channel config and management IPC stay with the trusted executor. This requires
an actual OS isolation boundary: a service account or sandbox that prevents the
Agent from reading daemon files, signaling its processes or connecting to its
private sockets. Hiding CLI flags cannot isolate two processes with the same
effective user and filesystem access.

## Ownership and lifecycle

The platform remains authoritative for allocation, identity, lease, node epochs,
allowed actions and execution receipts. Fabric compares the expected node ID,
offered services and the complete `ChannelBinding` during the v2 handshake; that
comparison is not authentication or a platform authorization decision.

1. The platform grants an allocation and prepares connectivity. Both supervisors
   receive the same canonical binding, including `leaseId`, allocation, attempt,
   source/target node IDs and epochs, and controlling identity.
2. The target supervisor provisions its fixed peer entrypoint and handler. The
   source supervisor starts the authenticated transport. SSH provisioning,
   temporary ports, firewall rules, keys/certificates and Relay remain platform
   responsibilities.
3. Each inbound request passes an authorizer. The target executor additionally
   checks its live allocation, tool permissions and desktop ownership before
   dispatch, and records side effects in its durable execution journal.
4. Both supervisors receive lease renewal/revocation through an independent
   platform management path. A failed peer data connection must not prevent
   revocation. Renewal must reach both endpoints; it is not relayed implicitly.
5. Expiry/revocation closes the channel, cancels in-flight requests and stops
   local reconnect. Closing a channel is not evidence that GUI work stopped.
   The target executor fences further execution and separately confirms cleanup
   of task-owned processes, documents, windows and temporary credentials.

`Lease` is in-memory and terminal for its lifetime. The supervisor must persist
the current grant and prevent any new process from loading a revoked or stale
config. A restarted accepting process does not inherit another process's
in-memory revocation or renewal. Update persisted config before relying on it
for a future SSH connection. A new allocation/attempt/epoch needs a new binding
and channel; `renew` only extends the same grant's expiration.

## Rust integration

Add `machine-fabric-peer` from the approved source revision (a Cargo path or a
pinned Git revision). Its only Fabric dependency is `machine-fabric-protocol`.
It does not require the runtime crate or initialize any global service.

Run a complete local example from a source checkout:

```sh
cargo run -p machine-fabric-peer --example duplex
```

[The example](../crates/machine-fabric-peer/examples/duplex.rs) creates two
independent peers, calls in both directions, revokes their lease and verifies
draining. Its loopback TCP streams are deliberately unauthenticated test IO.
In production:

- Use `Transport::new(reader, writer, shutdown)` with authenticated, encrypted
  IO. `shutdown` must promptly interrupt **both** IO halves. This boundary also
  permits a platform Relay adapter without changing RPC semantics.
- Use `PeerProtocol::ScopedV2`, trusted `PeerConfig`, an `Authorizer` for each
  inbound request, and a `Handler`. `RequestContext` carries peer ID, service,
  binding and cooperative cancellation.
- Call `PeerConnection::start`, then `wait_ready`. `call`/`call_timeout` can run
  concurrently from either side. Business actions and schemas belong to the
  executor.
- For an executor-owned SSH child, use `ManagedSshPeer::start` with
  `SshEndpoint` and `ReconnectPolicy`. It reconnects channels while the lease is
  live, never requests. Old handlers/IO must drain before reconnect; otherwise
  it stops with `drain-incomplete`.
- On shutdown, revoke/close and inspect `wait_drained` or `wait_stopped`.
  A noncooperative handler can outlive channel closure; do not mark the desktop
  reusable until the executor has confirmed actual cleanup.

Handlers should observe cancellation and must not indefinitely block on a
synchronous request back to the same saturated handler pool. Separate control
from long-running work and size the bounded concurrency for nested calls.

## Supervised process integration

The standalone commands do not start a Fabric Controller or register Fabric
tasks. The source daemon owns `channel-connect`; the target's fixed SSH
entrypoint runs `channel-accept`. Both provide private local request/control
endpoints and call a handler owned by their respective executor.

```text
Agent -> authorized executor tool API -> requestSocket
                                        peer process <== SSH ==> peer process
                                                                 handlerSocket -> CU executor
                 supervisor -> controlSocket              controlSocket <- supervisor
```

Daemon-owned config example on the source:

```json
{
  "apiVersion": "machine-fabric.peer-channel/v1",
  "localId": "cloudide-01",
  "peerId": "windows-01",
  "services": ["executor.events"],
  "requiredPeerServices": ["executor.tools"],
  "binding": {
    "leaseId": "lease-123",
    "scope": {
      "allocationId": "allocation-123",
      "attempt": 1,
      "sourceNodeId": "cloudide-01",
      "targetNodeId": "windows-01",
      "sourceEpoch": 7,
      "targetEpoch": 2,
      "controllerId": "owner-123"
    }
  },
  "expiresAtMs": 0,
  "handlerSocket": "/run/executor/lease-123/events.sock",
  "requestSocket": "/run/executor/lease-123/requests.sock",
  "controlSocket": "/run/executor/lease-123/control.sock",
  "allowedInboundActions": ["executor.event"],
  "allowedOutboundActions": ["cu.observe", "cu.click", "execution.get"],
  "ssh": {
    "host": "192.0.2.10",
    "port": 22022,
    "username": "executor-service",
    "identityFile": "/run/executor/lease-123/identity",
    "knownHostsFile": "/run/executor/lease-123/known_hosts",
    "remoteCommand": "machine-fabric peer channel-accept --config C:/ProgramData/Executor/lease-123/peer.json",
    "connectTimeoutSeconds": 10
  }
}
```

Replace the example address, paths, binding and **zero expiry** with the actual
platform grant. The zero is intentionally invalid. These action names illustrate
an executor contract; Fabric does not implement CU tools or a journal.

On Windows, the accepting config has no `ssh` field, swaps `localId`/`peerId`,
offers `["executor.tools"]`, requires `["executor.events"]`, and swaps inbound
and outbound action lists. Keep the entire `binding` identical, including its
source/target orientation. Set all three paths to distinct absolute Windows
paths, e.g. `C:/ProgramData/Executor/lease-123/requests.sock`. Use the same path
spelling in every client.

Start the executor's handler first, then the private peer process:

```sh
machine-fabric peer channel-connect --config /run/executor/lease-123/peer.json
```

Provision the target SSH key/account with a fixed `channel-accept` command,
no shell, no port/agent forwarding and no PTY. The command must use a fully
qualified trusted binary/config path in production. `remoteCommand` is an
administrator-owned value and is interpreted by the remote SSH environment;
never interpolate Agent input. The SSH client ignores user SSH config,
requires a pinned host key and public-key authentication, disables forwarding,
agent/password/interactive fallback and connection multiplexing. Its optional
`executable` must also be supervisor-controlled. It inherits the launch
environment; propagate the machine's proxy settings when applicable.

Unix config must be owned by the service user with no group/other permission
bits (normally `0600`). Put config, keys and sockets under a private `0700`
directory. Unix IPC endpoints are `0600`. Windows installers must restrict
config/key/directory ACLs to the service identity. The new adapter's named-pipe
DACL grants owner rights, LocalSystem and administrators, not all authenticated
users. Privileged processes remain part of the trusted host boundary.

### Local IPC contract

Each local connection exchanges one UTF-8 JSON `Request` followed by newline,
then one `Response` followed by newline. The envelope version is
`machine-fabric.dev/v1`, independently of channel protocol v2 and config v1.
`requestId` identifies an RPC; use a separate durable business `commandId`
for execution deduplication.

On Unix, connect to the configured filesystem socket. On Windows, the runtime
maps the configured path to
`\\.\pipe\machine-fabric-<sha256(lowercase(path-as-UTF-8))>`. The lowercase
operation is ASCII only. A non-Rust executor must use the same mapping for
its handler pipe. The path is an IPC name seed, not a socket file on Windows.

An outbound call to `requestSocket`:

```json
{"apiVersion":"machine-fabric.dev/v1","requestId":"local-1","action":"peer.call","params":{"service":"executor.tools","request":{"apiVersion":"machine-fabric.dev/v1","requestId":"rpc-1","action":"cu.observe","params":{"commandId":"command-1"}}}}
```

The outer response preserves `local-1`; its `result` is the **complete remote
Response**, including `rpc-1` and its own `ok`/`error`. Check both `ok` values.
This endpoint accepts only `peer.call` and enforces the configured outbound
action list. The remote endpoint independently enforces its inbound action list.

The adapter calls `handlerSocket` with a trusted dispatch envelope:

```json
{"apiVersion":"machine-fabric.dev/v1","requestId":"rpc-1","action":"peer.dispatch","params":{"peerId":"cloudide-01","service":"executor.tools","binding":{"leaseId":"lease-123","scope":{"allocationId":"allocation-123","attempt":1,"sourceNodeId":"cloudide-01","targetNodeId":"windows-01","sourceEpoch":7,"targetEpoch":2,"controllerId":"owner-123"}},"request":{"apiVersion":"machine-fabric.dev/v1","requestId":"rpc-1","action":"cu.observe","params":{"commandId":"command-1"}}}}
```

Validate the live grant and tool parameters, then reply with the same request
ID, e.g. `{"apiVersion":"machine-fabric.dev/v1","requestId":"rpc-1","ok":true,"result":{...}}`.
The adapter also preserves correlation IDs. Treat `params.binding` in this
outer envelope as adapter context; the inner request's parameters cannot
replace it. A disconnect/cancellation closes the handler's IPC connection;
the executor must observe that closure or its independent lease cancellation
and manage actual tool cancellation itself.

Private `controlSocket` actions:

| Action | Params | Behavior |
| --- | --- | --- |
| `peer.status` | `{}` | Channel state, generation/close reason, lease identity/expiry |
| `peer.renew` | `{"expiresAtMs":...}` | Extend a live local lease; cannot revive one that ended |
| `peer.revoke` | `{}` | End this process's lease, close, stop reconnect |
| `peer.close` | `{}` | Close this process's channel without revoking the platform grant |

A closing process can exit before the final control ACK is received. Confirm
process exit and endpoint cleanup through the supervisor. `peer.close` on an
accepting process permits the source to reconnect using a still-valid grant.
For revocation, invalidate the grant in **both** supervisors and fence future
starts before issuing local `peer.revoke`.

An accepting adapter owns stdin/stdout for its SSH session and exits on close.
Do not embed `run_peer_channel`'s accepting-stdio mode into a long-lived host:
stdio readers rely on process exit for interruption. Rust hosts use
`PeerConnection` with an interruptible `Transport` instead.

The private adapter never overwrites an existing IPC endpoint. After an
ungraceful process death, the supervisor must verify that the previous owner
has exited before removing its stale Unix socket paths and restarting. Prefer
an allocation-specific private directory; never remove another process's
endpoint to force startup.

### Limits and failure semantics

Optional `limits`: `maxFrameBytes` (default 8 MiB, permitted 1 KiB–256 MiB),
`maxOutboundRequests` (64), `maxInboundRequests` (16), `maxQueuedFrames` (64),
`handshakeTimeoutMs` (15,000), `requestTimeoutMs` (60,000). Set compatible limits
at both endpoints. Envelopes count toward limits; reserve headroom for local
dispatch/response wrappers. Screenshots/artifacts larger than the limit should
use executor-owned artifact storage with typed references.

The control listener has its own eight-client bound and 64 KiB frames. Local
connection/read/write waits are bounded and observe closure. SSH reconnect
uses 1/2/4/5-second backoff, up to 5 seconds, while the lease remains active.

| Outcome | Meaning |
| --- | --- |
| `PEER_NOT_READY`, `PEER_BUSY` | Rejected before the requested handler was dispatched |
| `PEER_ACTION_DENIED`, `PEER_SERVICE_UNAVAILABLE` | Rejected by channel routing/grant |
| `PEER_OUTCOME_UNKNOWN` | Request may have executed; query the durable journal before retry |
| `PEER_FRAME_TOO_LARGE` | Inspect context: an oversized request was not sent; an oversized **response** may follow successful execution |
| `PEER_HANDLER_PANICKED` | Handler outcome is unknown; inspect the journal |

Queue saturation can close a connection to keep memory bounded. Timeout,
cancel, transport loss or process exit never mean a click was undone.
No layer here promises exactly-once execution, automatically retries RPCs,
or persists acknowledgements. `ManagedSshPeer` only reconstructs connections.

## Compatibility and validation

Legacy `peer connect/accept` retains `machine-fabric.peer/v1` and its fixed
controller/executor routes, now backed by the shared core. It does not gain v2
platform scope semantics. Scoped peers never silently downgrade to v1.

Negotiated independent gzip responses remain compatible with current v1 peers.
Peers without the `response_gzip` handshake capability receive ordinary JSON
responses. Decompression respects both the channel's configured frame limit and
the existing 32 MiB compression budget. Legacy diagnostic calls keep their
10-second deadline; ordinary actions keep their one-hour deadline. Local socket
ownership locks prevent an older connection from removing a newer endpoint.

```sh
cargo fmt --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release --bin machine-fabric
MACHINE_FABRIC_TEST_BINARY="$PWD/target/release/machine-fabric" \
  python3 -m unittest discover -s tests -p test_executor_peer_channel.py -v
```

The last suite requires POSIX OpenSSH client/server tools and explicitly runs
real loopback SSH with disposable keys, fixed peer entrypoints, pinned hosts
and independent executor handler processes. It covers both directions,
receiver grants, bad scope/host key, expiry, renewal, reconnection without
replay and busy-channel revocation. It does not modify existing SSH setup.

Rust tests cover protocol bounds, authorization, cancellation, old v1 wire
compatibility and runtime route cleanup. Windows cross-compilation checks
platform-specific code; native Windows service-account ACLs, OpenSSH setup,
interactive desktop sessions and CloudIDE-to-Windows CU acceptance must also
be verified by the integrating platform in its real environment.
