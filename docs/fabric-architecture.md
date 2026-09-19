# Machine Fabric architecture

## The ownership boundary

A node normally runs one Controller and one Executor. They have deliberately
different authority:

```text
Agent on Mac
   │ local IPC
   ▼
Mac Controller ── SSH peer ──► Linux Executor
   │ owns task record             │ owns local admission
   │ and Agent lifecycle          │ capacity, resource queue, fences
   └──────────────────────────────┴──► Windows Executor
```

The Controller on the Agent's machine owns the submitted task record, retry
decision, and task lifecycle. It can route a capability call to any registered
Executor. The target Executor—not the caller's Controller—is authoritative for
its current capacity and resource availability. Each Controller still has its
own state store; there is no shared multi-writer database or global scheduler.

When a Controller invokes a capability, it records the task and calls the
target Executor. The Executor atomically admits the request against both its
concurrency limit and the capability's resource locks. Conflicting requests
wait in a bounded FIFO-per-resource queue; independent resources may proceed
in parallel. Queue length and capacity are available through the compact
`availability` RPC and `machine-fabric context`. Queue overflow and wait
timeout are explicit, retryable errors. Executor-issued fencing counters are
persisted locally, so separate Controllers cannot accidentally grant
conflicting writes by maintaining unrelated lease counters.

The `task.submit` RPC records a task but does not schedule it. Agent execution
uses `capability.invoke`, which creates the task and waits for Executor
admission and completion. A Controller outage makes its Agent's task control
unavailable; Machine Fabric does not silently move that task to another
Controller. Session handoff is explicit and separate from failure recovery.

## One SSH transport per node pair

Each selected node pair has one persistent full-duplex `PeerConnection`:

```text
Controller A ─────► Executor B
Controller B ─────► Executor A
```

The physical SSH dialer is independent of the logical call direction. A laptop
may initiate SSH to a remote node; requests that originate at the accepting
node can travel back over that same connection. Routine task execution goes
from the task-owning Controller directly to the destination Executor; it does
not ask the destination Controller to adopt or supervise the task.

The peer process invokes `machine-fabric peer accept` over `ssh -T` and carries
newline-framed RPC messages on stdin/stdout. A hello handshake checks protocol
version, expected node identity, and the Controller/Executor role manifest.
Request IDs correlate both logical directions, while reconnect uses bounded
backoff and a monotonically increasing connection generation.

SSH supplies authentication, encryption, host identity, and reachability. The
peer runtime supplies multiplexing, health, request correlation, and reconnect.
No SSH socket forwarding is used: Unix sockets and Windows Named Pipes remain
local to their node. Large artifact bytes use the artifact-transfer path rather
than blocking control RPC frames.

## Platform boundary

The wire protocol is shared. Node-local IPC and service supervision are native
to each OS:

| Platform | Local IPC | Service supervisor | SSH |
| --- | --- | --- | --- |
| macOS | Unix socket | launchd | OpenSSH client/server |
| Linux | Unix socket | systemd user service | OpenSSH |
| Windows | Named Pipe | Windows Service | native OpenSSH Server |

Windows does not require WSL or MSYS2. WSL2 is a separate Linux node only when
Linux behavior is itself needed. A Docker acceptance container is a Linux
simulation of a node role, not a substitute for native Windows/macOS testing.

## Failure model

- A lost SSH peer makes that route unavailable; bounded reconnect restores the
  route without changing task ownership.
- An Executor restart clears in-flight admission state. Its durable fences
  survive restart; the task-owning Controller reports interrupted calls as
  failed or outcome-unknown according to the RPC result.
- A Controller restart recovers its own persisted task state. Another
  Controller does not automatically take over its Agent's tasks.
- There is no quorum, leader election, cloud coordinator, or automatic task
  migration. This is intentionally optimized for a few machines owned by one
  developer.

Queue defaults can be tuned per Executor with
`MACHINE_FABRIC_EXECUTOR_MAX_CONCURRENCY`,
`MACHINE_FABRIC_EXECUTOR_MAX_QUEUE`, and
`MACHINE_FABRIC_EXECUTOR_QUEUE_WAIT_MS`.
