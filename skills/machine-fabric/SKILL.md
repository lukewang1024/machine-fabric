---
name: machine-fabric
description: Use a local Controller to inspect connected machine Executors, choose a capability, and run or diagnose an Agent task across a small SSH-connected machine fabric.
---

# Machine Fabric

## Agent request flow

1. Talk only to the Controller on the Agent's own machine. It owns this
   Agent's task record and lifecycle.
2. Start with `machine-fabric context`. Add `--capability NAME` to find nodes
   that support a specific capability, `--executor ID` to narrow the result,
   or `--resource KEY` to check that resource's current admission state.
3. Fetch one full contract on demand with
   `machine-fabric --socket <controller-socket> call capability.describe
   '{"executorId":"...","capability":"..."}'`. Context responses contain
   names and queue state, not the full input/output schemas.
4. Create/update a local session and acquire its driver lease, then use the
   local Controller's `capability.invoke` action with the selected remote
   Executor ID. The Controller records the task; the destination Executor
   decides when capacity and resource locks admit it.
5. If an invocation is waiting, inspect `machine-fabric context` again. A
   full queue or admission timeout is an explicit retryable error.

Do not call a remote Controller to create or supervise this task. Do not
interpret a peer connection as remote task ownership or automatic failover.
An explicit session handoff, if deliberately requested, is a separate
operation and never happens just because a node disconnects.

Command responses are bounded. Check `stdoutTruncated`/`stderrTruncated` and
the original byte counts; a truncation notice also prefixes clipped text.
Never use a partial inventory as proof that a helper/process is absent. Filter
before returning large output, query the exact PID and executable identity,
and independently confirm owned process shutdown. Keep older-version callers
safe by treating an exactly limit-sized response as potentially incomplete.

## Fabric setup and diagnosis

Use a `machine-fabric.dev/v1` manifest to validate node IDs, platform,
architecture, SSH aliases, and allowed roots:

```sh
machine-fabric manifest validate --file examples/mac-linux-windows.yaml
machine-fabric manifest plan --file examples/mac-linux-windows.yaml
```

SSH credentials and host verification remain in the user's normal SSH
configuration. Each selected node runs a local Controller and Executor; one
persistent SSH-stdio peer connection per node pair multiplexes both logical
directions. Never expose Unix sockets or Named Pipes across the network and do
not add SSH socket forwarding.

For an actual deployment, inspect the manifest plan and target identities
before installing services or changing peer topology. For repeatable local
protocol and queue checks, run `scripts/acceptance/docker-three-node.sh`. Its
Linux containers model node roles only; they are not native macOS/Windows
acceptance.

## Generic desktop control

Browser, Word and other desktop tasks use `machine-fabric computer-use` (also
`workbench computer-use` where the Workbench alias is installed). They require
no product-specific domain profile or workspace driver. Discover original SDK
schemas with `computer-use tools`, then open one FIFO session with a stable
owner/request key, await `state=active`, and use that exact owner/token for
calls and cleanup. Only fresh observation stateIds/references permit input.

Connectivity and desktop readiness are distinct. `DESKTOP_RECOVERY_REQUIRED`
records a previous uncertain native outcome, not an offline machine. Queue
status includes the last verified recovery reason. With a persisted kernel
boot identity, a different identity on the next Executor start proves a full
OS restart: the queue retires old active grants and clears quarantine without
replaying input. Missing evidence, legacy queues, and an Executor/helper restart
within the same OS boot retain quarantine. Maintenance ownership is preserved.
This automatic recovery does not prove the desktop is unlocked or grant missing
OS permissions; new actions still pass their normal readiness and input guards.

For same-boot explicit recovery, establish that old helpers have stopped and
the desktop has been reset before `desktop.recover` with
`confirmDesktopReset: true`. Do not equate online transport, an expired lease,
a stopped host, or a successful read-only probe with that reset. Never replay
unknown or partial input or send blanket key releases. Retain exact process
identity and complete, untruncated evidence when reconciling old helpers.
