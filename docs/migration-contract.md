# Migration contract

The migration is complete only when every legacy control-plane capability has
a typed replacement and compatibility coverage.

| Legacy surface | Core capability or object | Verification |
|---|---|---|
| status, doctor, dashboard | executor health and controller projection | protocol tests |
| executor registration and mesh | Executor and Unix/TCP/command Transport | transport tests |
| workspace driver | DriverLease and Handoff | conflict/stale-writer E2E |
| resource lease | ResourceLease | fencing and expiry tests |
| task get/list/wait/watch/prune | Task and TaskEvent | state-machine tests |
| artifact build/describe/transfer | Artifact | digest/provenance tests |
| fs read/list/search/write/patch/remove/restore | filesystem provider | allow-root and conflict tests |
| tool bash and approval | process provider and Approval | policy tests |
| process lifecycle/readiness/logs | process provider | probe/event tests |
| port allocation | ResourceLease (`port:<n>`) | allocation conflict tests |
| agent lifecycle | AgentInstance | provider contract tests |
| tmux inspection/log panes | external tmux provider | tmux plugin tests |
| environment/component placement | command-provider adapter transaction | adapter tests |
| resource publish | generation activation transaction | rollback/activation E2E |

Compatibility responses retain the legacy `v1` envelope while the native API
uses `machine-fabric.dev/v1` resources.

Concrete product fixtures and acceptance criteria live with their private
adapters. This repository intentionally contains no product names, private
paths, or domain document types.
