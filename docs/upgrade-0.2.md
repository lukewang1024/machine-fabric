# Upgrade to 0.2

Version 0.2 changes the node transport and write-authority model. Upgrade the
laptop and every selected devbox as one fabric; do not intentionally run a
mixed 0.1/0.2 fabric.

From the laptop checkout:

```sh
scripts/bootstrap-fabric.sh --version 0.2.0 devbox-a devbox-b
# Native Windows hosts use an explicit prefix:
scripts/bootstrap-fabric.sh --version 0.2.0 devbox-a windows:windows-devbox
```

The bootstrap first converges the laptop, then each node-local Controller and
Executor, then installs/restarts the deterministic peer services, registers
both roles, and verifies the full selected topology. Run the same command with
`--verify-only` for a read-only audit after installation.
Installers snapshot existing Controller and Executor state under the platform
state directory's `machine-fabric/backups/<UTC timestamp>` before
replacing services.

Migration behavior:

- legacy Controller state receives the configured stable node ID;
- legacy sessions become owned by that local Controller at authority epoch 1;
- existing executor registrations are refreshed through the new peer proxies;
- Executor fence state starts empty and is populated by the first 0.2 mutating
  capability call;
- 0.1 socket-forward peer jobs are replaced by SSH-stdio peer jobs.

Rollback is not supported after 0.2 mutating work has begun: a 0.1 Controller
does not emit the authority envelope required by a 0.2 Executor. Restore a
pre-upgrade state backup only if a full-fabric rollback is unavoidable.
