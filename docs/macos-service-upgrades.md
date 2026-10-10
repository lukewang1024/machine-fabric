# macOS service upgrades

The macOS Executor and managed peer connections can share the Machine Fabric
app executable. The installer identifies each loaded role by its launchd label
and verifies that its PID owns the expected executable. An executable path
alone does not authorize termination. Unknown process ownership or unsupported
peer configuration stops the upgrade before any job is stopped.

Loaded peers using the app are booted out by their own labels before replacing
the app. After the Controller and Executor become ready, the installer restarts
those peers from their existing plists and verifies their launchd owners and
executable paths. Previously unloaded peers stay unloaded. The installer does
not change peer topology. Peer process readiness does not establish remote
transport or desktop readiness; check those through the local Controller.

If installation or peer startup fails, rollback restores the previous executable
and service configuration, then restarts the roles that were previously loaded.
Durable tasks, payloads, fences and desktop queue state remain current. An old
uncertain input outcome is never cleared or replayed by a service upgrade.
