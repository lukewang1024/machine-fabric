# Windows single-node upgrade

`windows-targeted-upgrade.ps1` submits one Windows service upgrade to Task
Scheduler as SYSTEM. The worker and its protected receipt survive the Controller
and Executor service restart. It does not bootstrap a fabric, prune peers,
change desktop admission, or automatically retry a lost submission.

Use this only in an **exclusive node maintenance window**. Desktop maintenance
prevents new desktop sessions, but Fabric 0.1.x does not provide a general
Executor admission freeze: a momentary zero-work reading cannot prevent another
Controller from submitting non-desktop work. Coordinate all Controllers before
starting. Do not use this procedure if another owner can submit work during the
transaction. Take desktop maintenance using the existing `desktop.maintenance`
operation, verify the returned owner and safe point, and retain it until the
transaction and cross-node health checks finish. Never clear quarantine or
cancel someone else's session to make the check pass.

Obtain an immutable Windows release archive and its SHA-256 from the trusted
release source, and independently verify the upgrade tool from its own trusted
release. Copy the tool into an administrator-controlled directory before
executing it as SYSTEM; never execute a SYSTEM script from a directory writable
by an untrusted user. Stage the candidate archive and a UTF-8 JSON plan. The
plan has this shape (the values are examples, not install defaults):

```json
{
  "version": "0.1.91",
  "nodeId": "windows-node",
  "maintenanceOwner": "upgrade-owner",
  "archiveSha256": "<64 lowercase hex characters>",
  "previousBinarySha256": "<64 lowercase hex characters>",
  "policyHome": "C:\\Users\\Example",
  "allowRoots": ["C:/Users", "C:/ProgramData/machine-fabric"],
  "managedPathMappings": []
}
```

Populate the policy fields from the two currently running services and their
trusted installation record, preserving order and exact spelling. The tool
reconstructs both existing service command lines and rejects any difference,
unknown state, foreign service identity, active or queued work, active lease,
desktop helper, or missing/mismatched maintenance owner. Record the plan hash,
archive hash, tool hash and a unique transaction ID before submitting:

```powershell
& $verifiedTool -Action Start -TransactionId $transactionId `
  -PlanPath $plan -PlanSha256 $planSha256 -ArchivePath $archive `
  -ToolSha256 $toolSha256
```

The transaction directory is created under the trusted installation directory
with a SYSTEM/Administrators-only ACL. The submitted worker independently
rechecks the plan, archive, services, capacity and durable admission state before
using the release's normal `install-windows.ps1` with the exact prior policy.
Only the submitting command itself may occupy Executor capacity during initial
preflight; the scheduled worker requires zero active and queued work.

If the submission response or peer connection is lost, **do not submit again**.
Inspect the same transaction with `-Action Status -TransactionId <id>` after
reconnection or through an independent local operator session. A `prepared` or
`submitted` receipt is not proof that installation started or completed. A
`failed` or incomplete receipt must be retained and reconciled: the worker
only attempts to start existing service definitions after an installer failure;
it does not automatically roll back versioned state or clear desktop quarantine.
Verify both actual running service identities, installation digest, version,
peer health and unchanged policy before releasing the maintenance owner.

After a `succeeded` receipt, copy the receipt to operator-owned audit storage,
ensure the scheduled task is no longer running, and invoke `-Action Cleanup`
with the same transaction ID. Cleanup verifies task identity and removes only
that task and protected transaction directory. Verify the cleanup result.
