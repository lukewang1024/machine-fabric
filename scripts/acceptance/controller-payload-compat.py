"""Offline RPC proof from a complete immutable current Controller rollback bundle.

No executor calls: only task.get/wait/submit (existing key). Each server is a
bounded test subprocess on its own private socket and exits before this tool.
"""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def private_json(path, value):
    with open(path, "x", encoding="utf-8") as file:
        os.chmod(path, 0o600)
        json.dump(value, file, indent=2, ensure_ascii=False)


def sha_file(path):
    hasher = hashlib.sha256()
    with open(path, "rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def resolve(value, directory):
    if not isinstance(value, dict) or "$machineFabricPayload" not in value:
        return value
    assert set(value) == {"$machineFabricPayload"}
    ref = value["$machineFabricPayload"]
    assert set(ref) == {"bytes", "sha256"}
    digest = ref["sha256"]
    assert len(digest) == 64 and set(digest) <= set("0123456789abcdef")
    path = directory / (digest + ".json")
    assert not directory.is_symlink() and not path.is_symlink()
    raw = path.read_bytes()
    assert len(raw) == ref["bytes"] and digest_bytes(raw) == digest
    return json.loads(raw)


def main():
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("--candidate", required=True, type=Path)
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--work", required=True, type=Path)
    args = parser.parse_args()
    args.work.mkdir(mode=0o700, parents=True, exist_ok=False)
    snapshot = args.snapshot.resolve()
    source_files = sorted(p for p in snapshot.rglob("*") if p.is_file())
    before = {str(p.relative_to(snapshot)): sha_file(p) for p in source_files}
    shutil.copytree(snapshot, args.work / "candidate-state")
    state_path = args.work / "candidate-state/controller.json"
    source = json.loads((snapshot / "controller.json").read_bytes())
    source_payloads = snapshot / "controller.json.payloads"
    expected = {t["id"]: t for t in source["tasks"]}
    assert all(t["state"] in ("succeeded", "failed", "cancelled", "timedOut", "outcomeUnknown") for t in expected.values())
    report = {"sourceStateSha256": before["controller.json"], "sourceFileHashes": before,
              "candidateSha256": sha_file(args.candidate), "taskCount": len(expected),
              "taskStates": dict(collections.Counter(t["state"] for t in expected.values())),
              "rows": [], "approvedCompactionDifferences": [], "rounds": []}
    # Derive source evidence without emitting strings, tokens, body or base64.
    for tid, task in expected.items():
        row = {"id": tid, "state": task["state"], "capability": task["capability"]}
        for field in ("input", "output"):
            raw = canonical(resolve(task.get(field), source_payloads))
            row[field] = {"canonicalBytes": len(raw), "sha256": digest_bytes(raw)}
        report["rows"].append(row)
    payload_files = sorted(source_payloads.glob("*.json"))
    report["sourceSidecars"] = {"count": len(payload_files), "bytes": sum(p.stat().st_size for p in payload_files)}
    chosen = next(t for t in expected.values() if t["state"] == "succeeded")
    migrated_hashes = []
    for round_number in range(2):
        with tempfile.TemporaryDirectory(prefix="mf44-") as tmp:
            sock = str(Path(tmp) / "rpc.sock")
            env = os.environ.copy()
            env["XDG_STATE_HOME"] = str(args.work / "logs")
            with open(args.work / f"server-{round_number}.log", "xb") as log:
                os.chmod(log.name, 0o600)
                proc = subprocess.Popen([str(args.candidate), "--socket", sock, "controller", "serve",
                                         "--state", str(state_path), "--id", source["controllerId"]], env=env,
                                        stdout=log, stderr=log)
                try:
                    deadline = time.monotonic() + 60
                    while not Path(sock).exists():
                        assert proc.poll() is None, "isolated server exited; inspect server log"
                        assert time.monotonic() < deadline, "isolated startup deadline"
                        time.sleep(.05)

                    def call(action, params):
                        with socket.socket(socket.AF_UNIX) as client:
                            client.settimeout(30)
                            client.connect(sock)
                            request = {"apiVersion": "machine-fabric.dev/v1", "requestId": "proof44", "action": action, "params": params}
                            client.sendall(canonical(request) + b"\n")
                            with client.makefile("rb") as stream:
                                response = json.loads(stream.readline())
                        assert response["ok"], (action, response.get("error"))
                        return response["result"]

                    for index, (tid, task) in enumerate(expected.items()):
                        for action in ("task.get", "task.wait"):
                            value = call(action, {"taskId": tid, "timeoutMs": 0})
                            assert value["id"] == tid and value["state"] == task["state"]
                            for field in ("input", "output"):
                                target = resolve(task.get(field), source_payloads)
                                assert canonical(value.get(field)) == canonical(target), (tid, action, field)
                            for key in ("attempt", "createdAt", "updatedAt", "idempotencyKey", "workspaceSessionId", "executorId"):
                                assert value.get(key) == task.get(key), (tid, key)
                        if index % 200 == 0:
                            print(f"round={round_number} verified={index}/{len(expected)}", flush=True)
                    # Reused terminal task submit is a local persistence trigger, never dispatches.
                    reused = call("task.submit", {"workspaceSessionId": chosen["workspaceSessionId"],
                        "executorId": chosen["executorId"], "capability": chosen["capability"],
                        "idempotencyKey": chosen["idempotencyKey"], "input": resolve(chosen["input"], source_payloads)})
                    assert reused["reused"] and reused["task"]["id"] == chosen["id"]
                    assert canonical(reused["task"].get("output")) == canonical(resolve(chosen.get("output"), source_payloads))
                    saved = json.loads(state_path.read_bytes())
                    assert {t["id"] for t in saved["tasks"]} == set(expected)
                    for key in ("sessions", "leases", "leaseFences", "executors", "controllers", "controllerId"):
                        assert saved.get(key) == source.get(key), key
                    migrated_hashes.append(sha_file(state_path))
                    report["rounds"].append({"round": round_number, "getPassed": len(expected), "waitPassed": len(expected),
                                            "reusedPassed": True, "stateBytes": state_path.stat().st_size,
                                            "stateSha256": migrated_hashes[-1]})
                finally:
                    proc.terminate()
                    proc.wait(timeout=10)
    assert migrated_hashes[0] == migrated_hashes[1], "restart and repeated persistence changed state"
    report["sourceUnchanged"] = all(sha_file(snapshot / p) == digest for p, digest in before.items())
    assert report["sourceUnchanged"]
    refs = [(t, k, t[k + "Ref"]) for t in saved["tasks"] for k in ("input", "output") if t.get(k + "Ref")]
    for task, field, ref in refs:
        assert ref["locator"] == f'task-payloads/{ref["digest"]}.json'
        path = state_path.parent / ref["locator"]
        raw = path.read_bytes()
        assert len(raw) == ref["bytes"] and "sha256:" + digest_bytes(raw) == ref["digest"]
        assert canonical(json.loads(raw)) == canonical(resolve(expected[task["id"]].get(field), source_payloads))
        assert path.stat().st_mode & 0o777 == 0o600
    report["candidateReferences"] = {"count": len(refs), "unique": len({r[2]["digest"] for r in refs}),
                                     "referencedBytes": sum(r[2]["bytes"] for r in refs)}
    report["relayArchiveTasksExact"] = sum(t["capability"].startswith("artifact.relay.archive.") for t in expected.values())
    report["allPayloadHashesExact"] = True
    report["latestIdsStatesLeasesFencesPreserved"] = True
    private_json(args.work / "proof.json", report)
    print(json.dumps({k: v for k, v in report.items() if k not in ("rows", "sourceFileHashes")}))


if __name__ == "__main__":
    main()
