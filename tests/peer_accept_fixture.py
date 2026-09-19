#!/usr/bin/env python3
"""Independent SSH peer-accept fixture used by cross-platform E2E tests."""

import argparse
import json
import os
import socket
import sys
import threading
import uuid


WRITE_LOCK = threading.Lock()
PENDING = {}


def write_frame(frame: dict) -> None:
    with WRITE_LOCK:
        sys.stdout.write(json.dumps(frame, separators=(",", ":")) + "\n")
        sys.stdout.flush()


def forward_local(role: str, request: dict) -> dict:
    state = os.environ.get("XDG_STATE_HOME", os.path.expanduser("~/.local/state"))
    path = os.path.join(state, "machine-fabric", f"{role}.sock")
    with socket.socket(socket.AF_UNIX) as client:
        client.connect(path)
        client.sendall(json.dumps(request).encode() + b"\n")
        return json.loads(client.makefile().readline())


def serve_exposed(path: str, role: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(path)
        os.chmod(path, 0o600)
        server.listen()
        while True:
            client, _ = server.accept()
            threading.Thread(target=handle_exposed, args=(client, role), daemon=True).start()


def handle_exposed(client: socket.socket, role: str) -> None:
    with client:
        request = json.loads(client.makefile().readline())
        frame_id = f"fixture_{uuid.uuid4().hex}"
        event = threading.Event()
        result: list = []
        PENDING[frame_id] = (event, result)
        write_frame({"type": "request", "id": frame_id, "target_role": role, "request": request})
        if not event.wait(30):
            raise TimeoutError("peer response timed out")
        client.sendall(json.dumps(result[0]).encode() + b"\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("peer")
    parser.add_argument("accept")
    parser.add_argument("--id", required=True)
    parser.add_argument("--local-id", required=True)
    parser.add_argument("--local-controller-socket")
    parser.add_argument("--local-executor-socket")
    parser.add_argument("--expose-controller-socket", required=True)
    parser.add_argument("--expose-executor-socket", required=True)
    args = parser.parse_args()
    hello = json.loads(sys.stdin.readline())
    if (
        hello.get("type") != "hello"
        or hello.get("protocol") != "machine-fabric.peer/v1"
        or hello.get("node_id") != args.id
        or sorted(hello.get("roles", [])) != ["controller", "executor"]
    ):
        raise RuntimeError(f"invalid peer hello: {hello}")
    write_frame(
        {
            "type": "hello-ack",
            "protocol": "machine-fabric.peer/v1",
            "node_id": args.local_id,
            "roles": ["controller", "executor"],
        }
    )
    threading.Thread(
        target=serve_exposed,
        args=(args.expose_controller_socket, "controller"),
        daemon=True,
    ).start()
    threading.Thread(
        target=serve_exposed,
        args=(args.expose_executor_socket, "executor"),
        daemon=True,
    ).start()
    for line in sys.stdin:
        frame = json.loads(line)
        if frame["type"] == "request":
            response = forward_local(frame["target_role"], frame["request"])
            write_frame({"type": "response", "id": frame["id"], "response": response})
        elif frame["type"] == "response":
            pending = PENDING.pop(frame["id"], None)
            if pending:
                event, result = pending
                result.append(frame["response"])
                event.set()


if __name__ == "__main__":
    main()
