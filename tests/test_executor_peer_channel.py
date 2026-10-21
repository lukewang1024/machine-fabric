"""Real loopback OpenSSH + independent executor IPC acceptance.

MACHINE_FABRIC_TEST_BINARY=/absolute/machine-fabric python3 -m unittest \
    discover -s tests -p test_executor_peer_channel.py -v

Uses only disposable keys, a loopback sshd and task-owned Unix sockets. Never
modifies the user's SSH configuration or authorized_keys. This is Linux/macOS
transport acceptance, not evidence of a native Windows desktop/CU deployment.
"""
import getpass
import json
import os
from pathlib import Path
import shlex
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import uuid


def request(action, params=None):
    return {"apiVersion": "machine-fabric.dev/v1", "requestId": str(uuid.uuid4()),
            "action": action, "params": params or {}}


def rpc(path, action, params=None):
    with socket.socket(socket.AF_UNIX) as stream:
        stream.settimeout(4)
        stream.connect(str(path))
        stream.sendall(json.dumps(request(action, params)).encode() + b"\n")
        with stream.makefile("rb") as reader:
            value = reader.readline()
        if not value:
            raise EOFError("IPC ended before receipt")
        return json.loads(value)


def until(predicate, seconds=8):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, EOFError, KeyError):
            pass
        time.sleep(0.02)
    raise AssertionError("condition did not become true")


class ToolServer:
    def __init__(self, path):
        self.path = path
        self.listener = socket.socket(socket.AF_UNIX)
        self.listener.bind(str(path))
        self.listener.listen()
        self.listener.settimeout(0.1)
        self.stop = threading.Event()
        self.entered = threading.Event()
        self.release = threading.Event()
        self.calls = []
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.serve, daemon=True)
        self.thread.start()

    def serve(self):
        while not self.stop.is_set():
            try:
                stream, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self.handle, args=(stream,), daemon=True).start()

    def handle(self, stream):
        with stream:
            stream.settimeout(5)
            with stream.makefile("rb") as reader:
                envelope = json.loads(reader.readline())
            assert envelope["action"] == "peer.dispatch"
            params = envelope["params"]
            inner = params["request"]
            with self.lock:
                self.calls.append(params)
            if inner["action"] == "hold":
                self.entered.set()
                self.release.wait(5)
            response = {"apiVersion": "machine-fabric.dev/v1",
                        "requestId": envelope["requestId"], "ok": True,
                        "result": {"binding": params["binding"], "peerId": params["peerId"],
                                   "input": inner["params"]}}
            try:
                stream.sendall(json.dumps(response).encode() + b"\n")
            except (BrokenPipeError, ConnectionResetError):
                pass

    def close(self):
        self.release.set()
        self.stop.set()
        self.listener.close()
        self.thread.join(2)


@unittest.skipUnless(os.name == "posix" and os.environ.get("MACHINE_FABRIC_TEST_BINARY"),
                     "requires explicit built binary and POSIX loopback OpenSSH")
class ExecutorPeerChannelTests(unittest.TestCase):
    def setUp(self):
        self.binary = str(Path(os.environ["MACHINE_FABRIC_TEST_BINARY"]).resolve())
        self.root_context = tempfile.TemporaryDirectory(prefix="mf-peer-")
        self.root = Path(self.root_context.name)
        self.processes = []
        self.logs = []
        self.tools = [ToolServer(self.root / "a-handler"), ToolServer(self.root / "b-handler")]
        self.addCleanup(self.cleanup)
        self.env = dict(os.environ)
        self.env["XDG_STATE_HOME"] = str(self.root / "state")
        self.env["XDG_CONFIG_HOME"] = str(self.root / "config")
        self.sshd = shutil.which("sshd")
        self.ssh = shutil.which("ssh")
        if not self.sshd or not self.ssh or not shutil.which("ssh-keygen"):
            self.fail("real OpenSSH tools are required for this acceptance suite")
        for name in ["host-key", "client-key"]:
            subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "",
                            "-f", str(self.root / name)], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=self.env)
        self.server_config = self.channel_config("b", "a")
        self.write_private("b.json", self.server_config)
        self.authorized = self.root / "authorized_keys"
        self.authorized.write_text("restrict " + (self.root / "client-key.pub").read_text())
        self.authorized.chmod(0o600)
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", 0))
            self.port = reserve.getsockname()[1]
        forced = shlex.join(["/usr/bin/env",
                             "XDG_STATE_HOME=" + str(self.root / "remote-state"),
                             "XDG_CONFIG_HOME=" + str(self.root / "remote-config"),
                             self.binary, "peer", "channel-accept", "--config", str(self.root / "b.json")])
        sshd_config = "\n".join([
            f"Port {self.port}", "ListenAddress 127.0.0.1",
            f"HostKey {self.root / 'host-key'}", f"PidFile {self.root / 'sshd.pid'}",
            f"AuthorizedKeysFile {self.authorized}", "StrictModes no",
            "UsePAM no", "PasswordAuthentication no", "KbdInteractiveAuthentication no",
            "PubkeyAuthentication yes", "PermitRootLogin prohibit-password",
            "AllowTcpForwarding no", "AllowAgentForwarding no", "X11Forwarding no",
            "PermitTTY no", f"AllowUsers {getpass.getuser()}", f"ForceCommand {forced}",
            "LogLevel ERROR", "",
        ])
        (self.root / "sshd.conf").write_text(sshd_config)
        self.server = self.spawn([self.sshd, "-D", "-e", "-f", str(self.root / "sshd.conf")], "sshd")
        def listening():
            if self.server.poll() is not None:
                self.fail("test sshd exited: " + (self.root / "sshd.log").read_text())
            with socket.create_connection(("127.0.0.1", self.port), 0.2):
                return True
        until(listening)
        host_public = (self.root / "host-key.pub").read_text().split()
        self.known_hosts = self.root / "known_hosts"
        self.known_hosts.write_text(f"[127.0.0.1]:{self.port} " + " ".join(host_public[:2]) + "\n")
        self.client_config = self.channel_config("a", "b")
        self.client_config["ssh"] = {
            "host": "127.0.0.1", "port": self.port, "username": getpass.getuser(),
            "identityFile": str(self.root / "client-key"),
            "knownHostsFile": str(self.known_hosts), "executable": self.ssh,
            "remoteCommand": forced, "connectTimeoutSeconds": 2,
        }

    def cleanup(self):
        for name in ["a", "b"]:
            try:
                rpc(self.root / (name + "-control"), "peer.revoke")
            except (OSError, EOFError):
                pass
        for tool in getattr(self, "tools", []):
            tool.close()
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(3)
        for log in self.logs:
            log.close()
        self.root_context.cleanup()

    def channel_config(self, local, remote):
        return {
            "apiVersion": "machine-fabric.peer-channel/v1",
            "localId": local, "peerId": remote,
            "services": ["tools"], "requiredPeerServices": ["tools"],
            "binding": {"leaseId": "lease-1", "scope": {"allocationId": "allocation-1",
                        "attempt": 1, "sourceNodeId": "a", "targetNodeId": "b",
                        "sourceEpoch": 2, "targetEpoch": 3, "controllerId": "owner-1"}},
            "expiresAtMs": int(time.time() * 1000) + 30_000,
            "handlerSocket": str(self.root / (local + "-handler")),
            "requestSocket": str(self.root / (local + "-requests")),
            "controlSocket": str(self.root / (local + "-control")),
            "allowedInboundActions": ["echo", "click", "hold"],
            "allowedOutboundActions": ["echo", "click", "hold"],
            "limits": {"handshakeTimeoutMs": 2000, "requestTimeoutMs": 3000},
        }

    def write_private(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value))
        path.chmod(0o600)

    def spawn(self, args, name):
        log = (self.root / (name + ".log")).open("wb")
        self.logs.append(log)
        process = subprocess.Popen(args, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                   stderr=log, env=self.env)
        self.processes.append(process)
        return process

    def start_client(self):
        self.write_private("a.json", self.client_config)
        self.client = self.spawn([self.binary, "peer", "channel-connect", "--config",
                                  str(self.root / "a.json")], "client")

    def ready(self):
        return until(lambda: rpc(self.root / "a-control", "peer.status")["result"]["channel"]["state"] == "ready")

    def call(self, node, action, params=None):
        response = rpc(self.root / (node + "-requests"), "peer.call",
                       {"service": "tools", "request": request(action, params)})
        self.assertTrue(response["ok"], response)
        return response["result"]

    def test_real_ssh_duplex_no_controller_and_action_grant(self):
        self.start_client()
        self.ready()
        for source, target in [("a", "b"), ("b", "a")]:
            result = self.call(source, "echo", {"source": source})
            self.assertTrue(result["ok"], result)
            self.assertEqual(result["result"]["peerId"], source)
            self.assertEqual(result["result"]["input"], {"source": source})
            self.assertEqual(result["result"]["binding"]["scope"]["allocationId"], "allocation-1")
        denied = rpc(self.root / "a-requests", "peer.call",
                     {"service": "tools", "request": request("command.run", {"command": "forbidden"})})
        self.assertFalse(denied["ok"])
        self.assertEqual(len(self.tools[1].calls), 1)
        self.assertFalse((self.root / "controller.sock").exists())

    def test_revocation_interrupts_busy_tool_and_removes_private_ipc(self):
        self.start_client()
        self.ready()
        outcome = []
        def hold():
            try:
                outcome.append(self.call("a", "hold"))
            except (OSError, EOFError):
                outcome.append({"disconnected": True})
        thread = threading.Thread(target=hold)
        thread.start()
        self.assertTrue(self.tools[1].entered.wait(3))
        try:
            rpc(self.root / "a-control", "peer.revoke")
        except EOFError:
            pass  # Closing the owned control endpoint may race its final ACK.
        self.client.wait(5)
        thread.join(5)
        self.assertFalse(thread.is_alive())
        until(lambda: not (self.root / "a-requests").exists() and not (self.root / "b-requests").exists())
        self.assertEqual(len(self.tools[1].calls), 1)
        self.assertTrue(outcome)

    def test_expiry_stops_reconnect_and_active_ssh(self):
        expiry = int(time.time() * 1000) + 2500
        self.client_config["expiresAtMs"] = expiry
        self.server_config["expiresAtMs"] = expiry
        self.write_private("b.json", self.server_config)
        self.start_client()
        self.ready()
        self.client.wait(6)
        self.assertEqual(self.client.returncode, 0)
        until(lambda: not (self.root / "b-control").exists())

    def test_wrong_scope_never_reaches_tool(self):
        self.client_config["binding"]["scope"]["allocationId"] = "wrong-allocation"
        self.client_config["expiresAtMs"] = int(time.time() * 1000) + 2200
        self.start_client()
        self.client.wait(6)
        self.assertEqual(self.tools[1].calls, [])
        self.assertFalse((self.root / "a-requests").exists())

    def test_wrong_pinned_host_key_never_dispatches(self):
        wrong_key = (self.root / "client-key.pub").read_text().split()
        self.known_hosts.write_text(f"[127.0.0.1]:{self.port} " + " ".join(wrong_key[:2]) + "\n")
        self.client_config["expiresAtMs"] = int(time.time() * 1000) + 2000
        self.start_client()
        self.client.wait(6)
        self.assertEqual(self.tools[1].calls, [])

    def test_reconnect_does_not_replay_ambiguous_tool(self):
        self.start_client()
        self.ready()
        before = rpc(self.root / "a-control", "peer.status")["result"]["channel"]["generation"]
        outcome = []
        caller = threading.Thread(target=lambda: outcome.append(self.call("a", "hold")))
        caller.start()
        self.assertTrue(self.tools[1].entered.wait(3))
        # Stop just the accepting adapter. The connecting executor's lease remains
        # live, so it must start a fresh SSH session without replaying this call.
        try:
            rpc(self.root / "b-control", "peer.close")
        except EOFError:
            pass
        caller.join(5)
        self.assertFalse(caller.is_alive())
        self.assertEqual(outcome[0]["error"]["code"], "PEER_OUTCOME_UNKNOWN")
        def reconnected():
            status = rpc(self.root / "a-control", "peer.status")["result"]["channel"]
            return status["state"] == "ready" and status["generation"] > before
        until(reconnected)
        self.assertEqual(len(self.tools[1].calls), 1)
        self.assertTrue(self.call("a", "echo")["ok"])
        self.assertEqual(len(self.tools[1].calls), 2)

    def test_bilateral_renewal_extends_active_channel_then_expires(self):
        initial_expiry = int(time.time() * 1000) + 2500
        self.client_config["expiresAtMs"] = initial_expiry
        self.server_config["expiresAtMs"] = initial_expiry
        self.write_private("b.json", self.server_config)
        self.start_client()
        self.ready()
        renewed_expiry = initial_expiry + 1800
        # The remote supervisor must also persist the new grant for a future
        # accepting process. An IPC renewal alone only updates a running process.
        self.server_config["expiresAtMs"] = renewed_expiry
        self.write_private("b.json", self.server_config)
        for name in ["a", "b"]:
            renewed = rpc(self.root / (name + "-control"), "peer.renew",
                          {"expiresAtMs": renewed_expiry})
            self.assertTrue(renewed["ok"], renewed)
        until(lambda: int(time.time() * 1000) > initial_expiry + 100)
        self.assertTrue(self.call("a", "echo")["ok"])
        self.client.wait(5)
        self.assertEqual(self.client.returncode, 0)
        until(lambda: not (self.root / "b-control").exists())

    def test_receiver_grant_is_enforced_independently(self):
        self.server_config["allowedInboundActions"] = ["echo"]
        self.write_private("b.json", self.server_config)
        self.start_client()
        self.ready()
        denied = self.call("a", "click")
        self.assertFalse(denied["ok"])
        self.assertEqual(denied["error"]["code"], "PEER_ACTION_DENIED")
        self.assertEqual(self.tools[1].calls, [])


if __name__ == "__main__":
    unittest.main()
