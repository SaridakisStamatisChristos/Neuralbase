#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""NeuralBase guarded membership controller for local processes or Kubernetes.

Explicit single-writer deployment profile, immutable inventory, retained storage.
No HPA, automatic DR or production HA claim. See docs/PHASE7_OPERATOR.md.
"""
import argparse
import concurrent.futures
import fcntl
import json
import os
from pathlib import Path
import signal
import socket
import struct
import subprocess
import time

MAX_BYTES = 256 * 1024


class DesiredChanged(RuntimeError):
    pass


def read_json(path):
    with open(path, "rb") as file:
        data = file.read(MAX_BYTES + 1)
    if len(data) > MAX_BYTES:
        raise ValueError("document exceeds 256 KiB")
    return json.loads(data)


def save_json(path, value):
    data = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(data) > MAX_BYTES:
        raise ValueError("state exceeds 256 KiB")
    pending = path.with_suffix(".pending")
    with open(pending, "wb") as file:
        file.write(data)
        file.flush()
        os.fsync(file.fileno())
    os.replace(pending, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise OSError("management connection closed")
        data.extend(chunk)
    return bytes(data)


def admin(node, command, terminate=False):
    data = json.dumps({"cluster": node["topology"]["cluster"], "command": command}).encode()
    if len(data) > 8192:
        raise ValueError("management request too large")
    with socket.socket(socket.AF_UNIX) as sock:
        sock.settimeout(13)
        sock.connect(node["socket"])
        peer_pid, peer_uid, _ = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        if peer_uid != os.getuid() or peer_pid <= 0:
            raise ValueError("management peer identity mismatch")
        # Pin the process before requesting its configuration. If it exited
        # before pidfd_open, this fresh exchange must fail, even after PID reuse.
        pidfd = os.pidfd_open(peer_pid) if terminate else None
        try:
            return exchange(sock, data, node, peer_pid, pidfd)
        finally:
            if pidfd is not None:
                os.close(pidfd)


def exchange(sock, data, node, peer_pid, pidfd):
    sock.sendall(struct.pack("!I", len(data)) + data)
    size = struct.unpack("!I", receive(sock, 4))[0]
    if size > MAX_BYTES:
        raise ValueError("management response too large")
    result = json.loads(receive(sock, size))
    if result["node"] != node or result["pid"] != peer_pid:
        raise ValueError("management incarnation/configuration mismatch")
    if result["error"]:
        raise RuntimeError(result["error"])
    if pidfd is not None:
        signal.pidfd_send_signal(pidfd, signal.SIGTERM)
    return result


class Controller:
    def __init__(self, config, server, planner, readonly=False):
        self.readonly = readonly
        self.server = str(Path(server).resolve(strict=True))
        self.planner = str(Path(planner).resolve(strict=True))
        self.root = Path(config["root"])
        if not self.root.is_absolute() or self.root.is_symlink():
            raise ValueError("root must be an absolute non-symlink path")
        if not readonly:
            self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        if self.root.stat().st_mode & 0o077:
            raise ValueError("root must have mode 0700")
        self.lock = open(self.root / "controller.lock", "a+b")
        fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.path = self.root / "state.json"
        self.desired = json.loads(json.dumps(config["desired"]))
        if len(set(self.desired["voters"])) != len(self.desired["voters"]):
            raise ValueError("duplicate desired voter identities")
        self.desired["voters"] = sorted(self.desired["voters"])
        self.specs = config["processes"]
        self.children = []
        if set(self.specs) != set(self.desired["endpoints"]) or len(self.specs) > 128:
            raise ValueError("process inventory must equal endpoint inventory")
        self.validate_inventory()
        if self.path.exists():
            self.state = read_json(self.path)
            if self.state["version"] != 1:
                raise ValueError("unsupported controller state version")
            old = self.state["desired"]
            for key in ("version", "cluster", "endpoints", "minimum_voters"):
                if old[key] != self.desired[key]:
                    raise ValueError("immutable inventory/incarnation changed")
            if self.state["process_specs"] != self.specs:
                raise ValueError("immutable process inventory changed")
            if self.desired["revision"] < old["revision"] or (self.desired["revision"] == old["revision"] and self.desired != old):
                raise ValueError("stale/conflicting desired revision")
            self.state["desired"] = self.desired
            self.state["metrics"]["controller_starts"] += 1
        else:
            if readonly:
                raise ValueError("no initialized controller state")
            self.state = {"version": 1, "desired": self.desired, "process_specs": self.specs,
                          "nodes": {}, "pids": {}, "retiring": {}, "bootstrapped": False,
                          "failures": 0, "retry_after": 0, "last_success": None,
                          "metrics": {"controller_starts": 1, "attempts": 0, "blocked": 0, "failed": 0, "completed_actions": 0}}
        # Validate the desired document through the real Rust parser before save.
        self.plan({"cluster": self.desired["cluster"], "accepted_revision": self.desired["revision"],
                   "leader": "", "term": 0, "authority_index": 0, "transition_pending": False,
                   "committed": {"format_version": 1, "generation": 1, "config_index": 0,
                                 "voters": self.desired["voters"], "learners": [], "joint": None, "removed": []},
                   "processes": {}, "matched": {}})
        self.persist()

    def validate_inventory(self):
        ports = set()
        for node_id, spec in self.specs.items():
            if not node_id.startswith(self.desired["cluster"] + ".") or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_." for c in node_id):
                raise ValueError("invalid incarnation identity")
            for address in (self.desired["endpoints"][node_id], spec["sql_addr"], spec["metrics_addr"]):
                host, port = address.rsplit(":", 1)
                if host != "127.0.0.1" or not 1024 <= int(port) <= 65535 or int(port) in ports:
                    raise ValueError("adapter requires unique local unprivileged ports")
                ports.add(int(port))

    def admin(self, node, command):
        return admin(node, command)

    def stop(self, node_id):
        admin(self.state["nodes"][node_id], "Status", terminate=True)

    def assert_current_input(self):
        if hasattr(self, "config_path") and read_json(self.config_path) != self.expected_config:
            raise DesiredChanged("desired deployment input changed; restart the controller")

    def persist(self):
        if not self.readonly:
            save_json(self.path, self.state)

    def plan(self, observed):
        data = json.dumps({"desired": self.desired, "observed": observed}).encode()
        if len(data) > MAX_BYTES:
            raise ValueError("observation too large")
        result = subprocess.run([self.planner, "plan"], input=data, capture_output=True, timeout=5)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace")[:1000])
        if len(result.stdout) > MAX_BYTES:
            raise ValueError("planner output too large")
        return json.loads(result.stdout)

    def record_pid(self, node_id, pid):
        # Diagnostic only. Never use a remembered numeric PID to signal a process.
        self.state["pids"][node_id] = {"pid": pid}
        self.persist()

    def create(self, node_id, learner, seeds):
        if node_id in self.state["retiring"]:
            raise RuntimeError("retired incarnation must not restart automatically")
        if node_id not in self.state["nodes"]:
            node = {"topology": self.desired, "id": node_id, "seeds": sorted(seeds), "genesis": self.state["initial_voters"], "learner": learner,
                    "socket": str(self.root / (node_id + ".sock"))}
            self.state["nodes"][node_id] = node
            self.persist()  # Durable intent precedes process creation.
        node = self.state["nodes"][node_id]
        try:
            self.record_pid(node_id, self.admin(node, "Status")["pid"])
            return
        except (OSError, RuntimeError):
            pass
        # A crash after spawn but before PID publication is recovered via the
        # socket. Overlapping startup still cannot obtain two RocksDB locks.
        path = self.root / (node_id + ".json")
        save_json(path, node)
        spec = self.specs[node_id]
        aliases = {"NODE_ID", "PEERS", "DB_PATH", "LISTEN_ADDR", "RAFT_ADDR", "METRICS_PORT",
                   "RAFT_ELECTION_TIMEOUT_MS", "RAFT_TLS", "USERS_FILE", "AUTH_REQUIRED"}
        env = {k: v for k, v in os.environ.items() if not k.startswith(("NEURALBASE_", "TLS_")) and k not in aliases}
        env.update(NEURALBASE_OPERATOR_NODE=str(path), NEURALBASE_NODE_ID=node_id,
                   NEURALBASE_DB_PATH=str(self.root / (node_id + ".db")),
                   NEURALBASE_USERS_FILE=str(self.root / (node_id + ".users.json")), NEURALBASE_PEERS="",
                   NEURALBASE_RAFT_ADDR=self.desired["endpoints"][node_id], NEURALBASE_LISTEN_ADDR=spec["sql_addr"],
                   NEURALBASE_METRICS_PORT=spec["metrics_addr"].rsplit(":", 1)[1],
                   NEURALBASE_AUTH_REQUIRED="0", NEURALBASE_RAFT_TLS="0")
        with open(self.root / (node_id + ".log"), "ab") as log:
            child = subprocess.Popen([self.server], env=env, stdin=subprocess.DEVNULL, stdout=log,
                                     stderr=log, start_new_session=True)
        self.children = [c for c in self.children if c.poll() is None]
        self.children.append(child)
        self.record_pid(node_id, child.pid)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                self.record_pid(node_id, self.admin(node, "Status")["pid"])
                return
            except (OSError, RuntimeError):
                if child.poll() is not None:
                    raise RuntimeError("managed process failed to start; inspect its retained log")
                time.sleep(0.05)
        raise RuntimeError("managed process startup not yet observable; reobserve before retry")

    def bootstrap(self):
        if self.state["bootstrapped"]:
            # Duplicate bootstrap never resurrects removed initial voters.
            return
        if self.desired["revision"] != 1:
            raise ValueError("fresh bootstrap requires revision 1")
        self.state["initial_voters"] = sorted(self.desired["voters"])
        for node_id in self.state["initial_voters"]:
            self.assert_current_input()
            self.create(node_id, False, self.state["initial_voters"])
        self.state["bootstrapped"] = True
        self.persist()

    def statuses(self):
        def query(item):
            node_id, node = item
            try:
                return node_id, self.admin(node, "Status")
            except (OSError, RuntimeError):
                return node_id, None
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
            return {k: v for k, v in pool.map(query, list(self.state["nodes"].items())) if v is not None}

    def observe(self):
        statuses = self.statuses()
        authority = None
        for node_id, result in sorted(statuses.items()):
            if result["status"]["is_leader"]:
                try:
                    authority = self.admin(self.state["nodes"][node_id], "Observe")["status"]
                    break
                except (OSError, RuntimeError):
                    continue
        if authority is None:
            raise RuntimeError("no serving leader can establish quorum authority")
        time.sleep(0.08)
        statuses = self.statuses()
        fresh = statuses.get(authority["id"], {}).get("status")
        if fresh is None or not fresh["is_leader"] or fresh["term"] != authority["term"]:
            raise RuntimeError("leader changed while observing process catch-up")
        # Refresh match indexes after follower catch-up, retaining the original
        # proven barrier frontier. Do not append a new barrier while waiting.
        authority["matched"] = fresh["matched"]
        authority["committed"] = fresh["committed"]
        authority["transition_pending"] = fresh["transition_pending"]
        processes = {node_id: {"cluster": r["node"]["topology"]["cluster"],
                               "endpoint": r["node"]["topology"]["endpoints"][node_id],
                               "learner_bootstrap": r["node"]["learner"], "ready": r["status"]["ready"],
                               "applied": r["status"]["applied"]} for node_id, r in statuses.items()}
        return {"cluster": self.desired["cluster"], "accepted_revision": self.desired["revision"],
                "leader": authority["id"], "term": authority["term"], "authority_index": authority["authority_index"],
                "committed": authority["committed"], "transition_pending": authority["transition_pending"],
                "processes": processes, "matched": authority["matched"]}, statuses

    def step(self, dry_run=False):
        self.assert_current_input()
        if not self.state["bootstrapped"]:
            raise RuntimeError("explicit bootstrap required")
        self.state["metrics"]["attempts"] += 1
        observed, statuses = self.observe()
        plan = self.plan(observed)
        action = plan["action"]
        if dry_run:
            return {"plan": plan, "observed": observed}
        if action == "Converged":
            self.state["failures"] = 0
            self.state["retry_after"] = 0
            self.persist()
            return {"plan": plan, "observed": observed}
        name, node_id = next(iter(action.items()))
        if name == "Blocked":
            self.state["metrics"]["blocked"] += 1
            self.persist()
            return {"plan": plan, "observed": observed}
        current, statuses = self.observe()
        if self.plan(current) != plan:
            raise RuntimeError("action became stale; reobserve")
        self.assert_current_input()
        guard = {"leader": plan["leader"], "term": plan["term"], "generation": plan["membership_generation"]}
        leader = self.state["nodes"][plan["leader"]]
        if name == "CreateLearner":
            self.create(node_id, True, current["committed"]["voters"])
        elif name == "RestartMember":
            node = self.state["nodes"].get(node_id)
            if node is None:
                raise RuntimeError("restart requires original managed configuration")
            self.create(node_id, node["learner"], node["seeds"])
        elif name in ("AddLearner", "PromoteLearner", "RemoveMember"):
            change = "RemoveNode" if name == "RemoveMember" else name
            self.admin(leader, {"Membership": {"guard": guard, "change": {change: node_id}}})
        elif name == "TransferLeadership":
            self.admin(leader, {"Transfer": {"guard": guard, "target": node_id}})
        elif name == "StopRemoved":
            if node_id not in current["committed"]["removed"] or current["committed"]["joint"]:
                raise RuntimeError("retirement requires finalized committed tombstone")
            if node_id not in self.state["retiring"]:
                self.state["retiring"][node_id] = True
                self.persist()  # Crash after this point may resend SIGTERM safely.
            response = statuses.get(node_id)
            if response is not None:
                self.record_pid(node_id, response["pid"])
                self.stop(node_id)
        else:
            raise RuntimeError("unsupported planner action")
        self.state["last_success"] = plan
        self.state["metrics"]["completed_actions"] += 1
        self.state["failures"] = 0
        self.state["retry_after"] = 0
        self.persist()
        return {"plan": plan, "observed": current}

    def failed(self):
        self.state["failures"] = min(self.state["failures"] + 1, 1000)
        self.state["metrics"]["failed"] += 1
        self.state["retry_after"] = time.time() + min(0.1 * 2 ** min(self.state["failures"], 9), 30)
        self.persist()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["bootstrap", "reconcile", "plan", "status"])
    p.add_argument("--config", required=True)
    p.add_argument("--server", required=True)
    p.add_argument("--planner", required=True)
    p.add_argument("--steps", type=int, default=1)
    args = p.parse_args()
    if not 1 <= args.steps <= 1000:
        p.error("steps must be 1..=1000")
    readonly = args.command in ("plan", "status")
    config = read_json(Path(args.config))
    controller = Controller
    if "kubernetes" in config:
        from neuralbase_kubernetes import KubernetesController
        controller = KubernetesController
    c = controller(config, args.server, args.planner, readonly)
    c.config_path = Path(args.config)
    c.expected_config = config
    if args.command == "bootstrap":
        c.bootstrap()
        print(json.dumps({"bootstrapped": True}))
        return
    if args.command == "status":
        print(json.dumps(c.state))
        return
    for _ in range(args.steps):
        if not readonly:
            time.sleep(max(0, min(c.state["retry_after"] - time.time(), 30)))
        try:
            result = c.step(dry_run=readonly)
            print(json.dumps(result), flush=True)
            if result["plan"]["action"] == "Converged":
                return
        except DesiredChanged as error:
            print(json.dumps({"blocked": str(error)}), flush=True)
            raise SystemExit(2) from error
        except (OSError, RuntimeError, ValueError, subprocess.TimeoutExpired) as error:
            c.failed()
            print(json.dumps({"blocked": str(error)}), flush=True)
        time.sleep(0.2)
    if args.command == "reconcile":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
