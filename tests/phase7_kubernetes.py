# SPDX-License-Identifier: Apache-2.0
"""Real kind/StatefulSet/PVC lifecycle; invoked by the exact-head CI job."""
import base64
import hashlib
import hmac
import os
import json
from pathlib import Path
import subprocess
import tempfile
import time

REPO = Path(__file__).parents[1]
CONTEXT = "kind-neuralbase-phase7"
NAMESPACE = "neuralbase-phase7"


def kube(*args):
    return subprocess.check_output(["kubectl", "--context", CONTEXT, "-n", NAMESPACE, "--request-timeout=20s", *args], timeout=30).decode()


def main():
    with tempfile.TemporaryDirectory(prefix="nb7-kube-") as root:
        ids = ["k7." + c for c in "abcde"]
        dns = {node: f"{node.replace('.', '-')}-0.{node.replace('.', '-')}.{NAMESPACE}.svc.cluster.local" for node in ids}
        config = {"root": root + "/controller", "desired": {"version": 1, "cluster": "k7", "revision": 1,
                  "minimum_voters": 3, "voters": ids[:3], "endpoints": {node: host + ":7001" for node, host in dns.items()}},
                  "processes": {node: {"sql_addr": host + ":5432", "metrics_addr": host + ":9090"} for node, host in dns.items()},
                  "kubernetes": {"context": CONTEXT, "namespace": NAMESPACE, "image": "neuralbase:phase7", "storage_class": "standard", "storage": "1Gi"}}
        salt = os.urandom(16)
        salted = hashlib.pbkdf2_hmac("sha256", b"phase7-password", salt, 4096)
        encode = lambda value: base64.b64encode(value).decode()
        identity = {"users": [{"method": "scram-sha-256", "username": "postgres", "salt": encode(salt), "iterations": 4096,
                              "stored_key": encode(hashlib.sha256(hmac.digest(salted, b"Client Key", "sha256")).digest()),
                              "server_key": encode(hmac.digest(salted, b"Server Key", "sha256"))}]}
        identity_path = Path(root) / "bootstrap-users.json"
        identity_path.write_text(json.dumps(identity))
        identity_path.chmod(0o600)
        config["identity_bootstrap"] = {"path": str(identity_path), "sha256": hashlib.sha256(identity_path.read_bytes()).hexdigest()}
        path = Path(root) / "desired.json"
        def write():
            path.write_text(json.dumps(config))
        write()
        def invoke(verb):
            result = subprocess.run(["python3", str(REPO / "ops/neuralbase_operator.py"), verb, "--config", str(path),
                                     "--server", str(REPO / "target/debug/neuralbase"), "--planner", str(REPO / "target/debug/neuralbase-operator")],
                                    capture_output=True, text=True, timeout=240)
            assert result.returncode in (0, 2), result.stderr
            lines = result.stdout.strip().splitlines()
            assert lines, result.stderr
            response = json.loads(lines[-1])
            print(json.dumps(response), flush=True)
            return response
        def converge():
            deadline = time.monotonic() + 420
            while time.monotonic() < deadline:
                result = invoke("reconcile")
                if result.get("plan", {}).get("action") == "Converged":
                    return result
                time.sleep(0.5)
            raise AssertionError("Kubernetes reconciliation did not converge")
        def change(voters):
            config["desired"]["revision"] += 1
            config["desired"]["voters"] = voters
            write()
        def sql(node, statement, user="postgres", password="phase7-password"):
            return kube("exec", node.replace(".", "-") + "-0", "--", "env", "PGPASSWORD=" + password,
                        "psql", "-h", "127.0.0.1", "-U", user, "-d", "postgres", "-v", "ON_ERROR_STOP=1", "-Atc", statement).strip()
        def verify(voters):
            for node in voters:
                deadline = time.monotonic() + 30
                while True:
                    try:
                        valid = sql(node, "SELECT COUNT(*) FROM kube_items", "kube_user") == "1"
                        try:
                            sql(node, "SELECT COUNT(*) FROM kube_items", "kube_user", "wrong-password")
                            authenticated = False
                        except subprocess.CalledProcessError:
                            authenticated = True
                        if valid and authenticated:
                            break
                    except subprocess.CalledProcessError:
                        pass
                    assert time.monotonic() < deadline, "SQL/SCRAM did not converge on " + node
                    time.sleep(0.2)
        try:
            invoke("bootstrap")
            first = converge()
            leader = first["observed"]["leader"]
            sql(leader, "CREATE TABLE kube_items (id INT)")
            sql(leader, "INSERT INTO kube_items VALUES (1)")
            sql(leader, "CREATE USER kube_user WITH PASSWORD 'phase7-password'")
            verify(ids[:3])
            change(ids[:4])
            original = Path(root + "/controller/state.json").read_bytes()
            assert invoke("plan")["plan"]["action"] == {"CreateLearner": "k7.d"}
            assert Path(root + "/controller/state.json").read_bytes() == original
            # Partial creation: config/services succeed, PVC quota rejects the
            # learner's storage. It must not become a voter or lose its intent.
            kube("create", "quota", "phase7-pvc-limit", "--hard=persistentvolumeclaims=3")
            failed = invoke("reconcile")
            assert "quota" in failed.get("blocked", "").lower(), failed
            kube("delete", "quota", "phase7-pvc-limit")
            invoke("reconcile")
            # External template drift must block, never silently overwrite it.
            kube("patch", "statefulset", "k7-d", "--type=json", "-p", '[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"invalid.invalid/neuralbase:missing"}]')
            blocked = invoke("reconcile")
            assert "configuration drift" in blocked.get("blocked", "")
            kube("patch", "statefulset", "k7-d", "--type=json", "-p", '[{"op":"replace","path":"/spec/template/spec/containers/0/image","value":"neuralbase:phase7"}]')
            expanded = converge()
            assert len(expanded["observed"]["committed"]["voters"]) == 4
            verify(ids[:4])
            leader = expanded["observed"]["leader"]
            kube("delete", "pod", leader.replace(".", "-") + "-0", "--wait=false")
            recovered = converge()
            verify(ids[:4])
            old = recovered["observed"]["leader"]
            voters = [node for node in ids[:4] if node != old] + [ids[4]]
            change(voters)
            replaced = converge()
            assert old in replaced["observed"]["committed"]["removed"]
            assert json.loads(kube("get", "statefulset", old.replace(".", "-"), "-o", "json"))["spec"]["replicas"] == 0
            assert json.loads(kube("get", "pvc", old.replace(".", "-"), "-o", "json"))["status"]["phase"] == "Bound"
            verify(voters)
            remove = next(node for node in voters if node != replaced["observed"]["leader"])
            voters.remove(remove)
            change(voters)
            final = converge()
            assert len(final["observed"]["committed"]["voters"]) == 3
            verify(voters)
            assert json.loads(kube("get", "statefulset", remove.replace(".", "-"), "-o", "json"))["spec"]["replicas"] == 0
            assert converge()["observed"]["committed"]["generation"] == final["observed"]["committed"]["generation"]
            print("PASS: Kubernetes 3 -> 4 -> replacement -> 3; retained PVCs; SQL/SCRAM convergence", flush=True)
        except Exception:
            for node in ids:
                subprocess.run(["kubectl", "--context", CONTEXT, "-n", NAMESPACE, "logs", node.replace(".", "-") + "-0", "--tail=60"], timeout=20, check=False)
            raise


if __name__ == "__main__":
    main()
