# SPDX-License-Identifier: Apache-2.0
"""Single-writer Kubernetes adapter: one StatefulSet and retained PVC per identity."""
import json
from pathlib import Path
import re
import subprocess
import uuid

from neuralbase_operator import Controller, MAX_BYTES, read_json


def contains(actual, expected):
    """Allow API-defaulted fields, reject changes to every field we manage."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(k in actual and contains(actual[k], v) for k, v in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(contains(a, e) for a, e in zip(actual, expected))
    return actual == expected


class KubernetesController(Controller):
    def __init__(self, config, server, planner, readonly=False):
        self.kube = config["kubernetes"]
        if set(self.kube) != {"context", "namespace", "image", "storage_class", "storage"}:
            raise ValueError("invalid Kubernetes settings")
        for key in ("namespace", "storage_class"):
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,61}[a-z0-9]|[a-z0-9]", self.kube[key]):
                raise ValueError("invalid namespace/storage class")
        for key in ("context", "image", "storage"):
            if not self.kube[key] or len(self.kube[key]) > 256 or any(c.isspace() for c in self.kube[key]):
                raise ValueError("invalid Kubernetes setting")
        existing = Path(config["root"]) / "state.json"
        if existing.exists() and read_json(existing).get("kubernetes") != self.kube:
            raise ValueError("immutable deployment adapter settings changed")
        super().__init__(config, server, planner, readonly)
        self.persist()

    def persist(self):
        self.state.setdefault("kubernetes", self.kube)
        self.state.setdefault("owner", uuid.uuid4().hex)
        self.state.setdefault("objects", {})
        super().persist()

    def name(self, node_id):
        return node_id.replace(".", "-")

    def validate_inventory(self):
        names = set()
        for node_id, spec in self.specs.items():
            name = self.name(node_id)
            if not node_id.startswith(self.desired["cluster"] + ".") or not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,48}[a-z0-9]", name) or name in names:
                raise ValueError("invalid/duplicate Kubernetes incarnation name")
            names.add(name)
            dns = f"{name}-0.{name}.{self.kube['namespace']}.svc.cluster.local"
            if self.desired["endpoints"][node_id] != dns + ":7001" or spec != {"sql_addr": dns + ":5432", "metrics_addr": dns + ":9090"}:
                raise ValueError("inventory must use the managed stable pod DNS and ports")

    def kubectl(self, args, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        if data is not None and len(data) > MAX_BYTES:
            raise ValueError("Kubernetes request too large")
        result = subprocess.run(["kubectl", "--context", self.kube["context"], "--namespace", self.kube["namespace"],
                                 "--request-timeout=15s", *args], input=data, capture_output=True, timeout=20)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors="replace")[:1000])
        if len(result.stdout) > MAX_BYTES:
            raise ValueError("Kubernetes response too large")
        return json.loads(result.stdout) if result.stdout.strip() else None

    def get(self, kind, name):
        return self.kubectl(["get", kind, name, "--ignore-not-found", "-o", "json"])

    def manifests(self, node):
        name = self.name(node["id"])
        labels = {"neuralbase.io/owner": self.state["owner"], "neuralbase.io/member": name}
        meta = {"name": name, "namespace": self.kube["namespace"], "labels": labels}
        config = {"apiVersion": "v1", "kind": "ConfigMap", "metadata": meta, "immutable": True,
                  "data": {"node.json": json.dumps(node, sort_keys=True)}}
        service = {"apiVersion": "v1", "kind": "Service", "metadata": meta,
                   "spec": {"clusterIP": "None", "publishNotReadyAddresses": True, "selector": labels,
                            "ports": [{"name": "raft", "port": 7001, "targetPort": 7001}]}}
        pvc = {"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": meta,
               "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": self.kube["storage_class"],
                        "resources": {"requests": {"storage": self.kube["storage"]}}}}
        env = {"NEURALBASE_OPERATOR_NODE": "/config/node.json", "NEURALBASE_NODE_ID": node["id"],
               "NEURALBASE_DB_PATH": "/data/db", "NEURALBASE_USERS_FILE": "/data/users.json",
               "NEURALBASE_PEERS": "", "NEURALBASE_RAFT_ADDR": "0.0.0.0:7001", "NEURALBASE_LISTEN_ADDR": "0.0.0.0:5432",
               "NEURALBASE_METRICS_PORT": "9090", "NEURALBASE_AUTH_REQUIRED": "0", "NEURALBASE_RAFT_TLS": "0"}
        container = {"name": "neuralbase", "image": self.kube["image"], "imagePullPolicy": "IfNotPresent",
                     "command": ["/bin/sh", "-ec", "umask 077; mkdir -p /run/neuralbase/private; exec /app/neuralbase"],
                     "env": [{"name": k, "value": v} for k, v in sorted(env.items())],
                     "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}},
                     "securityContext": {"allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}},
                     "readinessProbe": {"exec": {"command": ["/app/neuralbase-operator", "ready", "/config/node.json"]},
                                        "timeoutSeconds": 15, "periodSeconds": 2, "failureThreshold": 1},
                     "volumeMounts": [{"name": "config", "mountPath": "/config", "readOnly": True},
                                      {"name": "data", "mountPath": "/data"}, {"name": "run", "mountPath": "/run/neuralbase"}]}
        stateful = {"apiVersion": "apps/v1", "kind": "StatefulSet", "metadata": meta,
                    "spec": {"serviceName": name, "replicas": 1, "selector": {"matchLabels": labels},
                             "updateStrategy": {"type": "OnDelete"},
                             "template": {"metadata": {"labels": labels},
                                          "spec": {"automountServiceAccountToken": False, "terminationGracePeriodSeconds": 10,
                                                   "securityContext": {"runAsUser": 10001, "runAsGroup": 10001, "fsGroup": 10001, "runAsNonRoot": True},
                                                   "containers": [container], "volumes": [
                                                       {"name": "config", "configMap": {"name": name}},
                                                       {"name": "data", "persistentVolumeClaim": {"claimName": name}},
                                                       {"name": "run", "emptyDir": {}}]}}}}
        sql_service = {"apiVersion": "v1", "kind": "Service",
                       "metadata": dict(meta, name=name + "-sql"),
                       "spec": {"selector": labels,
                                "ports": [{"name": "sql", "port": 5432, "targetPort": 5432}]}}
        return [config, service, sql_service, pvc, stateful]

    def validate_object(self, actual, expected):
        key = expected["kind"] + "/" + expected["metadata"]["name"]
        old = self.state["objects"].get(key)
        if old is not None and old != actual["metadata"]["uid"]:
            raise RuntimeError("managed Kubernetes object UID changed")
        compare = json.loads(json.dumps(expected))
        if expected["kind"] == "StatefulSet":
            compare["spec"]["replicas"] = actual["spec"]["replicas"]
            if actual["spec"]["replicas"] not in (0, 1):
                raise RuntimeError("raw replica scaling is forbidden")
        if not contains(actual, compare):
            raise RuntimeError("managed Kubernetes object configuration drift")
        return key

    def ensure(self, expected):
        actual = self.get(expected["kind"], expected["metadata"]["name"])
        if actual is None:
            actual = self.kubectl(["create", "-f", "-", "-o", "json"], expected)
        key = self.validate_object(actual, expected)
        self.state["objects"][key] = actual["metadata"]["uid"]
        self.persist()
        return actual

    def replicas(self, node, replicas):
        expected = self.manifests(node)[-1]
        actual = self.get("StatefulSet", self.name(node["id"]))
        if actual is None:
            if replicas == 0:
                return
            raise RuntimeError("missing managed StatefulSet")
        self.validate_object(actual, expected)
        # API-server compare-and-swap: stale reads and recreated objects fail.
        patch = [{"op": "test", "path": "/metadata/uid", "value": actual["metadata"]["uid"]},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": actual["metadata"]["resourceVersion"]},
                 {"op": "replace", "path": "/spec/replicas", "value": replicas}]
        if actual["spec"]["replicas"] != replicas:
            self.kubectl(["patch", "statefulset", self.name(node["id"]), "--type=json", "-p", json.dumps(patch), "-o", "json"])

    def create(self, node_id, learner, seeds):
        if node_id in self.state["retiring"]:
            raise RuntimeError("retired incarnation must not restart automatically")
        if node_id not in self.state["nodes"]:
            self.state["nodes"][node_id] = {"topology": self.desired, "id": node_id, "seeds": sorted(seeds), "genesis": self.state["initial_voters"],
                                            "learner": learner, "socket": "/run/neuralbase/private/admin.sock"}
            self.persist()
        node = self.state["nodes"][node_id]
        for expected in self.manifests(node):
            self.ensure(expected)
        self.replicas(node, 1)

    def admin(self, node, command):
        result = self.kubectl(["exec", "-i", self.name(node["id"]) + "-0", "--", "/app/neuralbase-operator", "admin", "/config/node.json"], command)
        if result["node"] != node or result["error"]:
            raise RuntimeError("management incarnation/configuration mismatch")
        return result

    def statuses(self):
        statuses = super().statuses()
        # An initializing/unreachable pod is still a deployment object. In
        # particular, tombstoned StatefulSets must reach zero before convergence.
        for node_id, node in self.state["nodes"].items():
            expected = self.manifests(node)[-1]
            actual = self.get("StatefulSet", self.name(node_id))
            if actual is None:
                continue
            self.validate_object(actual, expected)
            if actual["spec"]["replicas"] == 1 and node_id not in statuses:
                statuses[node_id] = {"node": node, "pid": 0,
                                     "status": {"is_leader": False, "ready": False, "applied": 0}}
            elif actual["spec"]["replicas"] == 0:
                # Wait for StatefulSet reconciliation and final pod exit.
                pod = self.get("Pod", self.name(node_id) + "-0")
                if pod is not None and node_id not in statuses:
                    statuses[node_id] = {"node": node, "pid": 0,
                                         "status": {"is_leader": False, "ready": False, "applied": 0}}
        return statuses

    def stop(self, node_id):
        self.replicas(self.state["nodes"][node_id], 0)
