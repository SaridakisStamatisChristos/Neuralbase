# SPDX-License-Identifier: Apache-2.0
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0, str(Path(__file__).parents[1] / "ops"))
from neuralbase_operator import Controller
from neuralbase_kubernetes import KubernetesController


class KubernetesGuards(unittest.TestCase):
    def setUp(self):
        self.controller = KubernetesController.__new__(KubernetesController)
        self.controller.state = {"owner": "test-owner", "objects": {"StatefulSet/k7-a": "original-uid"}}
        self.expected = {"kind": "StatefulSet", "metadata": {"name": "k7-a", "labels": {"owner": "ours"}},
                         "spec": {"replicas": 1, "template": {"image": "expected"}}}
        self.actual = copy.deepcopy(self.expected)
        self.actual["metadata"].update(uid="original-uid", resourceVersion="17")

    def pod(self, uid, ready=True, deleting=False):
        metadata = {"name": "k7-a-0", "uid": uid,
                    "labels": {"neuralbase.io/owner": "test-owner", "neuralbase.io/member": "k7-a"}}
        if deleting:
            metadata["deletionTimestamp"] = "2026-09-17T00:00:00Z"
        return {"metadata": metadata,
                "status": {"phase": "Running",
                           "containerStatuses": [{"name": "neuralbase", "ready": ready}]}}

    def test_recreated_object_is_not_adopted(self):
        self.actual["metadata"]["uid"] = "replacement-uid"
        with self.assertRaisesRegex(RuntimeError, "UID changed"):
            self.controller.validate_object(self.actual, self.expected)

    def test_external_scaling_and_template_drift_fail_closed(self):
        self.actual["spec"]["replicas"] = 2
        with self.assertRaisesRegex(RuntimeError, "raw replica scaling"):
            self.controller.validate_object(self.actual, self.expected)
        self.actual["spec"]["replicas"] = 1
        self.actual["spec"]["template"]["image"] = "other"
        with self.assertRaisesRegex(RuntimeError, "configuration drift"):
            self.controller.validate_object(self.actual, self.expected)

    def test_replica_patch_pins_uid_and_resource_version(self):
        captured = []
        self.controller.manifests = lambda _: [self.expected]
        self.controller.get = lambda *_: self.actual
        self.controller.kubectl = lambda args: captured.append(args)
        self.controller.replicas({"id": "k7.a"}, 0)
        patch = json.loads(captured[0][captured[0].index("-p") + 1])
        self.assertEqual(patch[:2], [
            {"op": "test", "path": "/metadata/uid", "value": "original-uid"},
            {"op": "test", "path": "/metadata/resourceVersion", "value": "17"}])
        self.assertEqual(patch[2], {"op": "replace", "path": "/spec/replicas", "value": 0})

    def test_admin_status_is_fenced_by_stable_pod_uid(self):
        node = {"id": "k7.a"}
        pods = [self.pod("pod-uid"), self.pod("pod-uid")]
        self.controller.get = lambda *_: pods.pop(0)
        self.controller.kubectl = lambda _args, _payload=None: {
            "node": node, "error": None,
            "status": {"is_leader": False, "ready": True, "applied": 7}}
        result = self.controller.admin(node, "Status")
        self.assertTrue(result["status"]["ready"])

    def test_admin_discards_status_if_old_pod_starts_terminating(self):
        node = {"id": "k7.a"}
        pods = [self.pod("old-pod"), self.pod("old-pod", deleting=True)]
        self.controller.get = lambda *_: pods.pop(0)
        self.controller.kubectl = lambda _args, _payload=None: {
            "node": node, "error": None,
            "status": {"is_leader": True, "ready": True, "applied": 9}}
        with self.assertRaisesRegex(RuntimeError, "changed during management exchange"):
            self.controller.admin(node, "Status")

    def test_admin_discards_status_if_stateful_pod_is_replaced(self):
        node = {"id": "k7.a"}
        pods = [self.pod("old-pod"), self.pod("new-pod")]
        self.controller.get = lambda *_: pods.pop(0)
        self.controller.kubectl = lambda _args, _payload=None: {
            "node": node, "error": None,
            "status": {"is_leader": True, "ready": True, "applied": 9}}
        with self.assertRaisesRegex(RuntimeError, "changed during management exchange"):
            self.controller.admin(node, "Status")

    def test_kubernetes_readiness_is_required_for_converged_status(self):
        node = {"id": "k7.a"}
        pods = [self.pod("pod-uid", ready=False), self.pod("pod-uid", ready=False)]
        self.controller.get = lambda *_: pods.pop(0)
        self.controller.kubectl = lambda _args, _payload=None: {
            "node": node, "error": None,
            "status": {"is_leader": False, "ready": True, "applied": 7}}
        result = self.controller.admin(node, "Status")
        self.assertFalse(result["status"]["ready"])

    def test_mid_step_desired_change_invalidates_execution(self):
        with tempfile.TemporaryDirectory() as root:
            controller = Controller.__new__(Controller)
            controller.config_path = Path(root) / "desired.json"
            controller.expected_config = {"desired": {"revision": 1}}
            controller.config_path.write_text(json.dumps({"desired": {"revision": 2}}))
            with self.assertRaisesRegex(RuntimeError, "input changed"):
                controller.assert_current_input()


if __name__ == "__main__":
    unittest.main()
