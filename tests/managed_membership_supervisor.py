# SPDX-License-Identifier: Apache-2.0
"""Process identity and document boundary checks for the Linux adapter."""
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import signal
import socket
import struct
import tempfile
import time
import unittest

spec = importlib.util.spec_from_file_location(
    "neuralbase_operator", Path(__file__).parents[1] / "ops/neuralbase_operator.py")
operator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(operator)


def peer(node, mismatch, ready):
    with socket.socket(socket.AF_UNIX) as listener:
        listener.bind(node["socket"])
        listener.listen(1)
        ready.set()
        with listener.accept()[0] as connection:
            size = struct.unpack("!I", operator.receive(connection, 4))[0]
            operator.receive(connection, size)
            response_node = dict(node, id="wrong-incarnation") if mismatch else node
            response = json.dumps({"node": response_node, "pid": os.getpid(), "error": None}).encode()
            connection.sendall(struct.pack("!I", len(response)) + response)
            time.sleep(10)


class SupervisorTests(unittest.TestCase):
    def check_peer(self, mismatch):
        with tempfile.TemporaryDirectory(prefix="nb7-peer-") as root:
            node = {"topology": {"cluster": "test"}, "id": "test.a", "socket": root + "/admin.sock"}
            ready = multiprocessing.Event()
            child = multiprocessing.Process(target=peer, args=(node, mismatch, ready))
            child.start()
            try:
                self.assertTrue(ready.wait(3), "peer did not start")
                if mismatch:
                    with self.assertRaisesRegex(ValueError, "mismatch"):
                        operator.admin(node, "Status", terminate=True)
                    self.assertTrue(child.is_alive(), "mismatched process must not be signaled")
                else:
                    operator.admin(node, "Status", terminate=True)
                    child.join(3)
                    self.assertEqual(child.exitcode, -signal.SIGTERM)
            finally:
                if child.is_alive():
                    child.kill()
                child.join(3)

    def test_matching_socket_identity_can_be_retired(self):
        self.check_peer(False)

    def test_mismatched_incarnation_cannot_be_retired(self):
        self.check_peer(True)

    def test_response_size_is_bounded(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "oversize.json"
            path.write_bytes(b" " * (operator.MAX_BYTES + 1))
            with self.assertRaisesRegex(ValueError, "256 KiB"):
                operator.read_json(path)


if __name__ == "__main__":
    unittest.main()
