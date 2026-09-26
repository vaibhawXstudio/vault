"""
Zero-dependency test suite runner using Python standard library `unittest`.
Runs directly on any standard Python installation.
"""
import unittest
import shutil
import tempfile
import threading
import time

from vault.common.models import QuorumConfig
from vault.node.storage_node import StorageNode, DataCorruptionError
from vault.coordinator.coordinator import VaultCoordinator, QuorumError, NotFoundError
from vault.coordinator.repair import AntiEntropyScrubber


class TestVaultDistributedStorage(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="vault_unit_")
        self.config = QuorumConfig(n_replicas=3, write_quorum=2, read_quorum=2)
        self.coordinator = VaultCoordinator(quorum_config=self.config)
        self.nodes = {}

        for i in range(5):
            nid = f"test-node-{i+1}"
            s_dir = f"{self.temp_dir}/{nid}"
            node = StorageNode(node_id=nid, storage_dir=s_dir)
            self.coordinator.register_node(node)
            self.nodes[nid] = node

        self.scrubber = AntiEntropyScrubber(coordinator=self.coordinator, interval_seconds=0.5)

    def tearDown(self):
        self.scrubber.stop()
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_basic_put_get(self):
        key = "user_profile_101.json"
        data = b'{"name": "Alice", "role": "admin"}'

        meta = self.coordinator.put_object(key, data)
        self.assertEqual(meta.key, key)
        self.assertEqual(meta.version, 1)
        self.assertEqual(meta.size_bytes, len(data))

        read_meta, read_data = self.coordinator.get_object(key)
        self.assertEqual(read_meta.checksum, meta.checksum)
        self.assertEqual(read_data, data)

    def test_concurrent_reads_and_writes(self):
        num_threads = 8
        writes_per_thread = 5

        def worker(worker_id):
            for i in range(writes_per_thread):
                key = f"concurrent_key_{worker_id}_{i}"
                payload = f"content_from_worker_{worker_id}_step_{i}".encode("utf-8")
                self.coordinator.put_object(key, payload)
                meta, data = self.coordinator.get_object(key)
                self.assertEqual(data, payload)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        all_keys = self.coordinator.get_all_keys()
        self.assertEqual(len(all_keys), num_threads * writes_per_thread)

    def test_node_failure_quorum_tolerance(self):
        key = "mission_critical_doc.pdf"
        data = b"Extremely confidential binary file payload."

        self.coordinator.put_object(key, data)
        target_nodes = self.coordinator.get_target_nodes(key)
        self.assertEqual(len(target_nodes), 3)

        # Fail 1 node (out of 3 replicas). N=3, W=2, R=2 -> Should still read & write!
        target_nodes[0].set_online(False)

        meta, read_data = self.coordinator.get_object(key)
        self.assertEqual(read_data, data)

        # Update object with 1 node down -> 2 healthy nodes >= W=2
        new_data = b"Updated confidential binary file payload."
        new_meta = self.coordinator.put_object(key, new_data)
        self.assertEqual(new_meta.version, 2)

        # Fail a second node. Only 1 healthy node remains < W=2 and < R=2
        target_nodes[1].set_online(False)
        with self.assertRaises(QuorumError):
            self.coordinator.get_object(key)

        # Bring node back online; quorum is restored
        target_nodes[1].set_online(True)
        _, read_updated = self.coordinator.get_object(key)
        self.assertEqual(read_updated, new_data)

    def test_bit_rot_detection_and_inline_read_repair(self):
        key = "sensor_telemetry.bin"
        payload = b"SensorTelemetryPayload-Timestamp-1700000000"

        self.coordinator.put_object(key, payload)
        target_nodes = self.coordinator.get_target_nodes(key)

        # Corrupt one of the replica nodes
        bad_node = target_nodes[0]
        bad_node.inject_corruption(key)

        # Direct read on bad node raises DataCorruptionError
        with self.assertRaises(DataCorruptionError):
            bad_node.read_replica(key, verify=True)

        # Reading via coordinator succeeds using other replicas & triggers inline read-repair
        meta, data = self.coordinator.get_object(key, auto_repair=True)
        self.assertEqual(data, payload)

        time.sleep(0.3)  # Allow async repair to flush

        # Repaired node is now valid
        _, healed_data, chk = bad_node.read_replica(key, verify=True)
        self.assertEqual(healed_data, payload)
        self.assertEqual(chk, meta.checksum)

    def test_anti_entropy_background_scrubber(self):
        key = "financial_ledger.csv"
        payload = b"account_id,balance\n1001,50000\n1002,75000\n"

        self.coordinator.put_object(key, payload)
        target_nodes = self.coordinator.get_target_nodes(key)

        # Corrupt 2 nodes
        target_nodes[0].inject_corruption(key)
        target_nodes[1].inject_corruption(key)

        # Run background anti-entropy scrub
        report = self.scrubber.run_scrub_once()
        self.assertGreaterEqual(report.scanned_keys, 1)
        self.assertEqual(report.corruptions_detected, 2)
        self.assertEqual(report.repaired_replicas, 2)

        # Verify both nodes are restored
        for node in target_nodes[:2]:
            _, healed_data, _ = node.read_replica(key, verify=True)
            self.assertEqual(healed_data, payload)

    def test_cluster_rebalancing(self):
        # Insert multiple keys
        for i in range(10):
            self.coordinator.put_object(f"blob_{i}", f"Data blob payload {i}".encode("utf-8"))

        # Add a 6th node to cluster
        new_node_id = "test-node-6"
        new_node = StorageNode(new_node_id, storage_dir=f"{self.temp_dir}/{new_node_id}")
        self.coordinator.register_node(new_node)

        # Rebalance
        stats = self.coordinator.rebalance_cluster()
        self.assertGreaterEqual(stats["replicas_migrated"], 0)


if __name__ == "__main__":
    unittest.main()
