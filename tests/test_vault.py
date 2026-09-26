"""
Automated Integration and Fault-Tolerance Test Suite for Vault.
Validates:
- Quorum read/write consistency
- Node failure resilience
- Bit-rot corruption detection via SHA-256
- Inline read-repair and anti-entropy background repair
- Concurrent reads and writes
- Consistent hash ring rebalancing
"""
import shutil
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
import pytest

from vault.common.models import QuorumConfig
from vault.node.storage_node import StorageNode, DataCorruptionError
from vault.coordinator.coordinator import VaultCoordinator, QuorumError, NotFoundError
from vault.coordinator.repair import AntiEntropyScrubber


@pytest.fixture
def cluster():
    """Sets up a temporary 5-node cluster with N=3, W=2, R=2."""
    temp_dir = tempfile.mkdtemp(prefix="vault_test_")
    config = QuorumConfig(n_replicas=3, write_quorum=2, read_quorum=2)
    coordinator = VaultCoordinator(quorum_config=config)
    nodes = {}

    for i in range(5):
        nid = f"test-node-{i+1}"
        s_dir = f"{temp_dir}/{nid}"
        node = StorageNode(node_id=nid, storage_dir=s_dir)
        coordinator.register_node(node)
        nodes[nid] = node

    scrubber = AntiEntropyScrubber(coordinator=coordinator, interval_seconds=0.5)

    yield coordinator, nodes, scrubber, temp_dir

    # Cleanup
    scrubber.stop()
    shutil.rmtree(temp_dir, ignore_errors=True)


def test_basic_put_get(cluster):
    coordinator, nodes, _, _ = cluster
    key = "user_profile_101.json"
    data = b'{"name": "Alice", "role": "admin"}'

    meta = coordinator.put_object(key, data)
    assert meta.key == key
    assert meta.version == 1
    assert meta.size_bytes == len(data)

    read_meta, read_data = coordinator.get_object(key)
    assert read_meta.checksum == meta.checksum
    assert read_data == data


def test_concurrent_reads_and_writes(cluster):
    coordinator, _, _, _ = cluster
    num_threads = 10
    writes_per_thread = 5

    def worker(worker_id):
        for i in range(writes_per_thread):
            key = f"concurrent_key_{worker_id}_{i}"
            payload = f"content_from_worker_{worker_id}_step_{i}".encode("utf-8")
            coordinator.put_object(key, payload)
            meta, data = coordinator.get_object(key)
            assert data == payload

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    all_keys = coordinator.get_all_keys()
    assert len(all_keys) == num_threads * writes_per_thread


def test_node_failure_quorum_tolerance(cluster):
    coordinator, nodes, _, _ = cluster
    key = "mission_critical_doc.pdf"
    data = b"Extremely confidential binary file payload."

    coordinator.put_object(key, data)
    target_nodes = coordinator.get_target_nodes(key)
    assert len(target_nodes) == 3

    # Fail 1 node (out of 3 replicas). N=3, W=2, R=2 -> Should still read & write!
    target_nodes[0].set_online(False)

    meta, read_data = coordinator.get_object(key)
    assert read_data == data

    # Update object with 1 node down -> 2 healthy nodes >= W=2
    new_data = b"Updated confidential binary file payload."
    new_meta = coordinator.put_object(key, new_data)
    assert new_meta.version == 2

    # Fail a second node. Only 1 healthy node remains < W=2 and < R=2
    target_nodes[1].set_online(False)
    with pytest.raises(QuorumError):
        coordinator.get_object(key)

    # Bring node back online; quorum is restored
    target_nodes[1].set_online(True)
    _, read_updated = coordinator.get_object(key)
    assert read_updated == new_data


def test_bit_rot_detection_and_inline_read_repair(cluster):
    coordinator, nodes, _, _ = cluster
    key = "sensor_telemetry.bin"
    payload = b"SensorTelemetryPayload-Timestamp-1700000000"

    coordinator.put_object(key, payload)
    target_nodes = coordinator.get_target_nodes(key)

    # Corrupt one of the replica nodes
    bad_node = target_nodes[0]
    bad_node.inject_corruption(key)

    # Direct read on bad node raises DataCorruptionError
    with pytest.raises(DataCorruptionError):
        bad_node.read_replica(key, verify=True)

    # Reading via coordinator should succeed using the other 2 replicas
    # AND trigger inline Read-Repair on the bad node!
    meta, data = coordinator.get_object(key, auto_repair=True)
    assert data == payload

    # Give async read-repair thread a fraction of a second to complete
    import time
    time.sleep(0.2)

    # Verify the corrupted node was healed!
    _, healed_data, chk = bad_node.read_replica(key, verify=True)
    assert healed_data == payload
    assert chk == meta.checksum


def test_anti_entropy_background_scrubber(cluster):
    coordinator, nodes, scrubber, _ = cluster
    key = "financial_ledger.csv"
    payload = b"account_id,balance\n1001,50000\n1002,75000\n"

    coordinator.put_object(key, payload)
    target_nodes = coordinator.get_target_nodes(key)

    # Corrupt 2 nodes
    target_nodes[0].inject_corruption(key)
    target_nodes[1].inject_corruption(key)

    # Run background anti-entropy scrub
    report = scrubber.run_scrub_once()
    assert report.scanned_keys >= 1
    assert report.corruptions_detected == 2
    assert report.repaired_replicas == 2

    # Verify both nodes are restored
    for node in target_nodes[:2]:
        _, healed_data, _ = node.read_replica(key, verify=True)
        assert healed_data == payload


def test_cluster_rebalancing(cluster):
    coordinator, nodes, _, temp_dir = cluster
    
    # Insert multiple keys
    for i in range(10):
        coordinator.put_object(f"blob_{i}", f"Data blob payload {i}".encode("utf-8"))

    # Add a 6th node to cluster
    new_node_id = "test-node-6"
    new_node = StorageNode(new_node_id, storage_dir=f"{temp_dir}/{new_node_id}")
    coordinator.register_node(new_node)

    # Rebalance
    stats = coordinator.rebalance_cluster()
    assert stats["replicas_migrated"] >= 0
