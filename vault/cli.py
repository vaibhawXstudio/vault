"""
Command Line Interface & Live Demo Runner for Vault.
Provides interactive verification of quorum, node failures, bit-rot, and auto-repair.
"""
import sys
import time
import argparse
from pathlib import Path
from typing import Tuple, Dict
from vault.common.models import QuorumConfig
from vault.node.storage_node import StorageNode
from vault.coordinator.coordinator import VaultCoordinator
from vault.coordinator.repair import AntiEntropyScrubber


def setup_demo_cluster(node_count: int = 5, base_dir: str = "./demo_nodes") -> Tuple:
    """Sets up a local multi-node cluster for testing."""
    config = QuorumConfig(n_replicas=3, write_quorum=2, read_quorum=2)
    coordinator = VaultCoordinator(quorum_config=config)
    nodes = {}
    
    for i in range(node_count):
        nid = f"node-{i+1}"
        s_dir = f"{base_dir}/{nid}"
        node = StorageNode(node_id=nid, storage_dir=s_dir)
        coordinator.register_node(node)
        nodes[nid] = node
        
    scrubber = AntiEntropyScrubber(coordinator=coordinator, interval_seconds=1.0)
    return coordinator, nodes, scrubber


def run_full_demo():
    """Runs a complete end-to-end demonstration of all Vault fault-tolerance capabilities."""
    print("=" * 70)
    print("  VAULT: FAULT-TOLERANT DISTRIBUTED OBJECT STORAGE SYSTEM")
    print("=" * 70)
    
    import shutil
    shutil.rmtree("./demo_nodes", ignore_errors=True)
    
    print("\n[Step 1] Initializing 5 Storage Nodes with N=3, W=2, R=2 Quorum Policy...")
    coordinator, nodes, scrubber = setup_demo_cluster(node_count=5)
    print(f"Cluster Status: {coordinator.get_cluster_status()['alive_nodes']} / {len(nodes)} nodes online.")
    print("Strong Consistency Guarantee: (R + W = 4) > (N = 3)")

    print("\n[Step 2] Storing 'dataset_v1.bin' (concurrent quorum write)...")
    payload = b"Hello, Vault Distributed Storage! Critical high-integrity payload."
    meta = coordinator.put_object("dataset_v1.bin", payload)
    target_nodes = [n.node_id for n in coordinator.get_target_nodes("dataset_v1.bin")]
    print(f"-> Successfully written across quorum. Target Nodes: {target_nodes}")
    print(f"-> Version: {meta.version}, SHA-256: {meta.checksum[:16]}..., Size: {meta.size_bytes}B")

    print("\n[Step 3] Simulating Independent Storage Node Crash...")
    failed_node_id = target_nodes[0]
    nodes[failed_node_id].set_online(False)
    print(f"-> Simulated crash of storage node [{failed_node_id}] (Offline)")

    print("\n[Step 4] Reading data during active node failure...")
    meta_read, data_read = coordinator.get_object("dataset_v1.bin")
    print(f"-> Read Quorum Succeeded! Verified payload: '{data_read.decode('utf-8')}'")
    print("-> System tolerated node failure without data loss or downtime.")

    print("\n[Step 5] Simulating Silent Disk Bit-Rot / Data Corruption...")
    second_node_id = target_nodes[1]
    nodes[second_node_id].inject_corruption("dataset_v1.bin")
    print(f"-> Injected byte corruption into disk replica on [{second_node_id}]")

    print("\n[Step 6] Running Anti-Entropy Scrubber to detect & auto-repair bit rot...")
    # Bring the first node back online so quorum can heal
    nodes[failed_node_id].set_online(True)
    print(f"-> Node [{failed_node_id}] recovered online.")
    
    report = scrubber.run_scrub_once()
    print(f"-> Scrub Complete: Scanned={report.scanned_keys}, Corruptions Detected={report.corruptions_detected}, Repaired={report.repaired_replicas}")

    print("\n[Step 7] Verifying Repaired Node Integrity...")
    _, healed_data, chk = nodes[second_node_id].read_replica("dataset_v1.bin", verify=True)
    print(f"-> Repaired Node [{second_node_id}] replica integrity verified! Checksum: {chk[:16]}...")
    assert healed_data == payload, "Repaired data must match original payload"

    print("\n[Step 8] Scaling Cluster & Demonstrating Consistent Hash Rebalancing...")
    new_node = StorageNode("node-6", storage_dir="./demo_nodes/node-6")
    coordinator.register_node(new_node)
    print("-> Added 'node-6' to cluster. Ring dynamically adjusted via virtual nodes.")
    rebalance_stats = coordinator.rebalance_cluster()
    print(f"-> Rebalance Complete: Migrated {rebalance_stats['replicas_migrated']} replicas to balance distribution.")

    print("\n" + "=" * 70)
    print("  ALL FAULT-TOLERANCE AND SELF-HEALING CHECKS PASSED SUCCESSFULLY! ")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Vault Distributed Storage CLI")
    parser.add_argument("command", choices=["demo", "status"], default="demo", nargs="?", help="Command to run")
    args = parser.parse_args()

    if args.command == "demo":
        run_full_demo()
    elif args.command == "status":
        print("Vault is ready. Run 'python -m vault.cli demo' for live demo.")


if __name__ == "__main__":
    main()
