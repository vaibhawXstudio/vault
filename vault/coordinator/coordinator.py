"""
Vault Coordinator: Manages distributed consensus, quorum writes/reads,
consistent hashing, metadata consistency, and self-healing replicas.
"""
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Set

from vault.cluster.hash_ring import HashRing
from vault.common.crypto import compute_checksum, verify_integrity
from vault.common.models import ObjectMetadata, QuorumConfig
from vault.node.storage_node import StorageNode, DataCorruptionError, NodeOfflineError


class QuorumError(Exception):
    """Raised when quorum requirement (W or R) cannot be satisfied."""
    pass


class NotFoundError(Exception):
    """Raised when an object does not exist in Vault."""
    pass


class VaultCoordinator:
    """
    Coordinates distributed object storage, quorum enforcement, read-repair,
    and automatic self-healing.
    """
    def __init__(self, quorum_config: Optional[QuorumConfig] = None, max_workers: int = 16):
        self.quorum_config = quorum_config or QuorumConfig(n_replicas=3, write_quorum=2, read_quorum=2)
        self.quorum_config.validate()
        
        self.hash_ring = HashRing(vnodes=128)
        self.nodes: Dict[str, StorageNode] = {}
        self.metadata_catalog: Dict[str, ObjectMetadata] = {}
        
        self._meta_lock = threading.RLock()
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="VaultCoord")

    def register_node(self, node: StorageNode) -> None:
        """Registers a storage node and adds it to the hash ring."""
        with self._meta_lock:
            self.nodes[node.node_id] = node
            self.hash_ring.add_node(node.node_id)

    def unregister_node(self, node_id: str) -> None:
        """Removes a storage node from the cluster."""
        with self._meta_lock:
            if node_id in self.nodes:
                self.hash_ring.remove_node(node_id)
                del self.nodes[node_id]

    def get_target_nodes(self, key: str) -> List[StorageNode]:
        """Gets the N target storage nodes for a given key via consistent hashing."""
        node_ids = self.hash_ring.get_preference_list(key, self.quorum_config.n_replicas)
        return [self.nodes[nid] for nid in node_ids if nid in self.nodes]

    def put_object(self, key: str, data: bytes, custom_meta: Optional[Dict[str, str]] = None) -> ObjectMetadata:
        """
        Stores an object with configurable replication (N) and write quorum (W).
        Returns metadata if write quorum is reached.
        """
        checksum = compute_checksum(data)
        size_bytes = len(data)
        
        with self._meta_lock:
            current_meta = self.metadata_catalog.get(key)
            new_version = (current_meta.version + 1) if current_meta else 1
            meta = ObjectMetadata(
                key=key,
                version=new_version,
                size_bytes=size_bytes,
                checksum=checksum,
                created_at=time.time(),
                deleted=False,
                custom_metadata=custom_meta
            )
            self.metadata_catalog[key] = meta

        target_nodes = self.get_target_nodes(key)
        if len(target_nodes) < self.quorum_config.write_quorum:
            raise QuorumError(
                f"Insufficient active nodes: required write quorum={self.quorum_config.write_quorum}, "
                f"available={len(target_nodes)}"
            )

        successes = 0
        errors = []

        def _write_single(node: StorageNode):
            return node.write_replica(key, meta.version, data, checksum)

        futures = {self._executor.submit(_write_single, node): node for node in target_nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                if future.result():
                    successes += 1
            except Exception as e:
                errors.append(f"Node {node.node_id}: {str(e)}")

        if successes < self.quorum_config.write_quorum:
            raise QuorumError(
                f"Write quorum failed for '{key}': required {self.quorum_config.write_quorum}, "
                f"achieved {successes}. Failures: {errors}"
            )

        return meta

    def get_object(self, key: str, auto_repair: bool = True) -> Tuple[ObjectMetadata, bytes]:
        """
        Retrieves an object respecting read quorum (R) and cryptographic verification.
        Performs inline Read-Repair if stale or corrupt replicas are detected.
        """
        with self._meta_lock:
            meta = self.metadata_catalog.get(key)
            if not meta or meta.deleted:
                raise NotFoundError(f"Object '{key}' not found.")

        target_nodes = self.get_target_nodes(key)
        if len(target_nodes) < self.quorum_config.read_quorum:
            raise QuorumError(
                f"Insufficient nodes for read quorum: required {self.quorum_config.read_quorum}, "
                f"available={len(target_nodes)}"
            )

        def _read_single(node: StorageNode):
            try:
                ver, data, chk = node.read_replica(key, verify=True)
                return (node, ver, data, chk, None)
            except Exception as e:
                return (node, None, None, None, e)

        futures = [self._executor.submit(_read_single, node) for node in target_nodes]
        
        valid_reads = []
        stale_or_corrupt_nodes: List[StorageNode] = []

        for future in as_completed(futures):
            node, ver, data, chk, err = future.result()
            if err is None and chk == meta.checksum and ver == meta.version:
                valid_reads.append((node, data))
            else:
                stale_or_corrupt_nodes.append(node)

        if len(valid_reads) < self.quorum_config.read_quorum:
            raise QuorumError(
                f"Read quorum failed for '{key}': required {self.quorum_config.read_quorum}, "
                f"valid replicas obtained {len(valid_reads)}"
            )

        chosen_data = valid_reads[0][1]

        # Inline Read-Repair: asynchronously heal unhealthy replicas
        if auto_repair and stale_or_corrupt_nodes:
            for bad_node in stale_or_corrupt_nodes:
                if bad_node.is_alive:
                    self._executor.submit(
                        bad_node.write_replica, key, meta.version, chosen_data, meta.checksum
                    )

        return meta, chosen_data

    def delete_object(self, key: str) -> bool:
        """Deletes object across quorum replicas."""
        with self._meta_lock:
            meta = self.metadata_catalog.get(key)
            if not meta or meta.deleted:
                raise NotFoundError(f"Object '{key}' not found.")
            meta.deleted = True

        target_nodes = self.get_target_nodes(key)
        deleted_count = 0
        for node in target_nodes:
            try:
                if node.is_alive and node.delete_replica(key):
                    deleted_count += 1
            except Exception:
                pass
        return True

    def repair_object(self, key: str) -> Tuple[int, int, int]:
        """
        Anti-entropy repair pass for a specific key:
        Returns: (repaired_count, corrupt_count, missing_count)
        """
        with self._meta_lock:
            meta = self.metadata_catalog.get(key)
            if not meta or meta.deleted:
                return 0, 0, 0

        target_nodes = self.get_target_nodes(key)
        authoritative_data: Optional[bytes] = None
        corrupted_nodes = []
        missing_nodes = []

        for node in target_nodes:
            if not node.is_alive:
                continue
            try:
                ver, data, chk = node.read_replica(key, verify=True)
                if chk == meta.checksum and ver == meta.version:
                    authoritative_data = data
                else:
                    corrupted_nodes.append(node)
            except DataCorruptionError:
                corrupted_nodes.append(node)
            except FileNotFoundError:
                missing_nodes.append(node)
            except Exception:
                pass

        if authoritative_data is None:
            # Cannot repair if no healthy copy exists
            return 0, len(corrupted_nodes), len(missing_nodes)

        repaired = 0
        nodes_to_heal = corrupted_nodes + missing_nodes
        for node in nodes_to_heal:
            try:
                node.write_replica(key, meta.version, authoritative_data, meta.checksum)
                repaired += 1
            except Exception:
                pass

        return repaired, len(corrupted_nodes), len(missing_nodes)

    def rebalance_cluster(self) -> Dict[str, int]:
        """
        Rebalances all keys across nodes to maintain consistent hash placement.
        Copies data to newly designated replica nodes.
        """
        stats = {"rebalanced_keys": 0, "replicas_migrated": 0}
        keys = self.get_all_keys()
        
        for key in keys:
            try:
                meta, data = self.get_object(key, auto_repair=False)
            except Exception:
                continue
                
            target_nodes = self.get_target_nodes(key)
            migrated_for_key = 0
            for node in target_nodes:
                if not node.is_alive:
                    continue
                try:
                    node.read_replica(key, verify=True)
                except Exception:
                    # Missing or corrupt on new designated node; write it
                    try:
                        node.write_replica(key, meta.version, data, meta.checksum)
                        migrated_for_key += 1
                    except Exception:
                        pass
            if migrated_for_key > 0:
                stats["rebalanced_keys"] += 1
                stats["replicas_migrated"] += migrated_for_key

        return stats

    def get_all_keys(self) -> List[str]:
        """Returns all non-deleted keys."""
        with self._meta_lock:
            return [k for k, v in self.metadata_catalog.items() if not v.deleted]

    def get_cluster_status(self) -> Dict:
        """Returns high-level health report of the cluster."""
        with self._meta_lock:
            total_nodes = len(self.nodes)
            alive_nodes = sum(1 for n in self.nodes.values() if n.is_alive)
            total_objects = len(self.get_all_keys())
            
            node_summaries = {}
            for nid, node in self.nodes.items():
                node_summaries[nid] = {
                    "alive": node.is_alive,
                    "storage_dir": str(node.storage_dir)
                }

            return {
                "total_nodes": total_nodes,
                "alive_nodes": alive_nodes,
                "quorum": {
                    "N": self.quorum_config.n_replicas,
                    "W": self.quorum_config.write_quorum,
                    "R": self.quorum_config.read_quorum,
                    "guarantee": "Strong Consistency (R+W > N)" if (
                        self.quorum_config.read_quorum + self.quorum_config.write_quorum > self.quorum_config.n_replicas
                    ) else "Eventual Consistency"
                },
                "total_objects": total_objects,
                "nodes": node_summaries
            }
