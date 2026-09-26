"""
Storage Node implementation for Vault.
Handles persistence, checksum validation, crash simulation, and bit-rot corruption.
"""
import os
import json
import threading
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from vault.common.crypto import compute_checksum, verify_integrity, corrupt_bytes
from vault.common.models import ReplicaDescriptor


class DataCorruptionError(Exception):
    """Raised when on-disk data does not match stored checksum (bit-rot)."""
    pass


class NodeOfflineError(Exception):
    """Raised when an operation is attempted on an offline or partitioned node."""
    pass


class StorageNode:
    """
    An independent storage node storing object chunks with local verification.
    """
    def __init__(self, node_id: str, storage_dir: Optional[str] = None):
        self.node_id = node_id
        self.storage_dir = Path(storage_dir or f"./data_nodes/{node_id}")
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir = self.storage_dir / "meta"
        self.data_dir = self.storage_dir / "data"
        self.meta_dir.mkdir(exist_ok=True)
        self.data_dir.mkdir(exist_ok=True)
        
        self.is_alive = True
        self._lock = threading.RLock()

    def set_online(self, status: bool) -> None:
        """Simulates node crash, power failure, or network partition recovery."""
        with self._lock:
            self.is_alive = status

    def _check_alive(self) -> None:
        if not self.is_alive:
            raise NodeOfflineError(f"Node {self.node_id} is unreachable / offline.")

    def _meta_path(self, key: str) -> Path:
        safe_key = key.replace("/", "_").replace("\\", "_")
        return self.meta_dir / f"{safe_key}.json"

    def _data_path(self, key: str) -> Path:
        safe_key = key.replace("/", "_").replace("\\", "_")
        return self.data_dir / f"{safe_key}.bin"

    def write_replica(self, key: str, version: int, data: bytes, checksum: str) -> bool:
        """
        Persists a replica along with its metadata.
        Atomically writes data first, then metadata.
        """
        self._check_alive()
        with self._lock:
            # Verify data matches checksum before writing
            if not verify_integrity(data, checksum):
                raise ValueError("Write rejected: payload checksum mismatch")

            d_path = self._data_path(key)
            m_path = self._meta_path(key)

            temp_d_path = d_path.with_suffix(".tmp")
            with open(temp_d_path, "wb") as f:
                f.write(data)
                f.flush()
                os.fsync(f.fileno())

            # Atomic replace
            temp_d_path.replace(d_path)

            meta = {
                "key": key,
                "version": version,
                "checksum": checksum,
                "size": len(data)
            }
            with open(m_path, "w", encoding="utf-8") as f:
                json.dump(meta, f)
            return True

    def read_replica(self, key: str, verify: bool = True) -> Tuple[int, bytes, str]:
        """
        Reads replica data and validates integrity against stored checksum.
        Raises DataCorruptionError if bit-rot is detected.
        """
        self._check_alive()
        with self._lock:
            m_path = self._meta_path(key)
            d_path = self._data_path(key)

            if not m_path.exists() or not d_path.exists():
                raise FileNotFoundError(f"Replica for key '{key}' not found on node {self.node_id}")

            with open(m_path, "r", encoding="utf-8") as f:
                meta = json.load(f)

            with open(d_path, "rb") as f:
                data = f.read()

            if verify:
                if not verify_integrity(data, meta["checksum"]):
                    raise DataCorruptionError(
                        f"Integrity check failed for key '{key}' on node {self.node_id}. "
                        f"Expected {meta['checksum']}, got {compute_checksum(data)}"
                    )

            return meta["version"], data, meta["checksum"]

    def delete_replica(self, key: str) -> bool:
        """Deletes replica data and metadata."""
        self._check_alive()
        with self._lock:
            m_path = self._meta_path(key)
            d_path = self._data_path(key)
            deleted = False
            if m_path.exists():
                m_path.unlink()
                deleted = True
            if d_path.exists():
                d_path.unlink()
                deleted = True
            return deleted

    def list_replicas(self) -> List[ReplicaDescriptor]:
        """Scans local metadata for all stored replicas on this node."""
        self._check_alive()
        with self._lock:
            results = []
            for m_path in self.meta_dir.glob("*.json"):
                try:
                    with open(m_path, "r", encoding="utf-8") as f:
                        meta = json.load(f)
                    d_path = self._data_path(meta["key"])
                    is_corrupt = False
                    if not d_path.exists():
                        is_corrupt = True
                    else:
                        with open(d_path, "rb") as f:
                            d = f.read()
                        if not verify_integrity(d, meta["checksum"]):
                            is_corrupt = True
                    results.append(ReplicaDescriptor(
                        node_id=self.node_id,
                        key=meta["key"],
                        version=meta["version"],
                        checksum=meta["checksum"],
                        is_corrupt=is_corrupt,
                        exists=True
                    ))
                except Exception:
                    continue
            return results

    def inject_corruption(self, key: str) -> bool:
        """
        Fault injection: artificially corrupts replica bytes on disk to test
        detection and auto-healing.
        """
        with self._lock:
            d_path = self._data_path(key)
            if not d_path.exists():
                return False
            with open(d_path, "rb") as f:
                data = f.read()
            corrupted = corrupt_bytes(data, offset=0)
            with open(d_path, "wb") as f:
                f.write(corrupted)
            return True
