"""
Data models and configurations for Vault distributed storage.
"""
from dataclasses import dataclass, asdict
import time
from typing import Optional, Dict, Any


@dataclass
class QuorumConfig:
    """Configurable replication and durability policy."""
    n_replicas: int = 3       # Total replication factor (N)
    write_quorum: int = 2     # Write quorum (W)
    read_quorum: int = 2      # Read quorum (R)

    def validate(self) -> None:
        if self.write_quorum > self.n_replicas:
            raise ValueError(f"Write quorum {self.write_quorum} cannot exceed N={self.n_replicas}")
        if self.read_quorum > self.n_replicas:
            raise ValueError(f"Read quorum {self.read_quorum} cannot exceed N={self.n_replicas}")
        # Note: If R + W > N, strong consistency is guaranteed
        # If R + W <= N, eventual consistency


@dataclass
class ObjectMetadata:
    """Metadata record for an object in Vault."""
    key: str
    version: int
    size_bytes: int
    checksum: str
    created_at: float
    deleted: bool = False
    custom_metadata: Optional[Dict[str, str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ObjectMetadata":
        return cls(**data)


@dataclass
class ReplicaDescriptor:
    """Status of a replica on a specific node."""
    node_id: str
    key: str
    version: int
    checksum: str
    is_corrupt: bool = False
    exists: bool = True
