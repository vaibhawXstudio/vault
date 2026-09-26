"""
Consistent Hash Ring with Virtual Nodes for uniform data distribution
and deterministic replica placement.
"""
import hashlib
import bisect
from typing import List, Dict, Set


class HashRing:
    """
    Consistent Hash Ring with virtual nodes (vnodes) to balance load
    and minimize data migration when nodes are added or removed.
    """
    def __init__(self, vnodes: int = 128):
        self.vnodes = vnodes
        self.ring: List[int] = []  # Sorted hash keys
        self.ring_map: Dict[int, str] = {}  # hash -> node_id
        self.nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        """Computes 32-bit integer hash from MD5."""
        return int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        """Adds a physical node to the ring with virtual replicas."""
        if node_id in self.nodes:
            return
        self.nodes.add(node_id)
        for i in range(self.vnodes):
            vkey = f"{node_id}#vnode-{i}"
            h = self._hash(vkey)
            self.ring_map[h] = node_id
            bisect.insort(self.ring, h)

    def remove_node(self, node_id: str) -> None:
        """Removes a physical node and its virtual nodes from the ring."""
        if node_id not in self.nodes:
            return
        self.nodes.remove(node_id)
        for i in range(self.vnodes):
            vkey = f"{node_id}#vnode-{i}"
            h = self._hash(vkey)
            if h in self.ring_map:
                del self.ring_map[h]
                idx = bisect.bisect_left(self.ring, h)
                if idx < len(self.ring) and self.ring[idx] == h:
                    self.ring.pop(idx)

    def get_preference_list(self, key: str, count: int) -> List[str]:
        """
        Returns an ordered list of `count` distinct physical nodes responsible
        for storing the replicas of `key`.
        """
        if not self.nodes:
            return []
        
        target_count = min(count, len(self.nodes))
        h = self._hash(key)
        idx = bisect.bisect_right(self.ring, h) % len(self.ring)
        
        chosen: List[str] = []
        seen: Set[str] = set()
        
        ring_len = len(self.ring)
        for step in range(ring_len):
            curr_idx = (idx + step) % ring_len
            node = self.ring_map[self.ring[curr_idx]]
            if node not in seen:
                seen.add(node)
                chosen.append(node)
                if len(chosen) == target_count:
                    break
                    
        return chosen
