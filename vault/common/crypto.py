"""
Cryptographic integrity and corruption simulation utilities for Vault.
"""
import hashlib


def compute_checksum(data: bytes) -> str:
    """Computes SHA-256 checksum for byte chunk."""
    return hashlib.sha256(data).hexdigest()


def verify_integrity(data: bytes, expected_checksum: str) -> bool:
    """Verifies that data matches the expected SHA-256 checksum."""
    return compute_checksum(data) == expected_checksum


def corrupt_bytes(data: bytes, offset: int = 0) -> bytes:
    """
    Simulates bit-rot / disk corruption by flipping a byte at the given offset.
    Used for fault-injection testing.
    """
    if not data:
        return b"corrupted"
    b_arr = bytearray(data)
    idx = offset % len(b_arr)
    # Flip all bits of the target byte
    b_arr[idx] = b_arr[idx] ^ 0xFF
    return bytes(b_arr)
