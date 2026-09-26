"""
Anti-Entropy Scrubber and Auto-Repair Engine for Vault.
Runs background periodic scrubs to detect missing replicas, bit rot, and inconsistent versions.
"""
import time
import logging
import threading
from typing import Dict, List, Optional
from vault.common.models import QuorumConfig

logger = logging.getLogger("vault.repair")


class RepairReport:
    """Summary of a background repair scrub cycle."""
    def __init__(self):
        self.scanned_keys: int = 0
        self.repaired_replicas: int = 0
        self.corruptions_detected: int = 0
        self.missing_replicas_fixed: int = 0
        self.errors: List[str] = []

    def to_dict(self) -> Dict:
        return {
            "scanned_keys": self.scanned_keys,
            "repaired_replicas": self.repaired_replicas,
            "corruptions_detected": self.corruptions_detected,
            "missing_replicas_fixed": self.missing_replicas_fixed,
            "errors": self.errors
        }


class AntiEntropyScrubber:
    """
    Background worker that continuously inspects replica health across nodes,
    fixes bit-rot, and restores under-replicated objects.
    """
    def __init__(self, coordinator, interval_seconds: float = 2.0):
        self.coordinator = coordinator
        self.interval = interval_seconds
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self.last_report = RepairReport()

    def start(self) -> None:
        """Starts background anti-entropy scrubber thread."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._scrub_loop, daemon=True, name="Vault-Scrubber")
        self._thread.start()

    def stop(self) -> None:
        """Stops background scrubber."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

    def run_scrub_once(self) -> RepairReport:
        """
        Executes a single anti-entropy scrub pass across all known keys.
        Detects bit-rot and under-replication, repairing from healthy replicas.
        """
        report = RepairReport()
        keys = self.coordinator.get_all_keys()
        
        for key in keys:
            report.scanned_keys += 1
            try:
                rep_count, corrupt_count, missing_count = self.coordinator.repair_object(key)
                report.repaired_replicas += rep_count
                report.corruptions_detected += corrupt_count
                report.missing_replicas_fixed += missing_count
            except Exception as e:
                report.errors.append(f"Failed to scrub key '{key}': {str(e)}")

        self.last_report = report
        return report

    def _scrub_loop(self) -> None:
        while self._running:
            try:
                self.run_scrub_once()
            except Exception as e:
                logger.error(f"Error during scrub loop: {e}")
            time.sleep(self.interval)
