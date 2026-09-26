# 🏛️ Vault: Fault-Tolerant Distributed Object Storage System

**Vault** is a distributed object storage architecture engineered to reliably store, replicate, retrieve, and self-heal objects across independently failing and unreliable storage nodes.

It implements configurable quorum replication, consistent hashing with virtual nodes, silent bit-rot detection via cryptographic checksums, inline read-repair, and an active anti-entropy background scrubber.

---

## 🚀 Key Architectural Features

- **Tunable Quorum & Consistency Guarantees**:
  - Configurable $N$ (Replication Factor), $W$ (Write Quorum), and $R$ (Read Quorum).
  - Strong consistency enforced when $R + W > N$.
  - Eventual consistency supported for high-throughput write workloads.
- **Consistent Hashing with Virtual Nodes**:
  - Deterministic data distribution across nodes using MD5/SHA-256 ring hashing.
  - 128 virtual nodes per physical host to eliminate hot spots and guarantee uniform spread.
  - Minimal data migration during dynamic cluster scaling (adding/removing nodes).
- **Data Integrity & Bit-Rot Detection**:
  - End-to-end SHA-256 checksum verification on every read and write.
  - Immediate detection of silent on-disk byte corruption / bit-rot.
- **Two-Tier Self-Healing & Repair**:
  - **Inline Read-Repair**: Asynchronous background repair of stale or corrupted replicas whenever a read request detects an anomaly across quorum responses.
  - **Anti-Entropy Scrubber**: Periodic background daemon that traverses replicas, flags under-replicated or corrupted chunks, and reconstitutes them from verified healthy replicas.
- **Fault Tolerance & Partition Resilience**:
  - Uninterrupted reads and writes during single or multi-node offline failures as long as quorum is satisfied.
  - Clean error propagation (`QuorumError`) when partition limits are exceeded, preventing split-brain or dirty reads.
- **Zero Third-Party Runtime Dependencies**:
  - Built using Python 3.9+ standard library (`threading`, `hashlib`, `concurrent.futures`, `json`, `pathlib`, `unittest`).

---

## 📐 System Architecture

```text
                        ┌─────────────────────────────────┐
                        │          Client / CLI           │
                        └───────────────┬─────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             VAULT COORDINATOR                               │
│  - Consistent Hash Ring (128 vnodes/node)                                   │
│  - Metadata Catalog (versioning, locks, timestamps)                         │
│  - Quorum Consensus Engine (N, W, R)                                        │
│  - Inline Read-Repair Dispatcher                                            │
└────────┬──────────────────────────────┬─────────────────────────────┬───────┘
         │                              │                             │
         ▼                              ▼                             ▼
┌─────────────────┐            ┌─────────────────┐           ┌─────────────────┐
│ Storage Node 1  │            │ Storage Node 2  │           │ Storage Node 3  │
│  - Data Store   │            │  - Data Store   │           │  - Data Store   │
│  - SHA-256 Meta │            │  - SHA-256 Meta │           │  - SHA-256 Meta │
│  - Integrity Ver│            │  - Integrity Ver│           │  - Integrity Ver│
└─────────────────┘            └─────────────────┘           └─────────────────┘
         ▲                              ▲                             ▲
         └──────────────────────────────┼─────────────────────────────┘
                                        │
                        ┌───────────────┴─────────────────┐
                        │      Anti-Entropy Scrubber      │
                        │    (Periodic Self-Healing)      │
                        └─────────────────────────────────┘
```

---

## 📂 Project Structure

```text
vault/
├── vault/
│   ├── __init__.py
│   ├── cli.py                     # Interactive CLI and live fault-injection demo runner
│   ├── common/
│   │   ├── __init__.py
│   │   ├── crypto.py              # SHA-256 checksum computation & bit-rot simulator
│   │   └── models.py              # Data classes for ObjectMetadata, QuorumConfig
│   ├── cluster/
│   │   ├── __init__.py
│   │   └── hash_ring.py           # Consistent hashing ring with virtual nodes
│   ├── node/
│   │   ├── __init__.py
│   │   └── storage_node.py        # Independent node with atomic writes & integrity checks
│   └── coordinator/
│       ├── __init__.py
│       ├── coordinator.py         # Quorum controller, metadata catalog, read-repair
│       └── repair.py              # Background anti-entropy scrubber daemon
├── tests/
│   ├── __init__.py
│   ├── test_vault.py              # Pytest test suite
│   └── run_tests.py               # Standalone standard-library unittest runner
├── requirements.txt               # Optional dev dependencies
└── README.md                      # Documentation & architecture specification
```

---

## ⚡ Quickstart & Live Demo

### 1. Run the Fault-Tolerance Simulation Demo
Experience a live multi-node failure, bit-rot injection, quorum read, and automatic self-healing cycle:
```bash
python -m vault.cli demo
```

**What the demo demonstrates:**
1. Spawns 5 storage nodes with $N=3, W=2, R=2$.
2. Performs concurrent quorum write of an object.
3. Kills a storage node and proves read operations continue without interruption.
4. Injects silent disk bit-rot (flips bytes) into a surviving replica.
5. Runs the Anti-Entropy Scrubber to detect the bad checksum and auto-heal the replica from healthy nodes.
6. Dynamically adds a new node and triggers consistent hash rebalancing.

---

### 2. Run the Automated Test Suite

Zero installation required—runs immediately on any standard Python 3.9+ installation:
```bash
python -m tests.run_tests
```

Or using pytest if installed:
```bash
pytest -v
```

---

## 💻 Python API Usage

```python
from vault.coordinator.coordinator import VaultCoordinator
from vault.node.storage_node import StorageNode
from vault.common.models import QuorumConfig

# 1. Initialize cluster with Strong Consistency (R + W = 4 > N = 3)
config = QuorumConfig(n_replicas=3, write_quorum=2, read_quorum=2)
coordinator = VaultCoordinator(quorum_config=config)

# 2. Register storage nodes
for i in range(5):
    node = StorageNode(node_id=f"node-{i+1}", storage_dir=f"./data/node_{i+1}")
    coordinator.register_node(node)

# 3. Store object (concurrent write across quorum)
meta = coordinator.put_object("dataset.csv", b"col1,col2\nval1,val2\n")
print(f"Stored version {meta.version}, SHA-256: {meta.checksum}")

# 4. Retrieve object (verified via read quorum with auto-repair)
meta, data = coordinator.get_object("dataset.csv")
print(f"Retrieved: {data.decode('utf-8')}")

# 5. Check cluster health
status = coordinator.get_cluster_status()
print(status)
```

---

## 🛡️ Fault Tolerance & Recovery Guarantees

| Failure Scenario | System Behavior | Recovery Mechanism |
| :--- | :--- | :--- |
| **Single Node Crash** | Write and Read quorums remain satisfied ($\ge W, \ge R$). No client impact. | Handled transparently by remaining quorum nodes. |
| **Network Partition ($< W$ nodes)** | Coordinator rejects write with `QuorumError`, preventing inconsistent state. | Writes succeed once partition heals and quorum re-assembles. |
| **Silent Bit-Rot / Corruption** | Read detects SHA-256 mismatch and rejects corrupted node. | **Inline Read-Repair** writes healthy copy immediately. |
| **Stale / Lagging Replica** | Reads compare version vectors; the newest valid replica is served. | Read-Repair or Anti-Entropy Scrubber syncs stale node. |
| **Cluster Node Addition / Loss** | Virtual nodes remap only $1/N$ of keys across the ring. | `rebalance_cluster()` redistributes keys to new target nodes. |

---

## 📤 Pushing to Your GitHub Repository

Initialize and push this project to GitHub in 3 simple steps:

```bash
# 1. Initialize git and stage all files
git init
git add .
git commit -m "feat: complete fault-tolerant distributed object storage system (Vault)"

# 2. Link your GitHub repository
# Replace <YOUR_GITHUB_REPO_URL> with your actual repository URL:
git remote add origin <YOUR_GITHUB_REPO_URL>

# 3. Push to main branch
git branch -M main
git push -u origin main
```
