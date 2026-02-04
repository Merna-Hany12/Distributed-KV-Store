# Distributed Key-Value Store & Search Engine

A robust, fault-tolerant, and persistent key-value store built from scratch in Python. This system goes beyond simple storage by implementing **ACID durability** via Write-Ahead Logging (WAL), **Advanced Search Capabilities** (Semantic & Full-Text), and two distinct distributed consistency models: **Raft (Strong Consistency)** and **Masterless (Eventual Consistency)**.

Designed to demonstrate advanced distributed systems concepts, this project handles process crashes, concurrent race conditions, and network partitioning without data loss.

## 🚀 Key Features

### 🛡️ Core Storage & ACID

* **Write-Ahead Log (WAL):** Ensures durability by appending writes to disk before acknowledgement.
* **Crash Recovery:** Automatically rebuilds state from WAL and Snapshots upon restart.
* **Concurrency Control:** Thread-safe operations using fine-grained locking.
* **Atomicity:** `BulkSet` operations are all-or-nothing.

### 🔍 Advanced Search Engine

Integrated directly into the KV Store (`indexing_module.py`):

* **Full-Text Search:** Inverted index with **TF-IDF** ranking.
* **Semantic Search:** Character n-gram embeddings with Cosine Similarity for finding related concepts (e.g., "coding" finds "python").
* **Phrase Search:** Exact substring matching for complex queries.

### 🌐 Dual Replication Models

The system implements two contrasting distributed architectures side-by-side:

1. **Raft Consensus (Strong Consistency):**
* Leader-Follower topology.
* Automatic Leader Election.
* Log replication to a quorum (majority).


2. **Masterless / Dynamo-Style (Eventual Consistency):**
* Multi-Master: Any node accepts writes.
* **Vector Clocks:** Detects causal ordering and concurrency.
* **Gossip Protocol:** Asynchronous background replication.
* **LWW (Last-Writer-Wins):** Conflict resolution strategy.



---

## 🏗️ Architecture

### System Components

| Module | Description |
| --- | --- |
| **`server.py`** | Core storage engine handling WAL, snapshots, and indexing. |
| **`cluster.py`** | Implements the **Raft** consensus algorithm (Leader/Candidate/Follower). |
| **`masterless_replication.py`** | Implements **Masterless** replication with Vector Clocks. |
| **`indexing_module.py`** | Handles Tokenization, Inverted Indices, and Vector Embeddings. |
| **`main.py`** | Unified CLI entry point for testing, benchmarking, and execution. |

### Replication Comparison

The project includes a direct comparison test suite (`test_replication_mastrless.py`):

| Feature | Raft Cluster | Masterless Cluster |
| --- | --- | --- |
| **Write Availability** | Low (Leader only) | **High (Any Node)** |
| **Consistency** | **Strong (Linearizable)** | Eventual |
| **Conflict Handling** | Prevented by Leader | **Vector Clocks + LWW** |
| **Partition Tolerance** | Stops if no majority | **Continues (AP system)** |

---

## 📦 Installation

No external dependencies are required. The project uses the Python standard library (`socket`, `threading`, `json`, `math`, `subprocess`).

```bash
git clone https://github.com/Merna-Hany12/Distributed-KV-Store.git
cd Distributed-KV-Store

```

---

## 💻 Usage

The system is controlled via `main.py`, which acts as a central hub.

### 1. Run Full Test Suite (Recommended)

Runs Unit tests, ACID crash tests, Raft failovers, Masterless sync tests, Search tests, and Benchmarks.

```bash
python main.py

```

### 2. Run Specific Benchmarks

Isolate specific replication behaviors:

```bash
# Compare Raft vs Masterless logic
python test_replication_mastrless.py

# Test Search Engine features
python test_enhanced.py

```

### 3. Manual Server Startup

Start a standalone node for manual interaction:

```bash
python main.py --server --port 8000 --dir my_data

```

### 4. Manual Cluster Setup

To run a 3-node cluster in separate terminals:

**Option A: Raft Cluster**

```bash
python cluster.py 0   # Terminal 1 (Leader Candidate)
python cluster.py 1   # Terminal 2
python cluster.py 2   # Terminal 3

```

**Option B: Masterless Cluster**

```bash
python masterless_replication.py 0
python masterless_replication.py 1
python masterless_replication.py 2

```

---

## 📊 Benchmarks & Performance

Based on internal benchmarking (`benchmark.py`) on standard hardware:

| Metric | Result | Notes |
| --- | --- | --- |
| **Write Throughput** | **~1,643 ops/sec** | Sustained sequential writes |
| **Degradation** | **0.4%** | From 0 to 50k keys stored |
| **Durability** | **100%** | 0 keys lost after 5 random `SIGKILL` events |
| **Search Speed** | **<10ms** | For In-Memory TF-IDF lookup |

**Durability Test Logic:**
The benchmark spawns a writer thread and a "Killer" thread. The Killer randomly sends `SIGKILL` (or `taskkill /F` on Windows) to the database process while it is writing. Upon restart, the WAL is replayed, and data integrity is verified.

---

## 🧪 Test Coverage

The project includes a rigorous test suite covering:

1. **ACID Compliance:**
* *Isolation:* Concurrent transactions on overlapping keys.
* *Atomicity:* Killing the server mid-batch write.


2. **Search Features:**
* TF-IDF ranking accuracy.
* Semantic embedding cosine similarity.
* Index persistence across restarts.


3. **Distributed Consensus:**
* **Raft:** Killing the Leader node and verifying automatic election and data retention.
* **Masterless:** Concurrent writes to different nodes and verifying eventual consistency via vector clock merging.



## Contributing

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/NewAlgorithm`)
3. Commit your Changes (`git commit -m 'Add Vector Clock optimization'`)
4. Push to the Branch (`git push origin feature/NewAlgorithm`)
5. Open a Pull Request
