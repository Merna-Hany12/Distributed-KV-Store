

# Distributed-KV-Store

A robust, fault-tolerant, and persistent key-value store built from scratch in Python. This system implements a custom **Write-Ahead Log (WAL)** for ACID durability and a **Raft-based consensus algorithm** for distributed consistency and leader election.

Designed to demonstrate core database engineering concepts, this project survives process crashes (`SIGKILL`), disk failures, and leader node outages without data loss.

## Key Features

### ACID Compliance

* **Atomicity:** Batch writes (`bulk_set`) are all-or-nothing. Partial writes resulting from crashes are safely rolled back.
* **Consistency:** Data remains consistent across the cluster via log replication.
* **Isolation:** Concurrent transactions are serialized to prevent race conditions.
* **Durability:** Data is flushed (`fsync`) to a binary Write-Ahead Log (WAL) before confirmation to the client.

### Distributed Consensus (Raft-Lite)

* **Leader Election:** Automatic failover occurs if the active leader node fails.
* **Log Replication:** Writes are only committed once replicated to a quorum (majority) of nodes.
* **Heartbeats:** Constant monitoring of node health within the 3-node cluster.

### High Performance

* **Group Commit:** Batches multiple client requests into single disk I/O operations to minimize latency.
* **Optimized Throughput:** Achieves approximately 1,600 writes/sec on standard hardware (limited by HDD/SSD latency).

## Architecture

The system is composed of two main layers:

1. **Consensus Module (`cluster.py`)**
* Manages the node state (Follower, Candidate, Leader).
* Handles "Heartbeats" and Vote Requests.
* Ensures strong consistency (writes are only committed when replicated to a quorum).


2. **Network Server (`server.py`)**
* Manages the in-memory hash map and the on-disk Write-Ahead Log.
* Handles crash recovery by replaying the WAL on startup.
* Performs atomic snapshots to compact the log.
* A multi-threaded TCP server that handles client connections.
* Implements a custom JSON-based wire protocol.



## Installation and Usage

### 1. Clone the Repository

```bash
git clone https://github.com/Merna-Hany12/Distributed-KV-Store.git
cd Distributed-KV-Store

```

### 2. Run the Full Test Suite

The easiest way to execute the system is to run the main driver. This runs functional tests, ACID crash tests, and the Cluster Failover simulation.

```bash
python main.py

```

### 3. Manual Cluster Setup

To run the 3-node cluster manually in separate terminals:

**Terminal 1 (Node 0):**

```bash
python cluster.py 0

```

**Terminal 2 (Node 1):**

```bash
python cluster.py 1

```

**Terminal 3 (Node 2):**

```bash
python cluster.py 2

```

## Benchmark Results

Running the `benchmark.py` suite yields the following performance metrics on a standard machine:

| Metric | Result |
| --- | --- |
| **Throughput** | ~1,620 writes/sec (Sequential) |
| **Durability** | 100% (0 keys lost after 5 hard crashes) |
| **Failover Time** | ~1.5 - 3.0 seconds (Configurable timeout) |

## Test Scenarios

The project includes a rigorous test suite (`tests.py`) covering the following scenarios:

* **Concurrent Isolation:** Verification of thread safety when two clients write to the same keys simultaneously.
* **Crash Atomicity:** Sending `SIGKILL` to the server mid-write to verify transaction rollback.
* **Chaos Engineering:** Simulating disk I/O errors during snapshotting to ensure data integrity.
* **Leader Failover:** Terminating the active Leader node and verifying that the Cluster promotes a new leader and retains data.

## Contributing

Contributions are welcome. Please fork the repository and submit a pull request for review.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/NewFeature`)
3. Commit your Changes (`git commit -m 'Add NewFeature'`)
4. Push to the Branch (`git push origin feature/NewFeature`)
5. Open a Pull Request
