import sys
import time
from server import KVServer
from client import KVClient
from tests import *
from benchmark import *
from test_enhanced import run_all_tests as run_enhanced_tests
from test_replication_mastrless import run_raft_tests, run_masterless_tests, print_comparison


# --- Logger Class to save output to file ---
class Logger(object):
    def __init__(self, filename="result.txt"):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()


def print_usage():
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║          PERSISTENT KEY-VALUE STORE - USAGE INSTRUCTIONS             ║
╚══════════════════════════════════════════════════════════════════════╝

📋 OPTION 1: Run All Tests and Benchmarks
───────────────────────────────────────────────────────────────────────
python main.py

Runs in this order:
  1. Basic functional tests
  2. ACID tests (Isolation, Atomicity, Chaos)
  3. Raft replication tests        ← cluster.py
  4. Masterless replication tests  ← masterless.py
  5. Raft vs Masterless comparison table
  6. Enhanced feature tests (Search, Semantic, Phrase)
  7. Write throughput benchmark
  8. Durability benchmark

All output saved to result.txt


🖥️  OPTION 2: Run Server Manually
───────────────────────────────────────────────────────────────────────
python main.py --server [--port PORT] [--dir DATA_DIR]

  python main.py --server
  python main.py --server --port 8080 --dir my_data

Defaults: port=8000, dir=kvstore_data/


🔄 OPTION 3: Run Replication Tests Only
───────────────────────────────────────────────────────────────────────
python test_replication.py

Runs Raft + Masterless tests and prints comparison table.


🔄 OPTION 4: Run a Single Cluster Type
───────────────────────────────────────────────────────────────────────
# Raft — 3 nodes
python cluster.py 0    # Terminal 1
python cluster.py 1    # Terminal 2
python cluster.py 2    # Terminal 3

# Masterless — 3 nodes
python masterless.py 0  # Terminal 1
python masterless.py 1  # Terminal 2
python masterless.py 2  # Terminal 3


📁 Files Created:
───────────────────────────────────────────────────────────────────────
kvstore_data/          # Default single-node data
masterless_data/       # Masterless cluster data
  ├── node_0/
  ├── node_1/
  └── node_2/
result.txt             # Full test + benchmark output


❓ Help:
───────────────────────────────────────────────────────────────────────
python main.py --help
    """)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] in ('--help', '-h'):
            print_usage()
            sys.exit(0)

        elif sys.argv[1] == '--server':
            port = int(sys.argv[3]) if len(sys.argv) > 3 else 8000
            data_dir = sys.argv[5] if len(sys.argv) > 5 else "kvstore_data"
            server = KVServer(port=port, data_dir=data_dir)
            try:
                server.start()
            except KeyboardInterrupt:
                print("\nShutting down server gracefully...")
                server.stop()
        else:
            print(f"Unknown option: {sys.argv[1]}")
            print_usage()
            sys.exit(1)

    else:
        # --- ENABLE LOGGING ---
        sys.stdout = Logger("result.txt")

        print("=" * 60)
        print("Key-Value Store - Full Test Suite")
        print("=" * 60)

        # -------------------------------------------------------
        # 1. Basic Functional Tests
        # -------------------------------------------------------
        run_basic_tests()

        # -------------------------------------------------------
        # 2. ACID Tests
        # -------------------------------------------------------
        test_concurrent_isolation()
        test_crash_atomicity()
        test_chaos_mode()

        # -------------------------------------------------------
        # 3. Raft Replication Tests
        # -------------------------------------------------------
        raft_results = run_raft_tests()
        time.sleep(3)  # Let ports release on Windows

        # -------------------------------------------------------
        # 4. Masterless Replication Tests
        # -------------------------------------------------------
        masterless_results = run_masterless_tests()
        time.sleep(3)

        # -------------------------------------------------------
        # 5. Raft vs Masterless Comparison Table
        # -------------------------------------------------------
        print_comparison(raft_results, masterless_results)

        # -------------------------------------------------------
        # 6. Enhanced Feature Tests (Search)
        # -------------------------------------------------------
        run_enhanced_tests()

        # -------------------------------------------------------
        # 7. Write Throughput Benchmark
        # -------------------------------------------------------
        benchmark_write_throughput()

        # -------------------------------------------------------
        # 8. Durability Benchmark
        # -------------------------------------------------------
        benchmark_durability()

        print("\n" + "=" * 60)
        print("All tests and benchmarks completed!")
        print("Results saved to result.txt")
        print("=" * 60)