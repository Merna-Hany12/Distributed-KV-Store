import sys
import time
from server import KVServer
from client import KVClient
from tests import *
from benchmark import *
from test_enhanced import run_all_tests as run_enhanced_tests


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
    """Print usage instructions."""
    print("""
╔══════════════════════════════════════════════════════════════════════╗
║          PERSISTENT KEY-VALUE STORE - USAGE INSTRUCTIONS             ║
╚══════════════════════════════════════════════════════════════════════╝

📋 OPTION 1: Run All Tests and Benchmarks (Recommended First Run)
───────────────────────────────────────────────────────────────────────
python main.py

This will:
  ✓ Run basic functional tests
  ✓ Run ACID tests (Isolation, Atomicity, Chaos)
  ✓ Run Cluster Replication & Failover test
  ✓ Run Enhanced Feature tests (Search, Semantic, Phrase)
  ✓ Benchmark write throughput
  ✓ Benchmark durability (random crashes)
  ✓ SAVE RESULTS to 'result.txt'


🖥️  OPTION 2: Run Server Manually
───────────────────────────────────────────────────────────────────────
python main.py --server [--port PORT] [--dir DATA_DIR]

Examples:
  python main.py --server
  python main.py --server --port 8080 --dir my_data

Defaults:
  - Port: 8000
  - Data directory: kvstore_data/

Stop server: Press Ctrl+C


💻 OPTION 3: Use the Client Programmatically
───────────────────────────────────────────────────────────────────────
from client import KVClient

client = KVClient(host='localhost', port=8000)

# Basic operations
client.Set('username', 'alice')
value = client.Get('username')       # Returns 'alice'
client.Delete('username')
client.BulkSet([('k1','v1'),('k2','v2')])

# Search operations
client.FullTextSearch('search query')
client.PhraseSearch('exact phrase')
client.SemanticSearch('semantic query', top_k=5)
client.SaveIndexes()

client.close()


📁 Files Created:
───────────────────────────────────────────────────────────────────────
kvstore_data/
  └── node_0/
        ├── wal.log          # Write-ahead log (durability)
        ├── snapshot.json    # Periodic snapshot
        └── indexes.json     # Search indexes

result.txt                   # All test + benchmark output


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
        print("Key-Value Store - Tests and Benchmarks")
        print("=" * 60)

        # 1. Basic Functional Tests
        run_basic_tests()

        # 2. ACID Tests
        test_concurrent_isolation()
        test_crash_atomicity()
        test_chaos_mode()

        # 3. Cluster Test
        test_cluster_failover()

        # 4. Enhanced Feature Tests (Search, Semantic, Phrase, Persistence)
        run_enhanced_tests()

        # 5. Performance Benchmarks
        benchmark_write_throughput()
        benchmark_durability()

        print("\n" + "=" * 60)
        print("All tests and benchmarks completed!")
        print("Results saved to result.txt")
        print("=" * 60)