
import sys
from server import KVServer
from tests import *
from benchmark import *

# --- Logger Class to save output to file ---
class Logger(object):
    def __init__(self, filename="result.txt"):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding='utf-8')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush() # Ensure it writes immediately

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
  ✓ Run 5 comprehensive tests
  ✓ Benchmark write throughput (with 0, 1K, 10K, 50K existing keys)
  ✓ Benchmark durability (random crashes with 100% recovery verification)
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
# In Python REPL or script:

from client import KVClient

# Connect to server (default port 8000)
client = KVClient(host='localhost', port=8000)

# Basic operations
client.Set('username', 'alice')
value = client.Get('username')  # Returns 'alice'
client.Delete('username')
client.Get('username')  # Returns None

# Bulk operations
items = [('key1', 'val1'), ('key2', 'val2'), ('key3', 'val3')]
client.BulkSet(items)

# Clean up
client.close()


🔧 OPTION 4: Quick Interactive Test
───────────────────────────────────────────────────────────────────────
# Terminal 1: Start server
python main.py --server

# Terminal 2: Run client
python3 << EOF
from client import KVClient
client = KVClient()
client.Set('test', 'hello world')
print(f"Value: {client.Get('test')}")
client.close()
EOF


📁 Files Created:
───────────────────────────────────────────────────────────────────────
kvstore_data/          # Default data directory
  ├── wal.log          # Write-ahead log (durability)
  └── snapshot.json    # Periodic snapshot (faster recovery)

result.txt             # Test results log
kvstore_test/          # Test data directory (auto-cleaned)
kvstore_bench/         # Benchmark data directory (auto-cleaned)
kvstore_durability/    # Durability test directory (auto-cleaned)


❓ Help:
───────────────────────────────────────────────────────────────────────
python main.py --help
    """)

if __name__ == "__main__":
    if len(sys.argv) > 1:
        if sys.argv[1] == '--help' or sys.argv[1] == '-h':
            print_usage()
            sys.exit(0)
        elif sys.argv[1] == '--server':
            # Server mode for subprocess
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
        # --- ENABLE LOGGING HERE ---
        # Redirect stdout to capture all print statements
        sys.stdout = Logger("result.txt")
        
        # Run tests and benchmarks
        print("="*60)
        print("Key-Value Store - Tests and Benchmarks")
        print("="*60)
        
        # 1. Functional Tests
        run_basic_tests()
        
        # 2. ACID & Cluster Tests
        test_concurrent_isolation()
        test_crash_atomicity()
        test_chaos_mode()
        test_cluster_failover()
        
        # 3. Performance Benchmarks
        benchmark_write_throughput()
        benchmark_durability()
        
        print("\n" + "="*60)
        print("All tests and benchmarks completed!")
        print("Results saved to result.txt")
        print("="*60)