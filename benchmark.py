import time
import subprocess
import os
import threading
import sys
from server import KVServer
from client import KVClient
import random
def safe_rmtree(path, retries=30, delay=0.2):
    import shutil, time, os
    if not os.path.exists(path):
        return
    for i in range(retries):
        try:
            shutil.rmtree(path)
            return
        except PermissionError:
            time.sleep(delay)
        except Exception:
            break
    # Final attempt with error suppression or manual skip
    try:
        shutil.rmtree(path, ignore_errors=True)
    except:
        pass

def benchmark_write_throughput():
    """Benchmark write throughput with varying data sizes."""
    import shutil
    
    print("\n" + "="*70)
    print("BENCHMARK: Write Throughput")
    print("="*70)
    
    bench_dir = "kvstore_bench"
    bench_port = 8002
    
    results = []
    
    for data_size in [0, 1000, 10000, 50000]:
        if os.path.exists(bench_dir):
            shutil.rmtree(bench_dir)
        
        # Start server
        server = KVServer(port=bench_port, data_dir=bench_dir)
        server_thread = threading.Thread(target=server.start)
        server_thread.daemon = True
        server_thread.start()
        time.sleep(1.0)  # Give server more time to start
        
        try:
            client = KVClient(port=bench_port)
        except ConnectionRefusedError:
            print(f"ERROR: Could not connect to server on port {bench_port}")
            server.stop()
            continue
        
        # Pre-populate data using BulkSet (this is just setup, not measured)
        if data_size > 0:
            print(f"Pre-populating with {data_size:,} keys...", end=" ", flush=True)
            batch_size = 1000
            for i in range(0, data_size, batch_size):
                batch = [(f"key_{j}", f"value_{j}") for j in range(i, min(i + batch_size, data_size))]
                client.BulkSet(batch)
            print("Done!")
        
        # Benchmark INDIVIDUAL writes (realistic workload)
        num_writes = 10000
        print(f"Benchmarking {num_writes:,} INDIVIDUAL writes...", end=" ", flush=True)
        
        start_time = time.time()
        for i in range(num_writes):
            client.Set(f"bench_key_{i}", f"bench_value_{i}")
        end_time = time.time()
        
        elapsed = end_time - start_time
        throughput = num_writes / elapsed
        
        print("Done!")
        
        results.append({
            'data_size': data_size,
            'elapsed': elapsed,
            'throughput': throughput
        })
        
        client.close()
        server.stop()
        time.sleep(1.0)  # Give server time to clean up
        shutil.rmtree(bench_dir)

    # Print results table
    print("\n" + "="*70)
    print("WRITE THROUGHPUT RESULTS (Individual Set() Operations)")
    print("="*70)
    print(f"{'Existing Keys':>15} | {'Elapsed Time':>15} | {'Throughput':>20}")
    print("-" * 70)
    
    baseline_throughput = None
    for result in results:
        data_size = f"{result['data_size']:,}"
        elapsed = f"{result['elapsed']:.2f}s"
        throughput = f"{result['throughput']:.2f} writes/sec"
        print(f"{data_size:>15} | {elapsed:>15} | {throughput:>20}")
        
        if baseline_throughput is None:
            baseline_throughput = result['throughput']
    
    print("="*70)
    
    # Show performance analysis
    if len(results) > 0:
        avg_throughput = sum(r['throughput'] for r in results) / len(results)
        print(f"\n📈 PERFORMANCE ANALYSIS:")
        print(f"  • Average Throughput: {avg_throughput:,.2f} writes/sec")
        print(f"  • Best Performance: {max(r['throughput'] for r in results):,.2f} writes/sec")
        print(f"  • Worst Performance: {min(r['throughput'] for r in results):,.2f} writes/sec")
        
        if len(results) > 1:
            degradation = ((results[0]['throughput'] - results[-1]['throughput']) / results[0]['throughput'] * 100)
            print(f"  • Performance Degradation: {degradation:.1f}% (0 keys → 50k keys)")
        
        print(f"\n  💡 Note: Each Set() operation includes:")
        print(f"     - TCP round-trip to server")
        print(f"     - Write to WAL file")
        print(f"     - fsync() to physical disk")
        print(f"     - Response back to client")
        print(f"\n  ⚡ For batch operations, use BulkSet() for 10-50x better throughput")
    
    print("="*70)
    
    return results




def benchmark_durability():
    """Benchmark durability by randomly killing the server."""
    import shutil
    
    print("\n" + "="*70)
    print("BENCHMARK: Durability (Random Crash Testing)")
    print("="*70)
    
    bench_dir = "kvstore_durability"
    bench_port = 8003
    
    if os.path.exists(bench_dir):
        safe_rmtree(bench_dir)
    
    num_crashes = 5
    
    acknowledged = set()
    acknowledged_lock = threading.Lock()
    
    stop_flag = threading.Event()
    crash_count = 0
    crash_lock = threading.Lock()
    writer_started = threading.Event()
    
    # Use subprocess for true process killing
    current_process = [None]  # Changed from current_server to current_process
    process_lock = threading.Lock()


    def start_server_subprocess():
        """Start server as a separate process."""
        with process_lock:
            proc = subprocess.Popen(
                [sys.executable, '-c', 
                 f"from server import KVServer; server = KVServer(port={bench_port}, data_dir='{bench_dir}'); server.start()"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == 'nt' else 0
            )
            current_process[0] = proc
        return proc
    
    def writer_thread():
        """Write data continuously."""
        time.sleep(2.0)  # Wait for server to be ready
        client = None
        
        for retry in range(5):
            try:
                client = KVClient(port=bench_port)
                writer_started.set()
                print("✓ Writer thread connected successfully")
                break
            except ConnectionRefusedError:
                if retry < 4:
                    print(f"  Writer connection attempt {retry + 1}/5 failed, retrying...")
                    time.sleep(1.5)
                else:
                    print("  ✗ ERROR: Writer could not connect to server")
                    stop_flag.set()
                    return
        
        if client is None:
            stop_flag.set()
            return
            
        i = 0
        reconnect_attempts = 0
        max_reconnect = 3
        
        while not stop_flag.is_set():
            try:
                key = f"key_{i}"
                value = f"value_{i}"
                if client.Set(key, value):
                    with acknowledged_lock:
                        acknowledged.add(key)
                    reconnect_attempts = 0  # Reset on success
                i += 1
                time.sleep(0.003)  # Small delay
            except Exception as e:
                # Server might be restarting
                reconnect_attempts += 1
                
                if reconnect_attempts > max_reconnect:
                    try:
                        client.close()
                    except:
                        pass
                    
                    # Wait for server to restart
                    time.sleep(2.5)
                    
                    # Try to reconnect
                    for retry in range(5):
                        try:
                            client = KVClient(port=bench_port)
                            reconnect_attempts = 0
                            break
                        except:
                            time.sleep(1.0)
    
    def killer_thread():
        """Kill and restart server randomly - ACTUAL PROCESS KILL."""
        nonlocal crash_count
        
        # Wait for writer to start
        if not writer_started.wait(timeout=10):
            print("  ✗ ERROR: Writer thread never started")
            stop_flag.set()
            return
        
        for i in range(num_crashes):
            wait_time = random.uniform(1.5, 3.0)
            time.sleep(wait_time)
            
            with crash_lock:
                crash_count += 1
                current_crash = crash_count
            
            # Record state before crash
            with acknowledged_lock:
                pre_crash_count = len(acknowledged)
            
            print(f"  💀 Crash #{current_crash}/{num_crashes}: KILLING server process (had {pre_crash_count:,} writes)...")
            
            # FORCEFULLY KILL the server process (simulates crash)
            with process_lock:
                if current_process[0]:
                    proc = current_process[0]
                    
                    if os.name == 'nt':  # Windows
                        # Use taskkill for forceful termination
                        try:
                            subprocess.run(['taskkill', '/F', '/PID', str(proc.pid)], 
                                         capture_output=True, timeout=5)
                        except:
                            proc.kill()  # Fallback
                    else:  # Unix/Linux/Mac
                        # Send SIGKILL (cannot be caught or ignored)
                        try:
                            os.kill(proc.pid, 9)  # SIGKILL
                        except:
                            proc.kill()  # Fallback
                    
                    try:
                        proc.wait(timeout=2)
                    except:
                        pass
                    
                    current_process[0] = None
            
            time.sleep(2.0)  # Give OS time to release the port
            print(f"  🔄 Crash #{current_crash}/{num_crashes}: Restarting server...")
            
            # Start new server process
            start_server_subprocess()
            time.sleep(3.0)  # Give server time to start
    
    # Start initial server
    print("Starting durability test server...")
    start_server_subprocess()
    time.sleep(3.0)
    
    # Test connection before starting
    try:
        test_client = KVClient(port=bench_port)
        test_client.close()
        print("✓ Server connection verified\n")
    except Exception as e:
        print(f"✗ ERROR: Could not connect to durability test server: {e}")
        with process_lock:
            if current_process[0]:
                current_process[0].kill()
        return {'crashes': 0, 'acknowledged': 0, 'lost': 0, 'durability': 0}
    
    # Start threads
    writer = threading.Thread(target=writer_thread, daemon=True)
    killer = threading.Thread(target=killer_thread, daemon=True)
    
    print("The server will be FORCEFULLY KILLED and restarted 5 times randomly.\n")
    
    writer.start()
    time.sleep(1.0)  # Let writer establish connection first
    killer.start()
    
    killer.join()
    stop_flag.set()
    writer.join(timeout=5)
    
    # Kill current server
    with process_lock:
        if current_process[0]:
            try:
                current_process[0].kill()
                current_process[0].wait(timeout=2)
            except:
                pass
            current_process[0] = None
    
    time.sleep(2.0)
    
    print("\n  🔍 Final check: Restarting server to verify data...")
    
    # Start final server for verification
    start_server_subprocess()
    time.sleep(3.0)
    
    # Check durability
    client = None
    for retry in range(5):
        try:
            client = KVClient(port=bench_port)
            break
        except ConnectionRefusedError:
            if retry < 4:
                print(f"  Reconnection attempt {retry + 1}/5...")
                time.sleep(2.0)
    
    if client is None:
        print("  ✗ ERROR: Could not connect to server for final check")
        with process_lock:
            if current_process[0]:
                current_process[0].kill()
        return {'crashes': crash_count, 'acknowledged': 0, 'lost': 0, 'durability': 0}
    
    with acknowledged_lock:
        acked_keys = list(acknowledged)
    
    print(f"  Verifying {len(acked_keys):,} acknowledged writes...")
    
    # Detailed verification
    lost_keys = []
    for key in acked_keys:
        value = client.Get(key)
        if value is None:
            lost_keys.append(key)
    
    client.close()
    
    # Kill final server
    with process_lock:
        if current_process[0]:
            try:
                current_process[0].kill()
                current_process[0].wait(timeout=2)
            except:
                pass
    
    time.sleep(1.0)
    
    durability = (len(acked_keys) - len(lost_keys)) / len(acked_keys) * 100 if acked_keys else 100
    
    # Print results table
    print("\n" + "="*70)
    print("DURABILITY TEST RESULTS")
    print("="*70)
    print(f"{'Metric':<40} | {'Value':>25}")
    print("-" * 70)
    print(f"{'Number of server crashes':<40} | {crash_count:>25,}")
    print(f"{'Total acknowledged writes':<40} | {len(acked_keys):>25,}")
    print(f"{'Lost keys after recovery':<40} | {len(lost_keys):>25,}")
    print(f"{'Recovered keys':<40} | {len(acked_keys) - len(lost_keys):>25,}")
    print(f"{'Data durability':<40} | {durability:>24.2f}%")
    print("="*70)
    
    if durability == 100 and len(acked_keys) > 0:
        print("✓ SUCCESS: 100% durability achieved!")
        print("  All acknowledged writes survived FORCEFUL KILLS.")
    elif len(acked_keys) == 0:
        print("⚠ WARNING: No writes were acknowledged (connection issues)")
    else:
        print(f"✗ FAILURE: {len(lost_keys):,} keys lost ({100-durability:.2f}% data loss)")
        if len(lost_keys) <= 10:
            print(f"  Lost keys: {', '.join(lost_keys[:10])}")
        print("  This indicates the WAL is not being fsynced properly!")
    
    print("="*70)
    
    return {
        'crashes': crash_count,
        'acknowledged': len(acked_keys),
        'lost': len(lost_keys),
        'durability': durability
    }

