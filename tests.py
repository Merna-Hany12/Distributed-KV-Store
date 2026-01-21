
from server import KVStore 
import shutil
import socket
import json
import threading
import os
import time
import subprocess
import sys


# ==========================================
# HELPER FUNCTIONS
# ==========================================
PEERS = [('localhost', 8000), ('localhost', 8001), ('localhost', 8002)]

def cleanup_dir(path):
    """Robust directory cleanup that retries on Windows PermissionErrors."""
    if not os.path.exists(path):
        return
        
    for i in range(10): # Try for up to 2 seconds
        try:
            shutil.rmtree(path)
            return # Success
        except PermissionError:
            time.sleep(0.2) # Wait for OS to release file lock
        except Exception:
            return # Ignore other errors

def send_req(port, msg):
    """Robust request sender with retry for connection."""
    for _ in range(3):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(2.0)
            s.connect(('localhost', port))
            s.sendall(json.dumps(msg).encode() + b'\n')
            data = s.recv(4096)
            if not data: return None
            resp = json.loads(data.decode())
            s.close()
            return resp
        except:
            time.sleep(0.5)
    return None

def find_leader():
    """Polls cluster nodes to find the current LEADER."""
    print("   (Scanning for leader...)", end=" ", flush=True)
    for i in range(10): # Retry loop
        for node_id, (host, port) in enumerate(PEERS):
            resp = send_req(port, {'type': 'get', 'key': '__ping__'})
            
            # If request accepted (status ok or value found), this is leader
            if resp and (resp.get('status') == 'ok' or 'value' in resp):
                print(f"Found Leader: Node {node_id}")
                return node_id, port
            
            # If redirected, we know who the leader is
            if resp and resp.get('status') == 'redirect':
                leader_id = resp.get('leader_id')
                if leader_id is not None:
                    print(f"Redirected to Leader: Node {leader_id}")
                    return leader_id, PEERS[leader_id][1]
        time.sleep(1.0)
    return None, None

# ==========================================
# PART 1: BASIC FUNCTIONAL TESTS
# ==========================================
def run_basic_tests():
    from server import KVServer
    from client import KVClient
    
    test_dir = "kvstore_test"
    test_port = 8010 # Use distinct port
    
    cleanup_dir(test_dir)
    
    server = KVServer(port=test_port, data_dir=test_dir)
    server_thread = threading.Thread(target=server.start)
    server_thread.daemon = True
    server_thread.start()
    time.sleep(1.0)
    
    print("\n" + "="*50)
    print("RUNNING BASIC FUNCTIONAL TESTS (Direct Server)")
    print("="*50)
    
    try:
        client = KVClient(port=test_port)
        
        print("\n1. Test: Set then Get")
        client.Set("key1", "value1")
        assert client.Get("key1") == "value1"
        print("   ✓ PASSED")
        
        print("\n2. Test: Set then Delete then Get")
        client.Set("key2", "value2")
        client.Delete("key2")
        assert client.Get("key2") is None
        print("   ✓ PASSED")

        print("\n3. Test: Persistence (Restart)")
        client.close()
        server.stop()
        time.sleep(1.0)
        
        # Restart
        server = KVServer(port=test_port, data_dir=test_dir)
        threading.Thread(target=server.start, daemon=True).start()
        time.sleep(1.0)
        
        client = KVClient(port=test_port)
        assert client.Get("key1") == "value1"
        print("   ✓ PASSED")
        
    except Exception as e:
        print(f"   FAILED: {e}")
    finally:
        try: client.close()
        except: pass
        server.stop()

# ==========================================
# PART 2: ACID TESTS (Using server.py)
# ==========================================
def test_concurrent_isolation():
    print("\n" + "="*50)
    print("ACID TEST 1: Concurrent Bulk Write Isolation")
    print("="*50)
    
    port = 8011
    cleanup_dir("kvstore_bench")
    
    cmd = [sys.executable, "-c", f"from server import KVServer; s=KVServer(port={port}, data_dir='kvstore_bench'); s.start()"]
    proc = subprocess.Popen(cmd)
    time.sleep(2)
    
    keys = [f"k{i}" for i in range(100)]
    
    def worker(val_prefix):
        items = [{'key': k, 'value': f"{val_prefix}_{k}"} for k in keys]
        send_req(port, {'command': 'bulk_set', 'items': items})

    t1 = threading.Thread(target=worker, args=("A",))
    t2 = threading.Thread(target=worker, args=("B",))
    
    print("   Starting concurrent conflicting transactions...")
    t1.start(); t2.start()
    t1.join(); t2.join()
    
    resp = send_req(port, {'command': 'get', 'key': 'k0'})
    
    if not resp or 'value' not in resp:
         print("   FAILURE: Could not read data.")
         proc.kill()
         return

    val0 = resp['value']
    if not val0:
        print("   FAILURE: Data missing.")
        proc.kill()
        return
        
    expected_prefix = val0.split('_')[0]
    
    consistent = True
    for k in keys:
        r = send_req(port, {'command': 'get', 'key': k})
        val = r['value'] if r else None
        if not val or not val.startswith(expected_prefix):
            consistent = False
            print(f"   FAILURE: Isolation violation! {k} is {val}, expected prefix {expected_prefix}")
            break
            
    if consistent:
        print(f"   ✓ SUCCESS: Transaction Isolation confirmed. All keys are '{expected_prefix}'.")
    else:
        print("   FAILURE: Data is mixed.")
        
    proc.kill()
    proc.wait() # Ensure process is dead

def test_crash_atomicity():
    print("\n" + "="*50)
    print("ACID TEST 2: Atomicity (SIGKILL Crash)")
    print("="*50)
    
    port = 8012
    cleanup_dir("kvstore_bench")
    
    cmd = [sys.executable, "-c", f"from server import KVServer; s=KVServer(port={port}, data_dir='kvstore_bench'); s.start()"]
    proc = subprocess.Popen(cmd)
    time.sleep(2)
    
    print("   Initiating massive bulk write...")
    huge_batch = [{'key': f"ak_{i}", 'value': "x"*100} for i in range(5000)]
    
    def killer():
        time.sleep(0.5) 
        print("   💀 KILLING SERVER with SIGKILL...")
        proc.kill() 
        
    t = threading.Thread(target=killer)
    t.start()
    
    try:
        send_req(port, {'command': 'bulk_set', 'items': huge_batch})
    except: pass
    
    t.join()
    proc.wait() # Wait for kill to finish
    
    print("   Restarting server to verify WAL Atomicity...")
    proc = subprocess.Popen(cmd)
    time.sleep(2)
    
    r1 = send_req(port, {'command': 'get', 'key': 'ak_0'})
    r2 = send_req(port, {'command': 'get', 'key': 'ak_4999'})
    k1 = r1.get('value') if r1 else None
    k_last = r2.get('value') if r2 else None
    
    if k1 is None and k_last is None:
        print("   ✓ SUCCESS: Atomicity Preserved (Rolled back).")
    elif k1 is not None and k_last is not None:
        print("   ✓ SUCCESS: Atomicity Preserved (Committed).")
    else:
        print(f"   FAILURE: Partial Write! k0={k1 is not None}, k4999={k_last is not None}")
        
    proc.kill()
    proc.wait()

def test_chaos_mode():
    print("\n" + "="*50)
    print("ACID TEST 3: Chaos Snapshot (Bonus)")
    print("="*50)
    
    cleanup_dir("kvstore_data/node_chaos")
    
    store = KVStore(instance_id="chaos")
    store.set("important_key", "my_data")
    
    print("   Attempting Snapshot with simulated disk failure...")
    result = store.save_snapshot(debug_chaos=True)
    print(f"   Snapshot outcome: {result}")
    
    new_store = KVStore(instance_id="chaos")
    val = new_store.get("important_key")
    
    if val == "my_data":
        print(f"   ✓ SUCCESS: Data recovered from WAL despite snapshot failure.")
    else:
        print("   FAILURE: Data lost!")

# ==========================================
# PART 3: CLUSTER TESTS (Using cluster.py)
# ==========================================
def test_cluster_failover():
    print("\n" + "="*50)
    print("CLUSTER TEST: Replication & Failover")
    print("="*50)
    
    cleanup_dir("kvstore_data")
    
    procs = []
    print("   Starting 3-node cluster (Ports 8000, 8001, 8002)...")
    for i in range(3):
        cmd = [sys.executable, "cluster.py", str(i)]
        procs.append(subprocess.Popen(cmd))
    
    try:
        lid, lport = find_leader()
        if lid is None:
            print("   FAILURE: Cluster failed to elect a leader.")
            return

        print(f"   Writing 'master_key' to Leader {lid}...")
        resp = send_req(lport, {'type': 'set', 'key': 'master_key', 'value': '123'})
        if not resp or resp.get('status') != 'ok':
            print(f"   FAILURE: Write rejected: {resp}")
            return

        print(f"   💀 KILLING Leader Node {lid}...")
        procs[lid].kill()
        
        print("   Waiting for Failover (Election)...")
        time.sleep(6) 
        
        new_lid, new_lport = find_leader()
        if new_lid is None or new_lid == lid:
            print("   FAILURE: Failover failed.")
        else:
            print(f"   New Leader is Node {new_lid}")
            resp = send_req(new_lport, {'type': 'get', 'key': 'master_key'})
            val = resp.get('value') if resp else None
            
            if val == '123':
                print("   ✓ SUCCESS: Data replicated and available on new Leader.")
            else:
                print(f"   FAILURE: Data lost! Got {val}")

    finally:
        print("   Cleaning up processes...")
        for p in procs:
            try: 
                p.kill()
                p.wait()
            except: pass
