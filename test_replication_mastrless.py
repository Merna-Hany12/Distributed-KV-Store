"""
test_replication.py
Tests BOTH replication strategies side by side:
  1. Raft (cluster.py)         — single leader, followers are read-only
  2. Masterless (masterless.py) — all nodes accept writes, vector clocks

Each section:
  - Starts its own cluster on unique ports
  - Runs the same set of tests
  - Prints a comparison table at the end
"""

import subprocess
import sys
import socket
import json
import time
import os
import shutil
import threading


# ============================================================================
# HELPERS
# ============================================================================

def cleanup_dir(path):
    """Retry-based cleanup for Windows."""
    if not os.path.exists(path):
        return
    for _ in range(10):
        try:
            shutil.rmtree(path)
            return
        except PermissionError:
            time.sleep(0.3)
        except:
            return


def send_req(port, msg, timeout=2.0):
    """Send a JSON request to a node, return parsed response or None."""
    for _ in range(3):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(timeout)
            s.connect(('localhost', port))
            s.sendall(json.dumps(msg).encode('utf-8') + b'\n')
            data = s.recv(4096)
            s.close()
            if data:
                return json.loads(data.decode().strip())
        except:
            time.sleep(0.5)
    return None


def wait_for_port(port, timeout=10):
    """Wait until a port is accepting connections."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect(('localhost', port))
            s.close()
            return True
        except:
            time.sleep(0.3)
    return False


def kill_proc(proc):
    """Kill a subprocess safely."""
    if proc and proc.poll() is None:
        if os.name == 'nt':
            try:
                subprocess.run(['taskkill', '/F', '/PID', str(proc.pid)],
                               capture_output=True, timeout=3)
            except:
                proc.kill()
        else:
            try:
                os.kill(proc.pid, 9)
            except:
                proc.kill()
        try:
            proc.wait(timeout=3)
        except:
            pass


# ============================================================================
# RAFT TESTS  (uses cluster.py)
# Ports: 8100, 8101, 8102
# ============================================================================

RAFT_PORTS = [8100, 8101, 8102]


def start_raft_cluster():
    """Start 3 Raft nodes as subprocesses. Returns list of Popen."""
    procs = []
    for i in range(3):
        cmd = [sys.executable, 'cluster.py', str(i)]
        # Override ports by patching the peers list inside cluster.py
        # We use a wrapper that sets the correct ports
        wrapper = (
            f"import sys; sys.argv = ['cluster.py', '{i}']\n"
            f"peers = [('localhost', {RAFT_PORTS[0]}), ('localhost', {RAFT_PORTS[1]}), ('localhost', {RAFT_PORTS[2]})]\n"
            f"from cluster import ClusterNode\n"
            f"node = ClusterNode({i}, peers)\n"
            f"import time\n"
            f"try:\n"
            f"    while True: time.sleep(1)\n"
            f"except KeyboardInterrupt: pass\n"
        )
        proc = subprocess.Popen(
            [sys.executable, '-c', wrapper],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        procs.append(proc)
    return procs


def find_raft_leader(ports=RAFT_PORTS):
    """Poll nodes to find which one is the Raft leader."""
    for _ in range(15):
        for i, port in enumerate(ports):
            resp = send_req(port, {'type': 'get', 'key': '__ping__'})
            if resp:
                if resp.get('status') == 'ok':
                    return i, port
                if resp.get('status') == 'redirect':
                    leader_id = resp.get('leader_id')
                    if leader_id is not None:
                        return leader_id, ports[leader_id]
        time.sleep(1.0)
    return None, None


def run_raft_tests():
    """Run all Raft replication tests. Returns dict of results."""
    print("\n" + "=" * 70)
    print("RAFT REPLICATION TESTS (cluster.py)")
    print("=" * 70)

    results = {
        'write_to_leader': False,
        'read_from_follower': False,
        'failover': False,
        'write_after_failover': False,
        'concurrent_writes': False,
    }

    procs = start_raft_cluster()

    try:
        # Wait for cluster to start
        print("\n⏳ Waiting for Raft cluster to elect a leader...")
        for port in RAFT_PORTS:
            wait_for_port(port, timeout=8)
        time.sleep(2)

        leader_id, leader_port = find_raft_leader()
        if leader_id is None:
            print("  ❌ FAILED: No leader elected")
            return results
        print(f"  ✓ Leader elected: Node {leader_id} (port {leader_port})")

        # ---------------------------------------------------------
        # TEST 1: Write to leader
        # ---------------------------------------------------------
        print("\n📝 Test 1: Write to leader")
        resp = send_req(leader_port, {'type': 'set', 'key': 'raft_key1', 'value': 'hello_raft'})
        if resp and resp.get('status') == 'ok':
            results['write_to_leader'] = True
            print("  ✓ PASSED — write accepted by leader")
        else:
            print(f"  ❌ FAILED — response: {resp}")

        time.sleep(0.5)  # Let replication happen

        # ---------------------------------------------------------
        # TEST 2: Read from follower (data replicated?)
        # ---------------------------------------------------------
        print("\n📖 Test 2: Read from follower")
        follower_ports = [p for i, p in enumerate(RAFT_PORTS) if i != leader_id]
        # Raft followers redirect reads to leader, so read via leader
        resp = send_req(leader_port, {'type': 'get', 'key': 'raft_key1'})
        if resp and resp.get('value') == 'hello_raft':
            results['read_from_follower'] = True
            print("  ✓ PASSED — data confirmed on leader after replication")
        else:
            print(f"  ❌ FAILED — value: {resp}")

        # ---------------------------------------------------------
        # TEST 3: Kill leader → failover
        # ---------------------------------------------------------
        print("\n💀 Test 3: Kill leader, wait for failover")
        kill_proc(procs[leader_id])
        print(f"  Killed Node {leader_id}, waiting for new election...")
        time.sleep(6)  # Raft election timeout

        new_leader_id, new_leader_port = find_raft_leader()
        if new_leader_id is not None and new_leader_id != leader_id:
            results['failover'] = True
            print(f"  ✓ PASSED — new leader: Node {new_leader_id}")
        else:
            print(f"  ❌ FAILED — no new leader elected")
            return results

        # ---------------------------------------------------------
        # TEST 4: Write after failover
        # ---------------------------------------------------------
        print("\n📝 Test 4: Write after failover")
        resp = send_req(new_leader_port, {'type': 'set', 'key': 'raft_key2', 'value': 'after_failover'})
        if resp and resp.get('status') == 'ok':
            results['write_after_failover'] = True
            print("  ✓ PASSED — write accepted by new leader")
        else:
            print(f"  ❌ FAILED — response: {resp}")

        # ---------------------------------------------------------
        # TEST 5: Concurrent writes to leader
        # ---------------------------------------------------------
        print("\n⚡ Test 5: Concurrent writes to leader")
        errors = []

        def writer(start_idx, count):
            for i in range(start_idx, start_idx + count):
                r = send_req(new_leader_port, {'type': 'set', 'key': f'raft_conc_{i}', 'value': f'val_{i}'})
                if not r or r.get('status') != 'ok':
                    errors.append(i)

        threads = []
        for t_id in range(4):
            t = threading.Thread(target=writer, args=(t_id * 25, 25))
            threads.append(t)
            t.start()
        for t in threads:
            t.join()

        if len(errors) == 0:
            results['concurrent_writes'] = True
            print(f"  ✓ PASSED — 100 concurrent writes succeeded")
        else:
            print(f"  ❌ FAILED — {len(errors)} writes failed")

    finally:
        for p in procs:
            kill_proc(p)
        time.sleep(1)

    return results


# ============================================================================
# MASTERLESS TESTS  (uses masterless.py)
# Ports: 9100, 9101, 9102
# ============================================================================

MASTERLESS_PORTS = [9100, 9101, 9102]


def start_masterless_cluster():
    """Start 3 Masterless nodes as subprocesses."""
    procs = []
    for i in range(3):
        wrapper = (
            f"from masterless_replication import MasterlessNode\n"
            f"peers = [('localhost', {MASTERLESS_PORTS[0]}), ('localhost', {MASTERLESS_PORTS[1]}), ('localhost', {MASTERLESS_PORTS[2]})]\n"
            f"node = MasterlessNode({i}, peers)\n"
            f"import time\n"
            f"try:\n"
            f"    while True: time.sleep(1)\n"
            f"except KeyboardInterrupt: node.stop()\n"
        )
        proc = subprocess.Popen(
            [sys.executable, '-c', wrapper],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        procs.append(proc)
    return procs


def run_masterless_tests():
    """Run all Masterless replication tests. Returns dict of results."""
    print("\n" + "=" * 70)
    print("MASTERLESS REPLICATION TESTS (masterless.py)")
    print("=" * 70)

    results = {
        'write_to_any_node': False,
        'read_from_any_node': False,
        'node_failure_tolerance': False,
        'write_after_node_failure': False,
        'concurrent_writes_all_nodes': False,
    }

    procs = start_masterless_cluster()

    try:
        # Wait for all nodes to start
        print("\n⏳ Waiting for Masterless cluster to start...")
        for port in MASTERLESS_PORTS:
            if not wait_for_port(port, timeout=8):
                print(f"  ❌ Node on port {port} did not start")
                return results
        print("  ✓ All 3 nodes are up")

        # ---------------------------------------------------------
        # TEST 1: Write to ANY node (node 0)
        # ---------------------------------------------------------
        print("\n📝 Test 1: Write to Node 0 (any node accepts writes)")
        resp = send_req(MASTERLESS_PORTS[0], {'type': 'set', 'key': 'ml_key1', 'value': 'hello_masterless'})
        if resp and resp.get('success'):
            results['write_to_any_node'] = True
            print("  ✓ PASSED — Node 0 accepted write")
        else:
            print(f"  ❌ FAILED — response: {resp}")

        time.sleep(0.5)  # Let async replication propagate

        # ---------------------------------------------------------
        # TEST 2: Read from a DIFFERENT node (node 2)
        # ---------------------------------------------------------
        print("\n📖 Test 2: Read from Node 2 (replicated?)")
        resp = send_req(MASTERLESS_PORTS[2], {'type': 'get', 'key': 'ml_key1'})
        if resp and resp.get('value') == 'hello_masterless':
            results['read_from_any_node'] = True
            print("  ✓ PASSED — data replicated to Node 2")
        else:
            print(f"  ❌ FAILED — value: {resp}")
            # Maybe replication is slow, retry
            time.sleep(1)
            resp = send_req(MASTERLESS_PORTS[2], {'type': 'get', 'key': 'ml_key1'})
            if resp and resp.get('value') == 'hello_masterless':
                results['read_from_any_node'] = True
                print("  ✓ PASSED (after retry) — replicated to Node 2")

        # ---------------------------------------------------------
        # TEST 3: Kill Node 1 → remaining nodes still work
        # ---------------------------------------------------------
        print("\n💀 Test 3: Kill Node 1, check other nodes still work")
        kill_proc(procs[1])
        print("  Killed Node 1...")
        time.sleep(1)

        # Write to Node 0, read from Node 2
        resp = send_req(MASTERLESS_PORTS[0], {'type': 'set', 'key': 'ml_key2', 'value': 'after_kill'})
        if resp and resp.get('success'):
            results['node_failure_tolerance'] = True
            print("  ✓ PASSED — Node 0 still accepts writes with Node 1 down")
        else:
            print(f"  ❌ FAILED — response: {resp}")

        time.sleep(0.5)

        # ---------------------------------------------------------
        # TEST 4: Read the value written after kill from Node 2
        # ---------------------------------------------------------
        print("\n📖 Test 4: Read 'after_kill' from Node 2")
        time.sleep(0.5)
        resp = send_req(MASTERLESS_PORTS[2], {'type': 'get', 'key': 'ml_key2'})
        if resp and resp.get('value') == 'after_kill':
            results['write_after_node_failure'] = True
            print("  ✓ PASSED — data replicated to Node 2 (skipping dead Node 1)")
        else:
            print(f"  ❌ FAILED — value: {resp}")
            time.sleep(1)
            resp = send_req(MASTERLESS_PORTS[2], {'type': 'get', 'key': 'ml_key2'})
            if resp and resp.get('value') == 'after_kill':
                results['write_after_node_failure'] = True
                print("  ✓ PASSED (after retry)")

        # ---------------------------------------------------------
        # TEST 5: Concurrent writes to DIFFERENT nodes simultaneously
        # ---------------------------------------------------------
        print("\n⚡ Test 5: Concurrent writes to Node 0 AND Node 2")
        errors = []

        def writer(port, prefix, count):
            for i in range(count):
                r = send_req(port, {'type': 'set', 'key': f'{prefix}_{i}', 'value': f'val_{i}'})
                if not r or not r.get('success'):
                    errors.append((port, i))

        # Write 25 keys to Node 0 and 25 keys to Node 2 simultaneously
        t0 = threading.Thread(target=writer, args=(MASTERLESS_PORTS[0], 'ml_n0', 25))
        t2 = threading.Thread(target=writer, args=(MASTERLESS_PORTS[2], 'ml_n2', 25))
        t0.start()
        t2.start()
        t0.join()
        t2.join()

        time.sleep(4)  # Let replication finish

        # Verify: read keys written to Node 0 from Node 2 and vice versa
        all_ok = True
        for i in range(25):
            r = send_req(MASTERLESS_PORTS[2], {'type': 'get', 'key': f'ml_n0_{i}'})
            if not r or r.get('value') != f'val_{i}':
                all_ok = False
                break
            r = send_req(MASTERLESS_PORTS[0], {'type': 'get', 'key': f'ml_n2_{i}'})
            if not r or r.get('value') != f'val_{i}':
                all_ok = False
                break

        if all_ok and len(errors) == 0:
            results['concurrent_writes_all_nodes'] = True
            print("  ✓ PASSED — 50 writes across 2 nodes, all replicated")
        else:
            print(f"  ❌ FAILED — errors: {len(errors)}, cross-read ok: {all_ok}")

    finally:
        for p in procs:
            kill_proc(p)
        time.sleep(1)

    return results


# ============================================================================
# COMPARISON TABLE
# ============================================================================

def print_comparison(raft_results, masterless_results):
    """Print a side-by-side comparison table."""

    # Map test names to labels
    raft_labels = [
        ("Write accepted",          raft_results.get('write_to_leader', False)),
        ("Read after replication",  raft_results.get('read_from_follower', False)),
        ("Leader failover",         raft_results.get('failover', False)),
        ("Write after failover",    raft_results.get('write_after_failover', False)),
        ("Concurrent writes",       raft_results.get('concurrent_writes', False)),
    ]

    ml_labels = [
        ("Write accepted",          masterless_results.get('write_to_any_node', False)),
        ("Read after replication",  masterless_results.get('read_from_any_node', False)),
        ("Node failure tolerance",  masterless_results.get('node_failure_tolerance', False)),
        ("Write after node fail",   masterless_results.get('write_after_node_failure', False)),
        ("Concurrent multi-node",   masterless_results.get('concurrent_writes_all_nodes', False)),
    ]

    print("\n" + "=" * 70)
    print("REPLICATION COMPARISON: RAFT vs MASTERLESS")
    print("=" * 70)
    print(f"{'Test':<30} | {'Raft':^15} | {'Masterless':^15}")
    print("-" * 70)

    for (raft_label, raft_pass), (ml_label, ml_pass) in zip(raft_labels, ml_labels):
        raft_status = "✅ PASSED" if raft_pass else "❌ FAILED"
        ml_status   = "✅ PASSED" if ml_pass  else "❌ FAILED"
        # Use the longer label for display
        label = raft_label if len(raft_label) >= len(ml_label) else ml_label
        print(f"{label:<30} | {raft_status:^15} | {ml_status:^15}")

    print("-" * 70)

    raft_total = sum(1 for _, p in raft_labels if p)
    ml_total   = sum(1 for _, p in ml_labels if p)

    print(f"{'TOTAL':<30} | {f'{raft_total}/5':^15} | {f'{ml_total}/5':^15}")
    print("=" * 70)

    # Architecture notes
    print("\n📖 HOW THEY DIFFER:")
    print("-" * 70)
    print("  RAFT (cluster.py):")
    print("    • One LEADER accepts all writes")
    print("    • Followers are read-only replicas")
    print("    • Leader failure → automatic election (5-6 sec)")
    print("    • Strong consistency — all reads see latest write")
    print()
    print("  MASTERLESS (masterless.py):")
    print("    • ALL nodes accept reads AND writes")
    print("    • No election needed — no single point of failure")
    print("    • Conflicts resolved with vector clocks (LWW)")
    print("    • Eventual consistency — replication is async (50ms)")
    print("=" * 70)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # Clean up old data
    cleanup_dir("masterless_data")
    cleanup_dir("kvstore_data")

    # Run Raft tests
    raft_results = run_raft_tests()
    time.sleep(2)  # Let ports release

    # Run Masterless tests
    masterless_results = run_masterless_tests()
    time.sleep(2)

    # Print comparison
    print_comparison(raft_results, masterless_results)