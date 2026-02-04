"""
masterless.py (FIXED VERSION)
Masterless (Multi-Master) Replication using Vector Clocks.

FIX: Changed data_dir initialization to avoid double-nesting directories
"""

import socket
import json
import threading
import time
import sys
from typing import List, Tuple, Dict, Optional, Any
from server import KVStore
from indexing_module import IndexManager


class MasterlessNode:
    """
    A single node in a masterless cluster.
    - Accepts reads and writes directly (no leader needed)
    - Pushes all local writes to every peer asynchronously
    - Uses vector clocks to detect and resolve conflicts
    """

    def __init__(self, node_id: int, peers: List[Tuple[str, int]]):
        self.node_id = node_id
        self.peers = peers                          # All nodes including self
        self.host, self.port = peers[node_id]

        # FIX: Pass the complete path, use empty instance_id to avoid double-nesting
        self.store = KVStore(data_dir=f"masterless_data/node_{node_id}", instance_id="")

        # Vector clock: {node_id_str -> version}
        # Tracks how many writes each node has done
        self.vector_clock: Dict[str, int] = {str(i): 0 for i in range(len(peers))}
        self.clock_lock = threading.Lock()

        self.applied_versions: Dict[str, int] = {str(i): 0 for i in range(len(peers))}

        # Replication queue — local writes waiting to be sent to peers
        self.replication_queue: List[Dict] = []
        self.queue_lock = threading.Lock()

        # Conflict log — stores any conflicts that were detected and resolved
        self.conflict_log: List[Dict] = []

        self.running = True

        # Start TCP server thread
        threading.Thread(target=self._server_loop, daemon=True).start()

        # Start background replication pusher
        threading.Thread(target=self._replication_loop, daemon=True).start()

        print(f"[masterless node_{self.node_id}] Started on {self.host}:{self.port}")
        self._sync_with_peers()

    # =====================================================================
    # VECTOR CLOCK OPERATIONS
    # =====================================================================
    def _increment_clock(self):
        """Increment own component of vector clock (called on local write)."""
        with self.clock_lock:
            self.vector_clock[str(self.node_id)] += 1
    def _sync_with_peers(self):
        """On startup, pull all latest writes from peers to catch up."""
        for peer_idx, (host, port) in enumerate(self.peers):
            if peer_idx == self.node_id:
                continue
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                s.connect((host, port))
                s.sendall(json.dumps({'type': 'get_all_entries'}).encode('utf-8') + b'\n')
                buffer = b''
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    buffer += chunk
                s.close()
                if buffer:
                    data = json.loads(buffer.decode())
                    for entry, clock, source in data.get('entries', []):
                        self._handle_replicate({
                            'entry': entry,
                            'vector_clock': clock,
                            'source_node': source
                        })
            except:
                pass

    def _merge_clock(self, other_clock: Dict[str, int]):
        """Merge incoming clock — take max of each component."""
        with self.clock_lock:
            for nid, ver in other_clock.items():
                current = self.vector_clock.get(nid, 0)
                self.vector_clock[nid] = max(current, ver)

    def _get_clock(self) -> Dict[str, int]:
        with self.clock_lock:
            return dict(self.vector_clock)

    def _is_concurrent(self, clock_a: Dict[str, int], clock_b: Dict[str, int]) -> bool:
        """
        Two events are CONCURRENT if neither happened-before the other.
        This means: a is not <= b AND b is not <= a.
        Concurrent writes = conflict.
        """
        a_leq_b = all(clock_a.get(k, 0) <= clock_b.get(k, 0) for k in set(clock_a) | set(clock_b))
        b_leq_a = all(clock_b.get(k, 0) <= clock_a.get(k, 0) for k in set(clock_a) | set(clock_b))
        return not a_leq_b and not b_leq_a

    # =====================================================================
    # TCP SERVER — listens for client requests AND peer replication
    # =====================================================================
    def _server_loop(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.settimeout(1.0)
        sock.bind((self.host, self.port))
        sock.listen(5)

        while self.running:
            try:
                client_sock, _ = sock.accept()
                threading.Thread(target=self._handle_connection, args=(client_sock,), daemon=True).start()
            except socket.timeout:
                continue
            except:
                break
        sock.close()

    def _handle_connection(self, sock: socket.socket):
        """Read one JSON message, dispatch, send response."""
        buffer = b""
        try:
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                buffer += chunk
                if b'\n' in buffer:
                    break

            if not buffer.strip():
                return

            msg = json.loads(buffer.decode().strip())
            response = self._dispatch(msg)
            sock.sendall(json.dumps(response).encode('utf-8') + b'\n')
        except Exception as e:
            try:
                sock.sendall(json.dumps({'status': 'error', 'message': str(e)}).encode('utf-8') + b'\n')
            except:
                pass
        finally:
            sock.close()

    def _dispatch(self, msg: Dict) -> Dict:
        """Route message to correct handler."""
        msg_type = msg.get('type') or msg.get('command')

        # --- Peer replication message ---
        if msg_type == 'replicate':
            return self._handle_replicate(msg)

        # --- Client messages ---
        if msg_type == 'set':
            return self._handle_set(msg)
        elif msg_type == 'get':
            return self._handle_get(msg)
        elif msg_type == 'delete':
            return self._handle_delete(msg)
        elif msg_type == 'bulk_set':
            return self._handle_bulk_set(msg)
        elif msg_type == 'full_text_search':
            results = self.store.full_text_search(msg.get('query', ''), msg.get('top_k', 10))
            return {'status': 'ok', 'results': results}
        elif msg_type == 'semantic_search':
            results = self.store.semantic_search(msg.get('query', ''), msg.get('top_k', 10))
            return {'status': 'ok', 'results': results}
        elif msg_type == 'phrase_search':
            results = self.store.phrase_search(msg.get('phrase', ''))
            return {'status': 'ok', 'results': results}
        elif msg_type == 'get_clock':
            return {'status': 'ok', 'clock': self._get_clock()}
        elif msg_type == 'get_conflicts':
            return {'status': 'ok', 'conflicts': self.conflict_log}

        return {'status': 'error', 'message': f'Unknown command: {msg_type}'}

    # =====================================================================
    # CLIENT WRITE HANDLERS — accept write, queue for replication
    # =====================================================================
    def _handle_set(self, msg: Dict) -> Dict:
        key = msg.get('key') or msg.get('query')
        value = msg.get('value')

        # 1. Increment own clock
        self._increment_clock()

        # 2. Apply locally
        self.store.set(key, value)

        # 3. Queue for replication to peers
        with self.queue_lock:
            self.replication_queue.append({
                'type': 'replicate',
                'entry': {'type': 'set', 'key': key, 'value': value},
                'vector_clock': self._get_clock(),
                'source_node': self.node_id
            })

        return {'status': 'ok', 'success': True}

    def _handle_get(self, msg: Dict) -> Dict:
        key = msg.get('key')
        value = self.store.get(key)
        return {'status': 'ok', 'value': value}

    def _handle_delete(self, msg: Dict) -> Dict:
        key = msg.get('key')
        self._increment_clock()
        success = self.store.delete(key)

        with self.queue_lock:
            self.replication_queue.append({
                'type': 'replicate',
                'entry': {'type': 'delete', 'key': key},
                'vector_clock': self._get_clock(),
                'source_node': self.node_id
            })

        return {'status': 'ok', 'success': success}

    def _handle_bulk_set(self, msg: Dict) -> Dict:
        items = msg.get('items', [])
        # Normalize: items might be [{'key':..,'value':..}] or [[k,v]]
        normalized = []
        for item in items:
            if isinstance(item, dict):
                normalized.append((item['key'], item['value']))
            else:
                normalized.append((item[0], item[1]))

        self._increment_clock()
        self.store.bulk_set(normalized)

        with self.queue_lock:
            self.replication_queue.append({
                'type': 'replicate',
                'entry': {'type': 'bulk_set', 'items': normalized},
                'vector_clock': self._get_clock(),
                'source_node': self.node_id
            })

        return {'status': 'ok', 'success': True}

    # =====================================================================
    # INCOMING REPLICATION FROM PEER
    # =====================================================================
    def _handle_replicate(self, msg: Dict) -> Dict:
        incoming_clock = msg.get('vector_clock', {})
        source_node = msg.get('source_node', -1)
        entry = msg.get('entry', {})

        # --- Skip if this version is already applied ---
        last_applied = self.applied_versions.get(str(source_node), 0)
        incoming_version = incoming_clock.get(str(source_node), 0)
        if incoming_version <= last_applied:
            return {'success': True}  # already applied

        # --- Detect concurrency BEFORE merging clocks ---
        my_clock = self._get_clock()
        concurrent = self._is_concurrent(my_clock, incoming_clock)

        # --- Resolve conflicts ---
        if concurrent:
            self.conflict_log.append({
                'time': time.time(),
                'source': source_node,
                'entry': entry,
                'my_clock': my_clock,
                'their_clock': incoming_clock,
                'resolution': 'Merged concurrent write'
            })
            # Last-Writer-Wins: higher node_id wins
            if source_node >= self.node_id:
                self.store.apply_replication_log(entry)
        else:
            # Not concurrent → safe to apply
            self.store.apply_replication_log(entry)

        # --- Merge clocks after applying ---
        self._merge_clock(incoming_clock)

        # --- Update applied_versions ---
        self.applied_versions[str(source_node)] = incoming_version

        return {'success': True}




    # =====================================================================
    # BACKGROUND REPLICATION PUSHER
    # =====================================================================
    def _replication_loop(self):
        """Every 10ms (was 50ms), grab pending ops and push to all peers."""
        while self.running:
            time.sleep(0.01) # Faster replication

            # Grab and clear the queue
            with self.queue_lock:
                if not self.replication_queue:
                    continue
                ops = self.replication_queue[:]
                self.replication_queue = []

            # Send each op to every peer (skip self)
            for peer_idx, (host, port) in enumerate(self.peers):
                if peer_idx == self.node_id:
                    continue
                for op in ops:
                    self._send_to_peer(host, port, op)

    def _send_to_peer(self, host: str, port: int, message: Dict):
        """Send a single message to a peer. Fire-and-forget."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect((host, port))
            s.sendall(json.dumps(message).encode('utf-8') + b'\n')
            s.recv(1024)  # Read ack
            s.close()
        except:
            pass  # Peer might be down — that's fine, eventual consistency

    # =====================================================================
    # STOP
    # =====================================================================
    def stop(self):
        self.running = False
        self.store.save_indexes()
        self.store.close()


# ============================================================================
# ENTRY POINT — run as: python masterless.py <node_id>
# ============================================================================
if __name__ == "__main__":
    peers = [('localhost', 9100), ('localhost', 9101), ('localhost', 9102)]

    if len(sys.argv) < 2:
        print("Usage: python masterless_replication <node_id 0-2>")
        sys.exit(1)

    node_id = int(sys.argv[1])
    node = MasterlessNode(node_id, peers)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[masterless node_{node_id}] Shutting down...")
        node.stop()