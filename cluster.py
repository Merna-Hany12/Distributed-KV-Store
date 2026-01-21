import socket
import threading
import time
import json
import random
import sys
from server import KVStore
from typing import  List, Tuple

# States
FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

class ClusterNode:
    def __init__(self, node_id: int, peers: List[Tuple[str, int]]):
        self.node_id = node_id
        self.peers = peers # List of (host, port) for all nodes
        self.host, self.port = peers[node_id]
        
        self.store = KVStore(instance_id=str(node_id))
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        
        # Election timers
        self.last_heartbeat = time.time()
        self.election_timeout = random.uniform(1.5, 3.0) # Randomized to prevent split vote
        
        self.running = True
        self.lock = threading.Lock()
        
        # Start Threads
        threading.Thread(target=self._server_loop, daemon=True).start()
        threading.Thread(target=self._election_loop, daemon=True).start()
        print(f"[Node {node_id}] Started on port {self.port}")

    def _server_loop(self):
        """Handle incoming TCP requests (Client or Peer)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen()
        
        while self.running:
            try:
                client, _ = sock.accept()
                threading.Thread(target=self._handle_request, args=(client,), daemon=True).start()
            except:
                break

    def _handle_request(self, sock):
        """Process JSON messages."""
        try:
            data = b""
            while True:
                chunk = sock.recv(4096)
                if not chunk: break
                data += chunk
                if b'\n' in data: break
            
            msg = json.loads(data.decode())
            resp = self._dispatch(msg)
            sock.sendall(json.dumps(resp).encode() + b'\n')
        except Exception as e:
            sock.sendall(json.dumps({'error': str(e)}).encode() + b'\n')
        finally:
            sock.close()

    def _dispatch(self, msg):
        """Route message based on type."""
        mtype = msg.get('type')
        
        # --- RAFT INTERNAL MESSAGES ---
        if mtype == 'request_vote':
            return self._handle_vote(msg)
        elif mtype == 'append_entries':
            return self._handle_heartbeat(msg)
        elif mtype == 'replicate':
            return self._handle_replication(msg)
            
        # --- CLIENT MESSAGES ---
        # "Writes and reads happen only to primary"
        if self.state != LEADER:
            return {'status': 'redirect', 'leader_id': self.leader_id}
            
        if mtype == 'get':
            return {'status': 'ok', 'value': self.store.get(msg['key'])}
        elif mtype == 'set':
            # Write to Local + Replicate to Quorum
            success = self._replicate_to_followers({'type': 'set', 'key': msg['key'], 'value': msg['value']})
            if success:
                return {'status': 'ok'}
            return {'status': 'error', 'message': 'replication failed'}
        elif mtype == 'bulk_set':
            success = self._replicate_to_followers({'type': 'bulk_set', 'items': msg['items']})
            return {'status': 'ok' if success else 'error'}
            
        return {'error': 'unknown command'}

    # --- REPLICATION LOGIC ---
    def _replicate_to_followers(self, entry):
        """Leader writes to self, then sends to followers."""
        # 1. Apply Locally
        if entry['type'] == 'set':
            self.store.set(entry['key'], entry['value'])
        elif entry['type'] == 'bulk_set':
            self.store.bulk_set(entry['items'])
            
        # 2. Send to Peers
        ack_count = 1 # Self is 1
        for i, peer in enumerate(self.peers):
            if i == self.node_id: continue
            
            try:
                # Simple implementation: Send entry directly
                # In real Raft, we send Log Index + Term
                resp = self._send_rpc(i, {'type': 'replicate', 'entry': entry, 'term': self.current_term})
                if resp and resp.get('success'):
                    ack_count += 1
            except:
                pass # Peer might be down
        
        # 3. Check Quorum (Majority)
        return ack_count >= 2 # 2 out of 3 is majority

    def _handle_replication(self, msg):
        """Follower receives data from Leader."""
        if msg['term'] < self.current_term:
            return {'success': False}
        
        # Reset timeout
        self.last_heartbeat = time.time()
        self.state = FOLLOWER
        self.leader_id = msg.get('leader_id') # Usually passed in append_entries
        
        # Write to disk
        self.store.apply_replication_log(msg['entry'])
        return {'success': True}

    # --- ELECTION LOGIC ---
    def _election_loop(self):
        while self.running:
            time.sleep(0.1)
            if self.state == LEADER:
                self._send_heartbeats()
            elif time.time() - self.last_heartbeat > self.election_timeout:
                self._start_election()

    def _start_election(self):
        print(f"[Node {self.node_id}] Timeout! Starting election...")
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        votes = 1
        
        self.last_heartbeat = time.time() # Reset to avoid spam
        
        for i in range(len(self.peers)):
            if i == self.node_id: continue
            try:
                resp = self._send_rpc(i, {'type': 'request_vote', 'term': self.current_term, 'candidate_id': self.node_id})
                if resp and resp.get('vote_granted'):
                    votes += 1
            except: pass
            
        if votes >= 2:
            print(f"[Node {self.node_id}] Became LEADER for Term {self.current_term}")
            self.state = LEADER
            self.leader_id = self.node_id
            self._send_heartbeats()

    def _handle_vote(self, msg):
        if msg['term'] > self.current_term:
            self.current_term = msg['term']
            self.state = FOLLOWER
            self.voted_for = None
            
        if msg['term'] == self.current_term and (self.voted_for is None or self.voted_for == msg['candidate_id']):
            self.voted_for = msg['candidate_id']
            self.last_heartbeat = time.time() # Don't start own election if we voted
            return {'vote_granted': True}
        return {'vote_granted': False}

    def _send_heartbeats(self):
        for i in range(len(self.peers)):
            if i == self.node_id: continue
            try:
                self._send_rpc(i, {'type': 'append_entries', 'term': self.current_term, 'leader_id': self.node_id})
            except: pass
            
    def _handle_heartbeat(self, msg):
        if msg['term'] >= self.current_term:
            self.current_term = msg['term']
            self.state = FOLLOWER
            self.leader_id = msg['leader_id']
            self.last_heartbeat = time.time()
            return {'success': True}
        return {'success': False}

    def _send_rpc(self, target_id, msg):
        host, port = self.peers[target_id]
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.2)
        try:
            s.connect((host, port))
            s.sendall(json.dumps(msg).encode() + b'\n')
            data = s.recv(1024)
            return json.loads(data.decode())
        finally:
            s.close()

# Helper to run specific node
if __name__ == "__main__":
    # peers config
    peers = [('localhost', 8000), ('localhost', 8001), ('localhost', 8002)]
    
    if len(sys.argv) < 2:
        print("Usage: python cluster.py <node_id 0-2>")
        sys.exit(1)
        
    node_id = int(sys.argv[1])
    node = ClusterNode(node_id, peers)
    
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")