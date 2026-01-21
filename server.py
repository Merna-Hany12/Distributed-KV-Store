import socket
import json
import threading
import os
import time
import random
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any


class KVStore:
    def __init__(self, data_dir: str = "kvstore_data", instance_id: str = "0"):
        self.data_dir = Path(data_dir) / f"node_{instance_id}"
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self.log_file = self.data_dir / "wal.log"
        self.snapshot_file = self.data_dir / "snapshot.json"
        
        self.data: Dict[str, str] = {}
        self.lock = threading.Lock()
        
        # Load data
        self._load_from_disk()
        
        # Open WAL for appending
        self.log_fd = open(self.log_file, 'ab', buffering=0)

    def _load_from_disk(self):
        """Recover state from Snapshot + WAL."""
        if self.snapshot_file.exists():
            try:
                with open(self.snapshot_file, 'r') as f:
                    self.data = json.load(f)
            except (json.JSONDecodeError, IOError):
                print(f"Warning: Corrupt snapshot found, ignoring.")

        if self.log_file.exists():
            with open(self.log_file, 'r') as f:
                for line in f:
                    if not line.strip(): continue
                    try:
                        entry = json.loads(line)
                        self._apply_entry(entry)
                    except json.JSONDecodeError:
                        print("Found incomplete WAL entry (crash recovery), discarding.")
                        continue

    def _apply_entry(self, entry: Dict):
        """Apply a log entry to memory."""
        op_type = entry['type']
        if op_type == 'set':
            self.data[entry['key']] = entry['value']
        elif op_type == 'delete':
            self.data.pop(entry['key'], None)
        elif op_type == 'bulk_set':
            for k, v in entry['items']:
                self.data[k] = v

    def _write_wal(self, entry: Dict):
        """Write to WAL with fsync for durability."""
        line = json.dumps(entry) + '\n'
        self.log_fd.write(line.encode('utf-8'))
        os.fsync(self.log_fd.fileno())

    def apply_replication_log(self, entry: Dict):
        """Used by Follower nodes to apply logs from Leader."""
        with self.lock:
            self._write_wal(entry)
            self._apply_entry(entry)

    def set(self, key: str, value: str) -> bool:
        entry = {'type': 'set', 'key': key, 'value': value}
        with self.lock:
            self._write_wal(entry)
            self._apply_entry(entry)
        return True

    def get(self, key: str) -> Optional[str]:
        with self.lock:
            return self.data.get(key)

    # --- MISSING METHODS ADDED BELOW ---

    def delete(self, key: str) -> bool:
        """Delete a key with WAL durability."""
        entry = {'type': 'delete', 'key': key}
        with self.lock:
            if key not in self.data:
                return False
            self._write_wal(entry)
            self._apply_entry(entry)
        return True

    def bulk_set(self, items: List[Tuple[str, str]]) -> bool:
        entry = {'type': 'bulk_set', 'items': items}
        with self.lock:
            self._write_wal(entry)
            self._apply_entry(entry)
        return True

    def save_snapshot(self, debug_chaos: bool = False) -> bool:
        """Compacts WAL into a snapshot."""
        with self.lock:
            if debug_chaos and random.random() < 0.50: 
                return False
            
            temp_file = self.snapshot_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(self.data, f)
            
            os.replace(temp_file, self.snapshot_file)
            return True

    def close(self):
        """Gracefully close the file handle."""
        if self.log_fd:
            try:
                os.fsync(self.log_fd.fileno())
                self.log_fd.close()
            except ValueError:
                pass # Already closed
class KVServer:
    """TCP server for the key-value store."""
    
    def __init__(self, host: str = 'localhost', port: int = 8000, data_dir: str = "kvstore_data"):
        self.host = host
        self.port = port
        self.store = KVStore(data_dir)
        self.running = False
        self.server_socket = None
        
    def start(self):
        """Start the TCP server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True
        
        print(f"KVStore server listening on {self.host}:{self.port}")
        
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                try:
                    client_socket, addr = self.server_socket.accept()
                    thread = threading.Thread(target=self._handle_client, args=(client_socket,))
                    thread.daemon = True
                    thread.start()
                except socket.timeout:
                    continue
            except Exception as e:
                if self.running:
                    print(f"Server error: {e}")
                break
    
    def _handle_client(self, client_socket: socket.socket):
        """Handle a client connection."""
        buffer = b""
        try:
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                
                buffer += chunk
                
                # Process complete messages (newline-delimited JSON)
                while b'\n' in buffer:
                    line, buffer = buffer.split(b'\n', 1)
                    if line:
                        try:
                            request = json.loads(line.decode('utf-8'))
                            response = self._process_request(request)
                            client_socket.sendall(json.dumps(response).encode('utf-8') + b'\n')
                        except json.JSONDecodeError:
                            error_response = {'status': 'error', 'message': 'Invalid JSON'}
                            client_socket.sendall(json.dumps(error_response).encode('utf-8') + b'\n')
        except Exception as e:
            print(f"Client handler error: {e}")
        finally:
            client_socket.close()
    
    def _process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process a client request."""
        cmd = request.get('command')
        
        if cmd == 'set':
            key = request.get('key')
            value = request.get('value')
            success = self.store.set(key, value)
            return {'status': 'ok', 'success': success}
        
        elif cmd == 'get':
            key = request.get('key')
            value = self.store.get(key)
            return {'status': 'ok', 'value': value}
        
        elif cmd == 'delete':
            key = request.get('key')
            success = self.store.delete(key)
            return {'status': 'ok', 'success': success}
        
        elif cmd == 'bulk_set':
            items = request.get('items', [])
            items = [(item['key'], item['value']) for item in items]
            success = self.store.bulk_set(items)
            return {'status': 'ok', 'success': success}
        
        else:
            return {'status': 'error', 'message': f'Unknown command: {cmd}'}
    
    def stop(self):
        """Stop the server gracefully."""
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        self.store.close()
