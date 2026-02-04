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

from indexing_module import IndexManager


class KVStore:
    def __init__(self, data_dir: str = "kvstore_data", instance_id: str = "0"):
        self.data_dir = Path(data_dir) / f"node_{instance_id}"
        self.data_dir.mkdir(parents=True, exist_ok=True)  # Creates ALL parent dirs

        self.log_file = self.data_dir / "wal.log"
        self.snapshot_file = self.data_dir / "snapshot.json"

        self.data: Dict[str, str] = {}
        self.lock = threading.Lock()

        # Initialize indexing FIRST
        self.index_manager = IndexManager()
        self.index_file = self.data_dir / "indexes.json"

        # Load data from disk
        self._load_from_disk()

        # Open WAL for appending
        self.log_fd = open(self.log_file, 'ab', buffering=0)

        # Load indexes from disk (after data is loaded)
        self._load_indexes()

        # Rebuild indexes if they don't exist but we have data
        if not self.index_file.exists() and len(self.data) > 0:
            print(f"Rebuilding indexes for {len(self.data)} keys...")
            for key, value in self.data.items():
                self.index_manager.index_value(key, value)

    def _load_indexes(self):
        """Load indexes from disk."""
        if self.index_file.exists():
            try:
                with open(self.index_file, 'r') as f:
                    index_data = json.load(f)
                self.index_manager = IndexManager.from_dict(index_data)
                print(f"Loaded indexes from {self.index_file}")
            except Exception as e:
                print(f"Warning: Could not load indexes: {e}")

    def _load_from_disk(self):
        """Recover state from Snapshot + WAL."""
        if self.snapshot_file.exists():
            try:
                with open(self.snapshot_file, 'r') as f:
                    self.data = json.load(f)
            except (json.JSONDecodeError, IOError):
                print("Warning: Corrupt snapshot found, ignoring.")

        if self.log_file.exists():
            with open(self.log_file, 'r') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        entry = json.loads(line)
                        self._apply_entry(entry)
                    except json.JSONDecodeError:
                        print("Found incomplete WAL entry (crash recovery), discarding.")
                        continue

    def _apply_entry(self, entry: Dict):
        """Apply a log entry to in-memory data."""
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
    def get_with_clock(self, key):
        data = self.get(key)
        if data is None:
            return None, {}
        return data['value'], data.get('clock', {})

    def set_with_clock(self, key, value, clock):
        self.set(key, {'value': value, 'clock': clock})

    def apply_replication_log(self, entry):
        if entry['type'] == 'set':
            self.set(entry['key'], entry['data'])
        elif entry['type'] == 'delete':
            self.delete(entry['key'])
    def apply_replication_log(self, entry: Dict):
        """Used by replication to apply logs from other nodes."""
        with self.lock:
            self._write_wal(entry)
            self._apply_entry(entry)

            # Index replicated data
            if entry['type'] == 'set':
                self.index_manager.index_value(entry['key'], entry['value'])
            elif entry['type'] == 'bulk_set':
                for k, v in entry['items']:
                    self.index_manager.index_value(k, v)
            elif entry['type'] == 'delete':
                self.index_manager.remove_value(entry['key'])

    # =====================================================================
    # CRUD
    # =====================================================================
    def set(self, key: str, value: str) -> bool:
        entry = {'type': 'set', 'key': key, 'value': value}
        with self.lock:
            self._write_wal(entry)
            self._apply_entry(entry)
            self.index_manager.index_value(key, value)
        return True

    def get(self, key: str) -> Optional[str]:
        with self.lock:
            return self.data.get(key)

    def delete(self, key: str) -> bool:
        with self.lock:
            if key not in self.data:
                return False
            entry = {'type': 'delete', 'key': key}
            self._write_wal(entry)
            self._apply_entry(entry)
            self.index_manager.remove_value(key)
        return True

    def bulk_set(self, items: List[Tuple[str, str]]) -> bool:
        entry = {'type': 'bulk_set', 'items': items}
        with self.lock:
            self._write_wal(entry)
            self._apply_entry(entry)
            for key, value in items:
                self.index_manager.index_value(key, value)
        return True

    # =====================================================================
    # SEARCH
    # =====================================================================
    def full_text_search(self, query: str, top_k: int = 10):
        return self.index_manager.full_text_search(query, top_k)

    def semantic_search(self, query: str, top_k: int = 10):
        return self.index_manager.semantic_search(query, top_k)

    def phrase_search(self, phrase: str):
        return self.index_manager.phrase_search(phrase)

    # =====================================================================
    # SNAPSHOT + INDEX PERSISTENCE
    # =====================================================================
    def save_snapshot(self, debug_chaos: bool = False) -> bool:
        """Compacts WAL into a snapshot."""
        with self.lock:
            if debug_chaos and random.random() < 0.50:
                return False

            # Ensure directory exists
            self.data_dir.mkdir(parents=True, exist_ok=True)

            temp_file = self.snapshot_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(self.data, f)

            os.replace(temp_file, self.snapshot_file)
            return True

    def save_indexes(self):
        """Save indexes to disk safely using atomic write."""
        try:
            with self.lock:
                index_data = self.index_manager.to_dict()

            # Ensure directory exists BEFORE writing temp file
            self.data_dir.mkdir(parents=True, exist_ok=True)

            temp_file = self.index_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(index_data, f)

            os.replace(temp_file, self.index_file)
            return True
        except Exception as e:
            print(f"Error saving indexes: {e}")
            return False

    # =====================================================================
    # CLOSE
    # =====================================================================
    def close(self):
        """Gracefully close the file handle."""
        if self.log_fd:
            try:
                os.fsync(self.log_fd.fileno())
                self.log_fd.close()
            except ValueError:
                pass  # Already closed


# ============================================================================
# TCP SERVER
# ============================================================================

class KVServer:
    """TCP server for the key-value store."""

    def __init__(self, host: str = 'localhost', port: int = 8000, data_dir: str = "kvstore_data"):
        self.host = host
        self.port = port
        self.store = KVStore(data_dir)
        self.running = False
        self.server_socket = None

    def _auto_save_indexes(self):
        """Periodically save indexes to disk."""
        while self.running:
            time.sleep(30)  # Every 30 seconds
            if self.running:
                self.store.save_indexes()

    def start(self):
        """Start the TCP server."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        self.running = True

        # Start auto-save thread
        threading.Thread(target=self._auto_save_indexes, daemon=True).start()

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
            pass  # Client disconnected
        finally:
            client_socket.close()

    def _process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Process a client request and return response."""
        cmd = request.get('command')

        # --- Search commands ---
        if cmd == 'full_text_search':
            results = self.store.full_text_search(request.get('query', ''), request.get('top_k', 10))
            return {'status': 'ok', 'results': results}

        elif cmd == 'phrase_search':
            results = self.store.phrase_search(request.get('phrase', ''))
            return {'status': 'ok', 'results': results}

        elif cmd == 'semantic_search':
            results = self.store.semantic_search(request.get('query', ''), request.get('top_k', 10))
            return {'status': 'ok', 'results': results}

        elif cmd == 'save_indexes':
            success = self.store.save_indexes()
            return {'status': 'ok', 'success': success}

        # --- CRUD commands ---
        elif cmd == 'set':
            success = self.store.set(request.get('key'), request.get('value'))
            return {'status': 'ok', 'success': success}

        elif cmd == 'get':
            value = self.store.get(request.get('key'))
            return {'status': 'ok', 'value': value}

        elif cmd == 'delete':
            success = self.store.delete(request.get('key'))
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
        print("Saving indexes...")
        self.store.save_indexes()
        if self.server_socket:
            self.server_socket.close()
        self.store.close()