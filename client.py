import socket
import json
import threading
from typing import Optional, List, Tuple, Dict, Any



class KVClient:
    """Client for the key-value store."""
    
    def __init__(self, host: str = 'localhost', port: int = 8000):
        self.host = host
        self.port = port
        self.socket = None
        self.buffer = b""
        self.lock = threading.Lock()
        self._connect()
    
    def _connect(self):
        """Connect to the server."""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
    
    def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Send a request and get response."""
        with self.lock:
            # Send request
            message = json.dumps(request).encode('utf-8') + b'\n'
            self.socket.sendall(message)
            
            # Receive response
            while b'\n' not in self.buffer:
                chunk = self.socket.recv(4096)
                if not chunk:
                    raise ConnectionError("Connection closed")
                self.buffer += chunk
            
            line, self.buffer = self.buffer.split(b'\n', 1)
            return json.loads(line.decode('utf-8'))
    
    def Set(self, key: str, value: str) -> bool:
        """Set a key-value pair."""
        request = {'command': 'set', 'key': key, 'value': value}
        response = self._send_request(request)
        return response.get('success', False)
    
    def Get(self, key: str) -> Optional[str]:
        """Get value for a key."""
        request = {'command': 'get', 'key': key}
        response = self._send_request(request)
        return response.get('value')
    
    def Delete(self, key: str) -> bool:
        """Delete a key."""
        request = {'command': 'delete', 'key': key}
        response = self._send_request(request)
        return response.get('success', False)
    
    def BulkSet(self, items: List[Tuple[str, str]]) -> bool:
        """Set multiple key-value pairs."""
        items_dict = [{'key': k, 'value': v} for k, v in items]
        request = {'command': 'bulk_set', 'items': items_dict}
        response = self._send_request(request)
        return response.get('success', False)
    
    def close(self):
        """Close the connection."""
        if self.socket:
            self.socket.close()
