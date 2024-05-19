from const import Value, KV
from collections import deque
import time
import heapq


class CommandHandler:
    def __init__(self):
        self._kv = {}
        
        self._expiry_map = {}
        self._expiry = []

        self._commands = {
            b'SET': self.kv_set,
            b'GET': self.kv_get,
            b'LEN': self.kv_len,
            b'EXPIRE': self.expire
        }
    
    def handle(self, command):
        return self._commands[command]
    
    
    def kv_set(self, key, value):
        data_type = KV
        self.unexpire(key)
        self._kv[key] = Value(data_type, value)
        return 1
    
    def kv_get(self, key):
        if key in self._kv and not self.check_expired(key):
            return self._kv[key].value
    
    def kv_len(self):
        return len(self._kv)
    
    def expire(self, key, nseconds):
        eta = time.time()+nseconds
        self._expiry_map[key] = eta
        heapq.heappush(self._expiry, (eta, key))
    
    def check_expired(self, key, ts=None):
        ts = ts or time.time()
        return key in self._expiry_map and ts > self._expiry_map[key]
    
    def unexpire(self, key):
        self._expiry_map.pop(key, None)
    
    def clean_expired(self, ts=None): # TODO: schedule clean up via schedular process
        ts = ts or time.time()
        n = 0
        while self._expiry:
            expires, key = heapq.heappop(self._expiry)
            if ts > expires:
                heapq.heappush(self._expiry, (expires, key))
                break
                
            if self._expiry_map.get(key) == expires:
                del self._expiry_map[key]
                del self._kv[key]
                n +=1
        return n
        