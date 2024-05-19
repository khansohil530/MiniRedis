from const import Value, KV
from collections import deque


class CommandHandler:
    def __init__(self):
        self._kv = {}
        self._commands = {
            b'SET': self.kv_set,
            b'GET': self.kv_get,
            b'LEN': self.kv_len
        }
    
    def handle(self, command):
        return self._commands[command]
    
    
    def kv_set(self, key, value):
        data_type = KV
        self._kv[key] = Value(data_type, value)
        return 1
    
    def kv_get(self, key):
        if key in self._kv:
            return self._kv[key].value
    
    def kv_len(self):
        return len(self._kv)