from const import Value, KV
from collections import deque
import time
import heapq

from exc import CommandError


class CommandHandler:
    def __init__(self):
        self._kv = {}
        
        self._expiry_map = {}
        self._expiry = []

        self._commands = {
            # Key value commands
            b'APPEND': self.kv_append,
            b'DECR': self.kv_decr,
            b'DECRBY': self.kv_decrby,
            b'DELETE': self.kv_delete,
            b'EXISTS': self.kv_exists,
            b'GET': self.kv_get,
            b'GETSET': self.kv_getset,
            b'INCR': self.kv_incr,
            b'INCRBY': self.kv_incrby,
            b'MDELETE': self.kv_mdelete,
            b'MGET': self.kv_mget,
            b'MPOP': self.kv_mpop,
            b'MSET': self.kv_mset,
            b'MSETEX': self.kv_msetex,
            b'POP': self.kv_pop,
            b'SET': self.kv_set,
            b'SETNX': self.kv_setnx,
            b'SETEX': self.kv_setex,
            b'LEN': self.kv_len,
            b'FLUSH': self.kv_flush,
            
            b'EXPIRE': self.expire
        }
    
    def handle(self, command):
        return self._commands[command]
    
    def kv_exists(self, key):
        return 1 if key in self._kv and not self.check_expired(key) else 0
    
    def kv_append(self, key, value):
        if not self.kv_exists(key):
            return self.kv_set(key, value)
        else:
            kv_val = self._kv[key]
            try:
                kv_val = Value(kv_val.data_type, kv_val.value + value)
                self._kv[key] = kv_val
            except:
                raise CommandError('Incompatible data-types')
        
        return self._kv[key].value
    
    def _kv_incr(self, key, n):
        if self.kv_exists(key):
            value = self._kv[key].value + n
        else:
            self.unexpire(key)
            value = n
        
        self._kv[key] = Value(KV, value)
        return value
    
    def kv_decr(self, key):
        return self._kv_incr(key, -1)
    
    def kv_decrby(self, key, n):
        return self._kv_incr(key, -n)
    
    def kv_delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0
    
    def kv_set(self, key, value):
        data_type = KV
        self.unexpire(key)
        self._kv[key] = Value(data_type, value)
        return 1
    
    def kv_setnx(self, key, value):
        if self.kv_exists(key):
            return 0
        else:
            self.unexpire(key)
            self._kv[key] = Value(KV, value)
            return 1
    
    def kv_setex(self, key, value, expires):
        self.kv_set(key, value)
        self.expire(key, expires)
        return 1
    
    def kv_get(self, key):
        if key in self._kv and not self.check_expired(key):
            return self._kv[key].value
    
    def kv_getset(self, key, value):
        if self.kv_exists(key):
            orig = self._kv[key].value
        else:
            orig = None
        
        self._kv[key] = Value(KV, value)
        return orig
    
    def kv_incr(self, key):
        return self._kv_incr(key, 1)
    
    def kv_incrby(self, key, n):
        return self._kv_incr(key, n)
    
    def kv_mdelete(self, *keys):
        n = 0
        for key in keys:
            try:
                del self._kv[key]
            except KeyError:
                pass
            else:
                n +=1
        return n
    
    def kv_mget(self, *keys):
        accum = []
        for key in keys:
            if self.kv_exists(key):
                accum.append(self._kv[key].value)
            else:
                accum.append(None)
        return accum
    
    def kv_mpop(self, *keys):
        accum = []
        for key in keys:
            if self.kv_exists(key):
                accum.append(self._kv.pop(key).value)
            else:
                accum.append(None)
        return accum
    
    def kv_mset(self, __data=None, **kwargs):
        n = 0
        data = {}
        if __data is not None:
            data.update(__data)
        if kwargs is not None:
            data.update(kwargs)
        
        for key, value in data.items():
            self.unexpire(key)
            self._kv[key] = Value(KV, value)
            n += 1

        return n

    def kv_msetex(self, data, expires):
        self.kv_mset(data)
        for key in data:
            self.expire(key, expires)
    
    def kv_pop(self, key):
        if self.kv_exists(key):
            return self._kv.pop(key).value
    
    def kv_len(self):
        return len(self._kv)
    
    def expire(self, key, nseconds):
        eta = time.time()+nseconds
        self._expiry_map[key] = eta
        heapq.heappush(self._expiry, (eta, key))
    
    def kv_flush(self):
        kvlen = self.kv_len()
        self._kv.clear()
        self._expiry = []
        self._expiry_map = {}
        return kvlen
    
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
        