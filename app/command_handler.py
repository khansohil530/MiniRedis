
from functools import wraps
from collections import deque
import time
import heapq

from const import Value, KV, SET, HASH, QUEUE
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
            
            # Set commands.
            b'SADD': self.sadd,
            b'SCARD': self.scard,
            b'SDIFF': self.sdiff,
            b'SDIFFSTORE': self.sdiffstore,
            b'SINTER': self.sinter,
            b'SINTERSTORE': self.sinterstore,
            b'SISMEMBER': self.sismember,
            b'SMEMBERS': self.smembers,
            b'SPOP': self.spop,
            b'SREM': self.srem,
            b'SUNION': self.sunion,
            b'SUNIONSTORE': self.sunionstore,
            
            b'EXPIRE': self.expire
        }
        
    def handle(self, command):
        return self._commands[command]
        
    def enforce_datatype(data_type, set_missing=True, subtype=None):
        def decorator(func):
            @wraps(func)
            def inner(self, key, *args, **kwargs):
                self.check_datatype(data_type, key, set_missing, subtype)
                return func(self, key, *args, **kwargs)
            return inner
        return decorator
    
    def check_datatype(self, data_type, key, set_missing=True, subtype=None):
        if key in self._kv and self.check_expired(key):
            del self._kv[key]
        
        if key in self._kv:
            value = self._kv[key]
            if value.data_type != data_type:
                raise CommandError('Operation against wrong key type.')
            if subtype is not None and not isinstance(value.value, subtype):
                raise CommandError('Operation against wrong value type.')
        elif set_missing:
            if data_type == HASH:
                value = {}
            elif data_type == QUEUE:
                value = deque()
            elif data_type == SET:
                value = set()
            elif data_type == KV:
                value = ''
            
            self._kv[key] = Value(data_type, value)
    
    @enforce_datatype(SET)
    def sadd(self, key, *members):
        self._kv[key].value.update(members)
        return self.scard(key)
    
    @enforce_datatype(SET)
    def scard(self, key):
        return len(self._kv[key].value)
    
    @enforce_datatype(SET)
    def sdiff(self, key, *keys):
        diff = set(self._kv[key].value)
        for other_key in keys:
            if other_key in self._kv:
                self.check_datatype(SET, other_key)
                diff -= set(self._kv[other_key].value)
        
        return list(diff)
    
    @enforce_datatype(SET)
    def sdiffstore(self, dest, key, *keys):
        diff = set(self.sdiff(key, *keys))
        self.check_datatype(SET, dest)
        self._kv[dest] = Value(SET, diff)
        return len(diff)
    
    @enforce_datatype(SET)
    def sinter(self, key, *keys):
        src = set(self._kv[key].value)
        for other_key in keys:
            self.check_datatype(SET, other_key)
            src &= self._kv[other_key].value

        return list(src)
    
    @enforce_datatype(SET)
    def sinterstore(self, dest, key, *keys):
        inter = set(self.sinter(key, *keys))
        self.check_datatype(SET, dest)
        self._kv[dest] = Value(SET, inter)
        return len(inter)
    
    @enforce_datatype(SET)
    def sismember(self, key, member):
        return 1 if member in self._kv[key].value else 0
    
    @enforce_datatype(SET)
    def smembers(self, key):
        return self._kv[key].value
    
    @enforce_datatype(SET)
    def spop(self, key, n=1):
        accum = []
        for _ in range(n):
            try:
                accum.append(self._kv[key].value.pop())
            except KeyError:
                break
        return accum
    
    @enforce_datatype(SET)
    def srem(self, key, *members):
        ct = 0
        for member in members:
            try:
                self._kv[key].value.remove(member)
            except KeyError:
                pass
            else:
                ct += 1
        return ct

    @enforce_datatype(SET)
    def sunion(self, key, *keys):
        src = set(self._kv[key].value)
        for key in keys:
            self.check_datatype(SET, key)
            src |= self._kv[key].value
        return list(src)

    @enforce_datatype(SET)
    def sunionstore(self, dest, key, *keys):
        un = set(self.sunion(key, *keys))
        self.check_datatype(SET, dest)
        self._kv[dest] = Value(SET, un)
        return len(un)
    
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
    
    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
    def kv_decr(self, key):
        return self._kv_incr(key, -1)
    
    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
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
    
    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
    def kv_incr(self, key):
        return self._kv_incr(key, 1)
    
    @enforce_datatype(KV, set_missing=False, subtype=(float, int))
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
        