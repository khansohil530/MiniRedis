import socket
from gevent.thread import get_ident
import time
import heapq

from protocol_handler import ProtocolHandler
from exc import CommandError
from const import Error


class SocketPool:
    def __init__(self, host, port, max_age=60):
        self.host = host
        self.port = port
        self.max_age = max_age
        self.free = []
        self.in_use = {}
        self._tid = get_ident
    
    def checkout(self):
        now = time.time()
        tid = self._tid()
        if tid in self.in_use:
            sock = self.in_use[tid]
            if sock.closed:
                del self.in_use[tid]
            else:
                return self.in_use[tid]
        
        while self.free:
            ts, sock = heapq.heappop(self.free)
            if ts < now - self.max_age:
                try:
                    sock.close()
                except OSError:
                    pass
            else:
                self.in_use[tid] = sock
                return sock
        sock = self.create_socket_file()
        self.in_use[tid] = sock
        return sock
        
    def create_socket_file(self):
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        conn.connect((self.host, self.port))
        return conn.makefile('rwb')

    def checkin(self):
        tid = self._tid()
        if tid in self.in_use:
            sock = self.in_use.pop(tid)
            if not sock.closed:
                heapq.heappush(self.free, (time.time(), sock))
            return True
        return False
    
    def close(self):
        tid = self._tid()
        sock = self.in_use.pop(tid, None)
        if sock:
            try:
                sock.close()
            except OSError:
                pass
            return True
        return False
    
class Client:
    def __init__(self, host='127.0.0.1', port=8888, pool_max_age=60):
        self._host = host
        self._port = port
        
        self._protocol = ProtocolHandler()
        self._socket_pool = SocketPool(host, port, pool_max_age)
        
    
    def execute(self, *args):
        socket_file = self._socket_pool.checkout()
        close_conn = args[0] in (b'QUIT', b'SHUTDOWN')
        self._protocol.write_response(socket_file, args)
        try:
            resp = self._protocol.handle_request(socket_file)
        except EOFError:
            self._socket_pool.close()
            raise Exception('server went away')
        except Exception:
            self._socket_pool.close()
            raise Exception('internal server error')
        else:
            if close_conn:
                self._socket_pool.close()
            else:
                self._socket_pool.checkin()
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp
    
    def command(cmd):
        def method(self, *args):
            return self.execute(cmd.encode('utf-8'), *args)
        
        return method
    
    # key value commands
    append = command('APPEND')
    decr = command('DECR')
    decrby = command('DECRBY')
    delete = command('DELETE')
    exists = command('EXISTS')
    get = command('GET')
    getset = command('GETSET')
    incr = command('INCR')
    incrby = command('INCRBY')
    mdelete = command('MDELETE')
    mget = command('MGET')
    mpop = command('MPOP')
    mset = command('MSET')
    msetex = command('MSETEX')
    pop = command('POP')
    set = command('SET')
    setex = command('SETEX')
    setnx = command('SETNX')
    length = command('LEN')
    flush = command('FLUSH')
    
    # SET commands
    sadd = command('SADD')
    scard = command('SCARD')
    sdiff = command('SDIFF')
    sdiffstore = command('SDIFFSTORE')
    sinter = command('SINTER')
    sinterstore = command('SINTERSTORE')
    sismember = command('SISMEMBER')
    smembers = command('SMEMBERS')
    spop = command('SPOP')
    srem = command('SREM')
    sunion = command('SUNION')
    
    # HASHMAP commands
    hdel = command('HDEL')
    hexists = command('HEXISTS')
    hget = command('HGET')
    hgetall = command('HGETALL')
    hincrby = command('HINCRBY')
    hkeys = command('HKEYS')
    hlen = command('HLEN')
    hmget = command('HMGET')
    hmset = command('HMSET')
    hset = command('HSET')
    hsetnx = command('HSETNX')
    hvals = command('HVALS')
    
    # Queue commands
    lpush = command('LPUSH')
    rpush = command('RPUSH')
    lpop = command('LPOP')
    rpop = command('RPOP')
    lrem = command('LREM')
    llen = command('LLEN')
    lindex = command('LINDEX')
    lrange = command('LRANGE')
    lset = command('LSET')
    ltrim = command('LTRIM')
    rpoplpush = command('RPOPLPUSH')
    lflush = command('LFLUSH')
    
    # MISC.
    expire = command('EXPIRE')
    flushall = command('FLUSHALL')
    quit = command('QUIT')
    shutdown = command('SHUTDOWN')
    save = command('SAVE')
    restore = command('RESTORE')
    merge = command('MERGE')

    def __len__(self):
        return self.length() 