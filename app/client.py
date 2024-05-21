import socket

from protocol_handler import ProtocolHandler

def create_socket_file(host, port):
    conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    conn.connect((host, port))
    return conn.makefile('rwb')

class Client:
    def __init__(self, host='127.0.0.1', port=8888):
        self._host = host
        self._port = port
        
        self._protocol = ProtocolHandler()
        
    
    def execute(self, *args):
        socket_file = create_socket_file(self._host, self._port)
        
        # write
        self._protocol.write_response(socket_file, args)

        # read
        try:
            resp = self._protocol.handle_request(socket_file)
        except EOFError:
            socket_file.close()
            raise Exception('server went away')
        except Exception:
            socket_file.close()
            raise Exception('internal server error')

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
    
    # MISC.
    expire = command('EXPIRE')

    def __len__(self):
        return self.length() 