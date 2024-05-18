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
        
    
    def execute(self, data):
        socket_file = create_socket_file(self._host, self._port)
        
        # write
        self._protocol.write_response(socket_file, data)

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