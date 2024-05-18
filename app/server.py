import logging
from optparse import OptionParser
import sys
from gevent.pool import Pool
from gevent.server import StreamServer
from gevent.thread import get_ident

from protocol_handler import ProtocolHandler


logger = logging.getLogger(__name__)

class QueueServer:
    def __init__(self, host='0.0.0.0', port=8888, max_clients=2**10):
        self._host = host
        self._port = port
        self._max_clients = max_clients
    
        self._pool = Pool(self._max_clients)
        self._server = StreamServer((self._host, self._port),
                                    self.connection_handler,
                                    spawn=self._pool)
        
        self._protocol = ProtocolHandler()
        
        
    
    def connection_handler(self, conn, address):
        logger.info(f'Request received on address {address[0]}:{address[1]}')
        # converting socket into file like objects, ease of use, can read/write by line
        # without multiple recv or send calls 
        socket_file = conn.makefile('rwb')
        while True:
            try:
                self.request_response(socket_file)
            except EOFError:
                logger.info(f"Finished reading request at {address[0]}:{address[1]}")
                socket_file.close()
                break
            except Exception as e:
                logger.error(f"Error processing request. {str(e)}")
                break
        logger.info('Finished reading request')
    
    def request_response(self, socket_file):
        data = self._protocol.handle_request(socket_file)
        try:
            resp = self.respond(data)
        except Exception as e:
            logger.exception(f'Unhandled error {str(e)}')
            resp = "Unhandled error"
        self._protocol.write_response(socket_file, resp)
    
    def respond(self, data):
        return "Persisted response\n"
    
    def run(self):
        print('Server is running')
        self._server.serve_forever()
                
        

def get_option_parse() -> OptionParser:
    parser = OptionParser()
    parser.add_option('-H', '--host', default='127.0.0.1', dest='host', help='Host to listen on.')
    parser.add_option('-p', '--port', default='8888', dest='port', help='Port to listen on.', type='int')
    parser.add_option('-d', '--debug', action='store_true', dest='debug', help='Log debug messages.')
    parser.add_option('-e', '--errors', action='store_true', dest='error', help='Log error messages only.')
    parser.add_option('-m', '--max-clients', default=1024, dest='max_clients', help='Maximum number of clients.', type=int)
    
    return parser

def configure_logger(options: OptionParser):
    logger.addHandler(logging.StreamHandler())
    if options.debug:
        logger.setLevel(logging.DEBUG)
    elif options.error:
        logger.setLevel(logging.ERROR)
    else:
        logger.setLevel(logging.INFO)
    

if __name__ == "__main__":
    options, args = get_option_parse().parse_args()
    try:
        from gevent import monkey; monkey.patch_all()
    except Exception:
        logger.error("Error applying monkey patch for gevent")
        sys.stderr.write("Error applying monkey patch for gevent")
        sys.stderr.flush()
        sys.exit(1)
    
    configure_logger(options)
    server = QueueServer(host=options.host, port=options.port, max_clients=options.max_clients)
    print('\x1b[32m  / \\__')
    print(' \x1b[32m (    @\\____', 
          '\x1b[1;32mMiniRedis '
          f'\x1b[1;33m{options.host}:{options.port}\x1b[32m')
    print(' \x1b[32m /         O')
    print(' \x1b[32m/   (_____ /')
    print(' \x1b[32m\\_____/   U')
    try:
        server.run()
    except KeyboardInterrupt:
        print('\x1b[1;31mshutting down\x1b[0m')

    
    
    