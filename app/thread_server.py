import socketserver as ss

class ThreadedStreamServer:
    def __init__(self, address, handler):
        self.address = address
        self.handler = handler
    
    def serve_forever(self):
        handler = self.handler
        class RequestHandler(ss.BaseRequestHandler):
            def handle(self):
                return handler(self.request, self.client_address)

        class ThreadedServer(ss.ThreadingMixIn, ss.TCPServer):
            allow_reuse_address = True

        self.stream_server = ThreadedServer(self.address, RequestHandler)
        self.stream_server.serve_forever()

    def stop(self):
        self.stream_server.shutdown()