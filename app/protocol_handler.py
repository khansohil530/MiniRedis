from io import BytesIO

class ProtocolHandler:
    def __init__(self):
        pass
    
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise EOFError()
        
        return first_byte + socket_file.readline()
    
    def write_response(self, socket_file, data):
        buf = BytesIO()
        bdata = data.encode('utf-8')
        buf.write(bdata)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()