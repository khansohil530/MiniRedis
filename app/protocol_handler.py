"""
Protocol is based on Redis wire protocol.

Client sends requests as an array of bulk strings.

Server replies, indicating response type using the first byte:

* "+" - simple string
* "-" - error
* ":" - integer
* "$" - bulk string
* "^" - bulk bytes string
* "@" - json string (uses bulk string rules)
* "*" - array
* "%" - dict
* "&" - set

Simple strings: "+string content\r\n"  <-- cannot contain newlines

Error: "-Error message\r\n"

Integers: ":1337\r\n"

Bulk String: "$number of bytes\r\nstring data\r\n"

* Empty string: "$0\r\n\r\n"
* NULL: "$-1\r\n"

Bulk bytes string (encoded as UTF-8): "^number of bytes\r\ndata\r\n"

JSON string: "@number of bytes\r\nJSON string\r\n"

Array: "*number of elements\r\n...elements..."

* Empty array: "*0\r\n"

Dictionary: "%number of elements\r\n...key0...value0...key1...value1..\r\n"

Set: "&number of elements\r\n...elements..."
"""

from io import BytesIO
import json
from collections import deque
import datetime

from const import Error

class ProtocolHandler:
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'^': self.handle_bytes,
            b'@': self.handle_json,
            b'*': self.handle_array,
            b'%': self.handle_dict,
            b'&': self.handle_set,
        }
    
    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n')
    
    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n'))
    
    def handle_integer(self, socket_file):
        number = socket_file.readline().rstrip(b'\r\n')
        if b'.' in number:
            return float(number)
        
        return int(number)
    
    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return
        
        return socket_file.read(length+2)[:-2]

    def handle_bytes(self, socket_file):
        return self.handle_string(socket_file).decode('utf-8')
    
    def handle_json(self, socket_file):
        return json.loads(self.handle_string(socket_file))
    
    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items*2)]
        return dict(zip(elements[::2], elements[1::2]))
    
    def handle_set(self, socket_file):
        return set(self.handle_array(socket_file))
    
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise EOFError()
        
        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            return first_byte + socket_file.readline().rstrip(b'\r\n')
    
    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()
        
    def _write(self, buf, data):
        if isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, str):
            bdata = data.encode('utf-8')
            buf.write(b'^%d\r\n%s\r\n' % (len(bdata), bdata))
        elif data is True or data is False:
            buf.write(b':%d\r\n' % (1 if data else 0))
        elif isinstance(data, (int, float)):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write(b'-%s\r\n' % (data.message.encode('utf-8')))
        elif isinstance(data, (list, tuple, deque)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif isinstance(data, set):
            buf.write(b'&%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif data is None:
            buf.write(b'$-1\r\n')
        elif isinstance(data, datetime.datetime):
            self._write(buf, str(data))
