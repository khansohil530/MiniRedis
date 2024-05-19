from collections import namedtuple

Error = namedtuple('Error', ('message',))
Value = namedtuple('Value', ('data_type', 'value'))

KV = 0