from collections import namedtuple

Error = namedtuple('Error', ('message',))
Value = namedtuple('Value', ('data_type', 'value'))

KV = 0
HASH = 1
QUEUE = 2
SET = 3
