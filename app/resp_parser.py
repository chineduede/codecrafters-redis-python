from enum import IntEnum
from socket import socket
from selectors import BaseSelector

from app.constants import BOUNDARY, STRING, BULK_STRING, ARRAY, MAGIC_STR, INTEGER

class WrongMessage(Exception):
    pass

class WrongCallOrder(Exception):
    pass

class States(IntEnum):
    # READ_MESSAGE = 0
    READ_TYPE = 0
    READ_STR_LEN = 1
    READ_ARR_LEN = 2
    READ_STR = 3
    READ_SMPL_STR = 4
    READ_ARR_ELE = 5
    READ_INTEGER = 6

class RespParser:
    def __init__(self, *, debug=False) -> None:
        self.debug = debug
        self.running_idx = 0
        self.pos = 0
        self.init()
    
    def incr(self):
        self.pos +=1
    
    def consume_boundary(self):
        if self.buffer[self.pos:self.pos+1] == b'\r' and self.buffer[self.pos + 1:self.pos+2] == b'\n':
            self.pos += 2

    def init(self):
        self.buffer = b''
        self.buffer_type: bytes = None
        self.running_idx += self.pos
        self.pos = 0
        self.current_state = States.READ_TYPE
        self.misc = {}
        self.arr_stack = []

    def consume_until_boundary(self, len_to_consume = None):
        if len_to_consume is None:
            end_idx = self.buffer.find(BOUNDARY, self.pos)
            if end_idx == -1:
                return
            val = self.buffer[self.pos:end_idx]
        else:
            val = self.buffer[self.pos:self.pos+len_to_consume]
        self.pos += len(val)
        return val
            

    def add_ele_to_arr(self, ele):
        top_arr = self.arr_stack[-1]
        top_arr['items'].append(ele)
        self.current_state = States.READ_ARR_ELE

    def parse_all(self, sock: socket, selector: BaseSelector | None = None):
        buffer = b''
        while True:
            try:
                chunk = sock.recv(1024)
                if not chunk:
                    if selector:
                        selector.unregister(sock)
                    sock.close()
                    return
                buffer += chunk
            except BlockingIOError:
                break
        return self.parse_multiple(buffer)

    def parse_multiple(self, data: bytes):
        all_msg = []
        while msg := self.parse(data[self.running_idx:]):
            all_msg.append(msg)

        self.running_idx = 0
        return all_msg

    def parse(self, data: bytes):
        if self.buffer_type is None:
            self.buffer_type = data[:1]
        self.buffer += data

        while True:
            
            if self.current_state == States.READ_TYPE:
                if self.pos < len(self.buffer):
                    data_type = self.buffer[self.pos:self.pos+1]
                    self.incr()
                    if data_type == BULK_STRING:
                        self.current_state = States.READ_STR_LEN
                    elif data_type == STRING:
                        self.current_state = States.READ_SMPL_STR
                    elif data_type == ARRAY:
                        self.current_state = States.READ_ARR_LEN
                    elif data_type == INTEGER:
                        self.current_state = States.READ_INTEGER
                else:
                    # validate data here
                    break
            elif self.current_state == States.READ_STR_LEN:
                length = self.consume_until_boundary()
                if length is None:
                    break
                length = int(length)
                self.consume_boundary()
                # check if bulk_string plus boundary is in buffer
                if self.buffer[self.pos + length: self.pos + length + 2] == BOUNDARY:
                    self.misc['bulk_consume_boundary'] = True
                elif self.buffer[self.pos : self.pos + len(MAGIC_STR)] == MAGIC_STR:
                    self.misc['bulk_consume_boundary'] = False
                else:
                    break
                self.misc['bulk_len'] = length
                self.current_state = States.READ_STR
            elif self.current_state == States.READ_STR:
                bulk_str_len = self.misc['bulk_len']
                bulk_str = self.consume_until_boundary(bulk_str_len)
                if bulk_str is None:
                    break
                del self.misc['bulk_len']
                self.debug and print(f'Bulk string {bulk_str}')
                if self.misc['bulk_consume_boundary']:
                    self.consume_boundary()
                del self.misc['bulk_consume_boundary']
                self.current_state = States.READ_TYPE
                # if in array
                if self.arr_stack:
                    self.add_ele_to_arr(bulk_str)
                # initial type is string, we can return
                elif self.buffer_type == BULK_STRING:
                    self.init()
                    return bulk_str
            elif self.current_state == States.READ_INTEGER:
                value = self.consume_until_boundary()
                if value is None:
                    break
                else:
                    value = int(value)
                    self.debug and print(f'Simple string {value}')
                    # self.incr()
                    self.consume_boundary()
                    self.current_state = States.READ_TYPE
                    if self.arr_stack:
                        self.add_ele_to_arr(value)
                    elif self.buffer_type == INTEGER:
                        self.init()
                        return value
            elif self.current_state == States.READ_SMPL_STR:
                value = self.consume_until_boundary()
                if value is None:
                    break
                else:
                    self.debug and print(f'Simple string {value}')
                    # self.incr()
                    self.consume_boundary()
                    self.current_state = States.READ_TYPE
                    if self.arr_stack:
                        self.add_ele_to_arr(value)
                    elif self.buffer_type == STRING:
                        self.init()
                        return value
            elif self.current_state == States.READ_ARR_LEN:
                no_of_items = self.consume_until_boundary()
                if no_of_items is None:
                    break
                no_of_items = int(no_of_items)
                self.consume_boundary()
                self.arr_stack.append({'length' : no_of_items, 'items': []})
                self.current_state = States.READ_ARR_ELE
                # self.in_array = True
            elif self.current_state == States.READ_ARR_ELE:
                if self.arr_stack:
                    # check if topmost array is complete
                    top_ele = self.arr_stack[-1]
                    if top_ele['length'] == len(top_ele['items']):
                        # nested array
                        if len(self.arr_stack) > 1:
                            self.arr_stack[-2]['items'].append(top_ele['items'])
                            self.arr_stack.pop()
                            # we dont change state here, because we want to handle the last array
                            self.current_state = States.READ_ARR_ELE
                        else:
                            arr = self.arr_stack.pop()
                            if self.buffer_type == ARRAY:
                                self.init()
                                return arr['items']
                    else:
                        self.current_state = States.READ_TYPE
                else:
                    self.current_state = States.READ_TYPE
        return None
    

# parser = Parser('*')
# parser.parse('*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n')

# parser = Parser()
# strr = '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
# parser.set_type(strr[0])
# c1 = parser.parse(strr)
# print('c1', c1)
# parser.set_type('+')
# c2 = parser.parse('+World\r\n')
# print('c2', c2)

if __name__ == "__main__":
    st = "*5\r\n$4\r\nXADD\r\n$6\r\nbanana\r\n$3\r\n0-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
    parser = RespParser()
    parser.set_type(st[0])
    print(parser.parse())