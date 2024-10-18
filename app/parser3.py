class IncompleteStream(Exception):
    pass


class Parser:
    SEP = '\r\n'

    def __init__(self, recv_str: str) -> None:
        self.input_stream = recv_str
        self.ends_with_boundary = Parser.stream_boundary(recv_str)
        self.input_end = False
        self.pos = 0
        self.first_byte = self.input_stream[0]
        # self.

    @staticmethod
    def stream_boundary(stream):
        return stream[-2:] == Parser.SEP

    def input_is_complete(self):
        # we know it must end with Parser.SEP, if not
        # we return early
        if not self.ends_with_boundary:
            return False, None
        
        try:
            parsed_res = self.parse()
            return True, parsed_res
        except IncompleteStream as e:
            print(e)
            return False, None
    
    def run(self):
        return self.input_is_complete()

    def re_run(self, more_str: str):
        self.input_stream += more_str
        self.ends_with_boundary = Parser.stream_boundary(self.input_stream)
        self.pos = 0
        return self.input_is_complete()

    def read_char(self):
        if self.pos < len(self.input_stream):
            char = self.input_stream[self.pos]
            self.pos += 1
            return char
        return None
    
    def read_until_boundary(self) -> str | None:
        end_of_token = self.input_stream.find(Parser.SEP, self.pos)
        if end_of_token == -1:
            return None

        # print()
        token = self.input_stream[self.pos:end_of_token]
        self.pos = end_of_token + 2
        return token
    
    
    def parse_string(self):
        string = self.read_until_boundary()
        if string is None:
            raise IncompleteStream('Incompte stream ', self.input_stream)
        return string
    
    def parse_array(self):
        num_of_ele = self.read_until_boundary()
        if not num_of_ele:
            raise IncompleteStream('Incompte stream ', self.input_stream)
        num_of_ele = int(num_of_ele)
        result = []
        for _ in range(num_of_ele):
            result.append(self.parse())

        if len(result) != num_of_ele:
            raise IncompleteStream('Incompte stream ', self.input_stream)
        return result
    
    def parse_bulk_string(self):
        len_of_str = self.read_until_boundary()
        if not len_of_str:
            raise IncompleteStream('Incompte stream ', self.input_stream)
        len_of_str = int(len_of_str)

        # read bulk string
        string = self.read_until_boundary()

        if not string or len(string) != len_of_str:
            raise IncompleteStream('Incompte stream ', self.input_stream)
        return string

    
    def parse(self):

        cmd = self.read_char()

        match cmd:
            case '+':
                return self.parse_string()
            case '*':
                return self.parse_array()
            case '$':
                return self.parse_bulk_string()
  
arr = '*2\r\n*3\r\n+1\r\n+2\r\n'
c = '$5\r\nhello\r\n'
parser = Parser(arr)
print(parser.run())
print(parser.re_run('+3\r\n*2\r\n+Hello\r\n+World\r\n'))
# print(parser.re_run('y\r\n'))cls

