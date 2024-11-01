from app.resp_parser import *

st = b"*5\r\n$4\r\nXADD\r\n$6\r\nbanana\r\n$3\r\n0-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
parser = RespParser()
print(parser.parse_multiple(st))
