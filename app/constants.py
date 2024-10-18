from enum import StrEnum

BOUNDARY = '\r\n'

class RespType(StrEnum):
    STRING = '+'
    BULK_STRING = '$'
    ARRAY = '*'
