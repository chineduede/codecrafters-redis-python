from enum import StrEnum

BOUNDARY = '\r\n'
NULL_BULK_STR = '$-1'

class RespType(StrEnum):
    STRING = '+'
    BULK_STRING = '$'
    ARRAY = '*'
