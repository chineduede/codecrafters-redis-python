from enum import IntEnum
from app.constants import BOUNDARY, STRING, ARRAY, BULK_STRING, NULL_BULK_STR, INTEGER
from app.util import encode

class EncodedMessageType(IntEnum):
    SIMPLE_STRING = 0
    BULK_STRING = 1
    NULL_STR = 2
    ARRAY = 3
    INTEGER = 4

class RespEncoder:

    def encode(self, message: bytes | list[bytes], _type: EncodedMessageType, **kwargs) -> bytes | None:
        match _type:
            case EncodedMessageType.SIMPLE_STRING:
                return self.encode_smpl_str(encode(message))
            case EncodedMessageType.BULK_STRING:
                return self.encode_bulk_str(encode(message))
            case EncodedMessageType.NULL_STR:
                return self.null_bulk_str()
            case EncodedMessageType.ARRAY:
                return self.encode_array(encode(message), **kwargs)
            case EncodedMessageType.INTEGER:
                return self.encode_integer(encode(message))
            case _:
                return None

    def encode_integer(self, msg) -> bytes:
        return INTEGER + msg + BOUNDARY
    
    def encode_smpl_str(self, message) -> bytes:
        return STRING + message + BOUNDARY
            
    def encode_bulk_str(self, message) -> bytes:
        return BULK_STRING + str(len(message)).encode() + BOUNDARY + message + BOUNDARY
    
    def encode_array(self, array, *, encode_type = EncodedMessageType.BULK_STRING):
        to_ret = [ARRAY, str(len(array)).encode(), BOUNDARY]

        for element in array:
            if isinstance(element, list):
                e = self.encode_array(element)
            elif element is None:
                e = self.null_bulk_str()
            else:
                e = self.encode(element, encode_type)

            if e is None:
                e = b''
            to_ret.append(e)
        return b''.join(to_ret)
            
    @staticmethod
    def null_bulk_str():
        return NULL_BULK_STR + BOUNDARY
    

ENCODER = RespEncoder()
# if __name__ == "__main__":
    # print(encoder.encode(['red', 'blue'], EncodedMessageType.ARRAY))