from enum import IntEnum
from app.constants import BOUNDARY, STRING, ARRAY, BULK_STRING, NULL_BULK_STR, INTEGER, ERR
from app.util import encode

QUEUED = b'+QUEUED\r\n'

class EncodedMessageType(IntEnum):
    SIMPLE_STRING = 0
    BULK_STRING = 1
    NULL_STR = 2
    ARRAY = 3
    INTEGER = 4
    ERROR = 5

class RespEncoder:

    def encode(self, message: bytes | list[bytes], _type: EncodedMessageType, **kwargs) -> bytes | None:
        match _type:
            case EncodedMessageType.SIMPLE_STRING:
                return self.encode_smpl_str(encode(message))
            case EncodedMessageType.BULK_STRING:
                return self.encode_bulk_msg(encode(message))
            case EncodedMessageType.NULL_STR:
                return self.null_bulk_str()
            case EncodedMessageType.ARRAY:
                return self.encode_array(encode(message), **kwargs)
            case EncodedMessageType.INTEGER:
                return self.encode_integer(encode(message))
            case EncodedMessageType.ERROR:
                return self.encode_smpl_str(encode(message), ERR)
            case _:
                return None

    def encode_integer(self, msg) -> bytes:
        return INTEGER + msg + BOUNDARY
    
    def encode_smpl_str(self, message, starter=STRING) -> bytes:
        return starter + message + BOUNDARY
            
    def encode_bulk_msg(self, message) -> bytes:
        return BULK_STRING + str(len(message)).encode() + BOUNDARY + message + BOUNDARY
    
    def encode_array(self, array, *, already_encoded = False, encode_type = EncodedMessageType.BULK_STRING):
        to_ret = [ARRAY, str(len(array)).encode(), BOUNDARY]

        for element in array:
            if not already_encoded:
                if isinstance(element, list):
                    element = self.encode_array(element)
                elif element is None:
                    element = self.null_bulk_str()
                else:
                    element = self.encode(element, encode_type)

                if element is None:
                    element = b''
            to_ret.append(element)
        return b''.join(to_ret)
            
    @staticmethod
    def null_bulk_str():
        return NULL_BULK_STR + BOUNDARY
    

ENCODER = RespEncoder()
# if __name__ == "__main__":
    # print(encoder.encode(['red', 'blue'], EncodedMessageType.ARRAY))