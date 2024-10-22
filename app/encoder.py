from enum import IntEnum
from app.constants import BOUNDARY, RespType, NULL_BULK_STR

class EncodedMessageType(IntEnum):
    SIMPLE_STRING = 0
    BULK_STRING = 1
    NULL_STR = 2
    ARRAY = 3

class RespEncoder:

    def encode(self, message: str | list, _type: EncodedMessageType, **kwargs) -> bytes | None:
        match _type:
            case EncodedMessageType.SIMPLE_STRING:
                return self.encode_smpl_str(message)
            case EncodedMessageType.BULK_STRING:
                return self.encode_bulk_str(message)
            case EncodedMessageType.NULL_STR:
                return self.null_bulk_str()
            case EncodedMessageType.ARRAY:
                return self.encode_array(message, **kwargs)
            case _:
                return None
            
    def encode_smpl_str(self, message) -> bytes:
        return f'{RespType.STRING}{message}{BOUNDARY}'.encode()
            
    def encode_bulk_str(self, message) -> bytes:
        return f'{RespType.BULK_STRING}{len(str(message))}{BOUNDARY}{message}{BOUNDARY}'.encode()
    
    def encode_array(self, array, *, encode_type = EncodedMessageType.BULK_STRING):
        to_ret = [str(RespType.ARRAY).encode(), str(len(array)).encode(), BOUNDARY.encode()]

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
        return (NULL_BULK_STR + BOUNDARY).encode()
    

ENCODER = RespEncoder()
# if __name__ == "__main__":
    # print(encoder.encode(['red', 'blue'], EncodedMessageType.ARRAY))