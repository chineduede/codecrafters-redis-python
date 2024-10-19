from enum import IntEnum
from app.constants import BOUNDARY, RespType, NULL_BULK_STR

class EncodedMessageType(IntEnum):
    SIMPLE_STRING = 0
    BULK_STRING = 1
    NULL_STR = 2

class RespEncoder:

    def encode(self, message: str, _type: EncodedMessageType) -> bytes | None:
        match _type:
            case EncodedMessageType.SIMPLE_STRING:
                return self.encode_smpl_str(message)
            case EncodedMessageType.BULK_STRING:
                return self.encode_bulk_str(message)
            case EncodedMessageType.NULL_STR:
                return self.null_bulk_str()
            case _:
                return None
            
    def encode_smpl_str(self, message) -> bytes:
        return f'{RespType.STRING}{message}{BOUNDARY}'.encode()
            
    def encode_bulk_str(self, message) -> bytes:
        return f'{RespType.BULK_STRING}{len(message)}{BOUNDARY}{message}{BOUNDARY}'.encode()
    
    @staticmethod
    def null_bulk_str():
        return (NULL_BULK_STR + BOUNDARY).encode()
    

if __name__ == "__main__":
    encoder = RespEncoder()
    print(encoder.null_bulk_str())