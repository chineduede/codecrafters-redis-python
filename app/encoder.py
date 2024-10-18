from enum import IntEnum
from app.constants import BOUNDARY, RespType

class EncodedMessageType(IntEnum):
    SIMPLE_STRING = 0
    BULK_STRING = 1

class RespEncoder:

    def encode(self, message: str, _type: EncodedMessageType) -> bytes | None:
        match _type:
            case EncodedMessageType.SIMPLE_STRING:
                return self.encode_smpl_str(message)
            case EncodedMessageType.BULK_STRING:
                return self.encode_bulk_str(message)
            case _:
                return None
            
    def encode_smpl_str(self, message) -> bytes:
        return f'{RespType.STRING}{message}{BOUNDARY}'.encode()
            
    def encode_bulk_str(self, message) -> bytes:
        return f'{RespType.BULK_STRING}{len(message)}{BOUNDARY}{message}{BOUNDARY}'.encode()