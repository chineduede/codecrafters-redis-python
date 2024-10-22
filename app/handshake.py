from enum import IntEnum
from app.commands import CommandEnum
from app.encoder import ENCODER, EncodedMessageType
from app.namespace import ConfigNamespace

class HandShakeStates(IntEnum):
    INIT = 0
    AWAIT_PONG = 1
    SEND_REPLCONF = 2
    SEND_PSYNC = 3
    END = 4

class Handshake:
    state = HandShakeStates.INIT
    def __init__(self) -> None:
        pass

    @classmethod
    def handle_stage(cls, data=None | bytes):
        if cls.state == HandShakeStates.INIT:
            # initial state, send PING
            cls.state = HandShakeStates.AWAIT_PONG
            return ENCODER.encode([CommandEnum.PING], EncodedMessageType.ARRAY)
        if cls.state == HandShakeStates.AWAIT_PONG and data is not None:
            if data != b'PONG':
                return
            cls.state = HandShakeStates.SEND_REPLCONF
            return ENCODER.encode([CommandEnum.REPLCONF, 'listening-port', ConfigNamespace.port], EncodedMessageType.ARRAY)
        if cls.state == HandShakeStates.SEND_REPLCONF and data is not None:
            if data != b'OK':
                return
            cls.state = HandShakeStates.SEND_PSYNC
            return ENCODER.encode([CommandEnum.REPLCONF, 'capa', 'psync2'], EncodedMessageType.ARRAY)
        if cls.state == HandShakeStates.SEND_PSYNC and data is not None:
            if data != b'OK':
                return
            cls.state = HandShakeStates.END
            return ENCODER.encode([CommandEnum.PSYNC, '?', '-1'], EncodedMessageType.ARRAY)
