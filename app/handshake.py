from enum import IntEnum
from app.commands import CommandEnum
from app.encoder import ENCODER, EncodedMessageType
from app.namespace import ConfigNamespace

class HandShakeStates(IntEnum):
    INIT = 0
    AWAIT_PONG = 1
    SEND_REPLCONF_1 = 2
    END = 3

class Handshake:
    state = HandShakeStates.INIT
    def __init__(self) -> None:
        pass

    @classmethod
    def handle_stage(cls, data=None):
        if cls.state == HandShakeStates.INIT:
            # initial state, send PING
            cls.state = HandShakeStates.AWAIT_PONG
            return ENCODER.encode([CommandEnum.PING.upper()], EncodedMessageType.ARRAY)
        if cls.state == HandShakeStates.AWAIT_PONG and data is not None:
            if data != 'PONG':
                return
            cls.state = HandShakeStates.SEND_REPLCONF_1
            return ENCODER.encode([CommandEnum.REPLCONF, 'listening-port', ConfigNamespace.port], EncodedMessageType.ARRAY)
        if cls.state == HandShakeStates.SEND_REPLCONF_1 and data is not None:
            if data != 'OK':
                return
            cls.state = HandShakeStates.END
            return ENCODER.encode([CommandEnum.REPLCONF, 'capa', 'psync2'], EncodedMessageType.ARRAY)
