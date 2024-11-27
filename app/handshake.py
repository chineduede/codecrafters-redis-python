from enum import IntEnum
from app.commands import CommandEnum
from app.encoder import ENCODER, EncodedMessageType
from app.namespace import ConfigNamespace, ServerConfig, server_config

class HandShakeStates(IntEnum):
    INIT = 0
    AWAIT_PONG = 1
    SEND_REPLCONF = 2
    SEND_PSYNC = 3
    FULLRESYNC = 4
    END = 5

class Handshake:
    state = HandShakeStates.INIT
    def __init__(self) -> None:
        pass

    @classmethod
    def handle_stage(cls, data=None | bytes, server_config: ServerConfig | None = None):
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
            cls.state = HandShakeStates.FULLRESYNC
            return ENCODER.encode([CommandEnum.PSYNC, '?', '-1'], EncodedMessageType.ARRAY)
        if cls.state == HandShakeStates.FULLRESYNC:
            # consume and do nothing
            if server_config:
                server_config.finished_handshake = True
            cls.state = HandShakeStates.END
            return HandShakeStates.END
