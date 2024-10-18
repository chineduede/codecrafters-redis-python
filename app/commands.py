from enum import StrEnum

from app.encoder import RespEncoder, EncodedMessageType

class CommandEnum(StrEnum):
    ECHO = 'echo'
    PING = 'ping'

class InvalidCommandCall(Exception):
    pass

class CantEncodeMessage(Exception):
    pass

class Command:

    def __init__(self, encoder: RespEncoder) -> None:
        self.encoder = encoder

    def handle_cmd(self, *args):
        if len(args) < 1:
            raise InvalidCommandCall(f'Must pass a command.')
        cmd = args[0]
        if isinstance(cmd, str):
            cmd = cmd.lower()

        match cmd:
            case CommandEnum.ECHO:
                return self.handle_echo_cmd(*args)
            case CommandEnum.PING:
                return self.handle_ping_cmd()

    def handle_echo_cmd(self, *args):
        if len(args) < 2:
            raise InvalidCommandCall(f'Echo cmd must be called with argument.')
        encoded_msg = self.encoder.encode(args[1], EncodedMessageType.BULK_STRING)
        if encoded_msg is None:
            raise CantEncodeMessage(f'Cant encode message.')
        return encoded_msg

    def handle_ping_cmd(self):
        return self.encoder.encode('PONG', EncodedMessageType.SIMPLE_STRING)
