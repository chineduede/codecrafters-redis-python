from enum import StrEnum

from app.encoder import RespEncoder, EncodedMessageType
from app.storage import RedisDB
from app.constants import SET_ARGS

class CommandEnum(StrEnum):
    ECHO = 'echo'
    PING = 'ping'
    SET = 'set'
    GET = 'get'

class InvalidCommandCall(Exception):
    pass

class CantEncodeMessage(Exception):
    pass

class Command:

    def __init__(self, *, encoder: RespEncoder = None, storage: RedisDB = None) -> None:
        self.encoder = RespEncoder() if encoder is None else encoder
        self.storage = RedisDB() if storage is None else storage

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
            case CommandEnum.SET:
                return self.handle_set_cmd(*args)
            case CommandEnum.GET:
                return self.handle_get_cmd(*args)
            
    def verify_args_len(self, _type, num, args):
        if len(args) < num:
            raise InvalidCommandCall(f'{_type.upper()} cmd must be called with enough argument(s). Called with only {num} argument(s).')

    def handle_get_cmd(self, *args):
        self.verify_args_len(CommandEnum.GET, 2, args)
        msg = self.storage.get(args[1])
        # print('**msg**', msg)
        if msg is None:
            return self.encoder.encode('', EncodedMessageType.NULL_STR)
        return self.encoder.encode(msg, EncodedMessageType.BULK_STRING)
    
    def parse_set_args(self, args):
        # list of args for SET cmd, we return a dict
        args_dict = {}
        other_args: list = [x.lower() if isinstance(x, str) else x for x in args[3:]]
        for cmd in SET_ARGS:
            if cmd in other_args:
                idx = other_args.index(cmd)
                args_dict[cmd] = other_args[idx+1]
        return args_dict

    
    def handle_set_cmd(self, *args):
        self.verify_args_len(CommandEnum.SET, 3, args)
        other_args = self.parse_set_args(args)
        resp = self.storage.set(args[1], args[2], **other_args)
        return self.encoder.encode(resp, EncodedMessageType.SIMPLE_STRING)


    def handle_echo_cmd(self, *args):
        self.verify_args_len(CommandEnum.ECHO, 2, args)
        encoded_msg = self.encoder.encode(args[1], EncodedMessageType.BULK_STRING)
        if encoded_msg is None:
            raise CantEncodeMessage(f'Cant encode message.')
        return encoded_msg

    def handle_ping_cmd(self):
        return self.encoder.encode('PONG', EncodedMessageType.SIMPLE_STRING)
