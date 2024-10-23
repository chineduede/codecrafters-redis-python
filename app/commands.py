import re
from enum import StrEnum

from app.encoder import RespEncoder, EncodedMessageType, ENCODER
from app.storage import RedisDB
from app.constants import SET_ARGS, BOUNDARY
from app.namespace import ConfigNamespace

class CommandEnum(StrEnum):
    ECHO = 'echo'
    PING = 'ping'
    SET = 'set'
    GET = 'get'
    CONFIG = 'config'
    KEYS = 'keys'
    INFO = 'info'
    REPLCONF = 'replconf'
    PSYNC = 'psync'

class InvalidCommandCall(Exception):
    pass

class CantEncodeMessage(Exception):
    pass

class Command:

    def __init__(self, *, encoder: RespEncoder = None, storage: RedisDB = None) -> None:
        self.encoder = ENCODER
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
            case CommandEnum.CONFIG:
                return self.handle_config_cmd(*args)
            case CommandEnum.KEYS:
                return self.handle_keys(*args)
            case CommandEnum.INFO:
                return self.handle_info_cmd(*args)
            case CommandEnum.REPLCONF:
                return self.handle_replconf(*args)
            case CommandEnum.PSYNC:
                return self.handle_psync_cmd(*args)

            
    def verify_args_len(self, _type, num, args):
        if len(args) < num:
            raise InvalidCommandCall(f'{_type.upper()} cmd must be called with enough argument(s). Called with only {num} argument(s).')

    def handle_psync_cmd(self, *args):
        self.verify_args_len(CommandEnum.PSYNC, 2, args)
        res = self.encoder.encode('FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0', EncodedMessageType.SIMPLE_STRING)
        full_db = b'$' + str(len(EMPTY_DB)).encode('utf-8') + BOUNDARY + EMPTY_DB
        return res + full_db

    def handle_replconf(self, *args):
        self.verify_args_len(CommandEnum.REPLCONF, 2, args)
        return self.encoder.encode('OK', EncodedMessageType.SIMPLE_STRING)

    def handle_info_cmd(self, *args):
        return_vals = [
            f'role:{ConfigNamespace.server_type}',
            'master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
            'master_repl_offset:0',
        ]
        return self.encoder.encode('\n'.join(return_vals), EncodedMessageType.BULK_STRING)

    def handle_keys(self, *args):
        self.verify_args_len(CommandEnum.KEYS, 2, args)
        regex = re.compile(re.escape(args[1]).replace(r'\*', '.*').replace(r'\?', '.'))
        matches = [s for s in self.storage.get_all_keys() if regex.match(s)]
        return self.encoder.encode(matches, EncodedMessageType.ARRAY)

    def handle_config_cmd(self, *args):
        self.verify_args_len(CommandEnum.CONFIG, 3, args)
        config_type = args[1]
        if not isinstance(config_type, str):
            raise InvalidCommandCall(f'Wrong type of Config, {type(config_type)}')
        else:
            config_type = config_type.lower()

        if config_type == 'get':
            return self.handle_config_get(args[2])

    def handle_config_get(self, key):
        value = getattr(ConfigNamespace, key, None)
        return self.encoder.encode([key, value], EncodedMessageType.ARRAY)

    def handle_get_cmd(self, *args):
        self.verify_args_len(CommandEnum.GET, 2, args)
        msg = self.storage.get(args[1])
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


EMPTY_DB = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
