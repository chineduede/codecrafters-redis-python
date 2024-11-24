import re
from enum import StrEnum
from socket import socket

from app.encoder import RespEncoder, EncodedMessageType, ENCODER
from app.storage import RedisDB
from app.constants import SET_ARGS, BOUNDARY
from app.namespace import ConfigNamespace
from app.util import decode

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
    WAIT = 'wait'
    TYPE = 'type'
    XADD = 'xadd'
    XRANGE = 'xrange'
    XREAD = 'xread'
    INCR = 'incr'

class InvalidCommandCall(Exception):
    pass

class CantEncodeMessage(Exception):
    pass

class Command:

    def __init__(self, *, encoder: RespEncoder = None, storage: RedisDB = None) -> None:
        self.encoder = ENCODER
        self.storage = RedisDB() if storage is None else storage
        self.replicas: list[socket] = []
        self.processed_offset = 0

    def handle_cmd(self, command_arr: list[bytes] | bytes, socket: socket):
        if not isinstance(command_arr, list):
            return {}

        if len(command_arr) < 1:
            raise InvalidCommandCall(f'Must pass a command.')
        cmd = command_arr[0].strip().lower()

        match cmd.decode('utf-8'):
            case CommandEnum.ECHO:
                return self.handle_echo_cmd(command_arr, socket)
            case CommandEnum.PING:
                return self.handle_ping_cmd(socket)
            case CommandEnum.SET:
                return self.handle_set_cmd(command_arr, socket)
            case CommandEnum.GET:
                return self.handle_get_cmd(command_arr, socket)
            case CommandEnum.CONFIG:
                return self.handle_config_cmd(command_arr, socket)
            case CommandEnum.KEYS:
                return self.handle_keys_cmd(command_arr, socket)
            case CommandEnum.INFO:
                return self.handle_info_cmd(command_arr, socket)
            case CommandEnum.REPLCONF:
                return self.handle_replconf_cmd(command_arr, socket)
            case CommandEnum.PSYNC:
                return self.handle_psync_cmd(command_arr, socket)
            case CommandEnum.WAIT:
                return self.handle_wait_cmd(command_arr, socket)
            case CommandEnum.TYPE:
                return self.handle_get_type_cmd(command_arr, socket)
            case CommandEnum.XADD:
                return self.handle_xadd_cmd(command_arr, socket)
            case CommandEnum.XRANGE:
                return self.handle_xrange_cmd(command_arr, socket)
            case CommandEnum.XREAD:
                return self.handle_xread_cmd(command_arr, socket)
            case CommandEnum.INCR:
                return self.handle_incr_cmd(command_arr, socket)
    
    def accum_proc(self, cmd_arr):
        encoded = self.encoder.encode(cmd_arr, EncodedMessageType.ARRAY)
        self.processed_offset += len(encoded)

    def verify_args_len(self, _type, num, args):
        if len(args) < num:
            raise InvalidCommandCall(f'{_type.upper()} cmd must be called with enough argument(s). Called with only {num} argument(s).')

    def handle_wait_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.WAIT, 3, cmd_arr)

        no_of_replicas = int(cmd_arr[1])
        wait_ms = int(cmd_arr[2])

        cn_reps = str(len(self.replicas))
        socket.sendall(self.encoder.encode(cn_reps.encode('utf-8'), EncodedMessageType.INTEGER))

    def handle_xread_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.XREAD, 4, cmd_arr)
        
        cmd_arr = [decode(x) for x in cmd_arr]

        response = self.storage.xread(**self.parse_xread(cmd_arr))
        if response:
            msg = self.encoder.encode(response, EncodedMessageType.ARRAY)
        else:
            msg = self.encoder.encode('', EncodedMessageType.NULL_STR)
        socket.sendall(msg)

    def handle_incr_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.INCR, 2, cmd_arr)
        key = cmd_arr[1].decode('utf-8')
        value = self.storage.get(key)

        if not value:
            value = 1
        else:
            try:
                value = int(value)
            except:
                pass

            if isinstance(value, int):
                value = int(value)
                value += 1
        self.storage.set(key, str(value))

        socket.sendall(self.encoder.encode(value, EncodedMessageType.INTEGER))

    def parse_xread(self, cmd_arr: list[str]):
        streams_idx = cmd_arr.index('streams')

        str_keys = cmd_arr[streams_idx+1:]
        s_k_len = len(str_keys)
        mid = s_k_len // 2
        streams = str_keys[:mid]
        keys = str_keys[mid:]
        block_idx, block = None, None
        
        try:
            block_idx = cmd_arr.index('block')
        except ValueError:
            pass
        
        if block_idx:
            block = int(cmd_arr[block_idx+1])

        return {
            "streams": streams,
            "keys": keys,
            "block": block
        }

    def handle_xrange_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.XRANGE, 4, cmd_arr)
        
        cmd_arr = [decode(x) for x in cmd_arr]
        response = self.storage.xrange(cmd_arr[1], cmd_arr[2], cmd_arr[3])
        msg = self.encoder.encode(response, EncodedMessageType.ARRAY)
        socket.sendall(msg)

    def handle_xadd_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.XADD, 5, cmd_arr)
        
        cmd_arr = [decode(x) for x in cmd_arr]
        success, response = self.storage.xadd(cmd_arr[1], cmd_arr[2], cmd_arr[3], cmd_arr[4])
        
        if not success:
            msg = self.encoder.encode(response, EncodedMessageType.ERROR)
        else:
            msg = self.encoder.encode(response, EncodedMessageType.BULK_STRING)
        socket.sendall(msg)
        
    def handle_psync_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.PSYNC, 2, cmd_arr)
        res = self.encoder.encode('FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0', EncodedMessageType.SIMPLE_STRING)
        full_db = b'$' + str(len(EMPTY_DB)).encode('utf-8') + BOUNDARY + EMPTY_DB
        socket.sendall(res + full_db)

    def handle_replconf_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.REPLCONF, 2, cmd_arr)
        msg = self.encoder.encode('OK', EncodedMessageType.SIMPLE_STRING)
        if cmd_arr[1].lower() == b'listening-port':
            self.replicas.append(socket)
        if cmd_arr[1].lower() == b'getack':
            msg = self.encoder.encode([CommandEnum.REPLCONF, 'ACK', self.processed_offset], EncodedMessageType.ARRAY)
        if msg:
            socket.sendall(msg)
        self.accum_proc(cmd_arr)

    def handle_info_cmd(self, cmd_arr, socket: socket):
        return_vals = [
            f'role:{ConfigNamespace.server_type}',
            'master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
            'master_repl_offset:0',
        ]
        socket.sendall(self.encoder.encode('\n'.join(return_vals), EncodedMessageType.BULK_STRING))

    def handle_keys_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.KEYS, 2, cmd_arr)
        key_arg = cmd_arr[1]
        if isinstance(key_arg, bytes):
            key_arg = key_arg.decode('utf-8')
        regex = re.compile(re.escape(key_arg).replace(r'\*', '.*').replace(r'\?', '.'))
        matches = [s for s in self.storage.get_all_keys() if regex.match(s)]
        socket.sendall(self.encoder.encode(matches, EncodedMessageType.ARRAY))

    def handle_config_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.CONFIG, 3, cmd_arr)
        config_type = cmd_arr[1].lower()

        if config_type == b'get':
            return self.handle_config_get(cmd_arr[2].decode('utf-8'), socket)

    def handle_config_get(self, key, socket: socket):
        value = getattr(ConfigNamespace, key, None)
        socket.sendall(self.encoder.encode([key, value], EncodedMessageType.ARRAY))

    def handle_get_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.GET, 2, cmd_arr)
        msg = self.storage.get(cmd_arr[1].decode('utf-8'))
        if msg is None:
            msg = self.encoder.encode('', EncodedMessageType.NULL_STR)
        else:
            msg = self.encoder.encode(msg, EncodedMessageType.BULK_STRING)
        socket.sendall(msg)
        
    def handle_get_type_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.GET, 2, cmd_arr)
        msg = self.storage.get_type(cmd_arr[1].decode('utf-8'))
        if msg is None:
            msg = self.encoder.encode('', EncodedMessageType.NULL_STR)
        else:
            msg = self.encoder.encode(msg, EncodedMessageType.SIMPLE_STRING)
        socket.sendall(msg)
    
    def parse_set_args(self, cmd_arr):
        # list of args for SET cmd, we return a dict
        args_dict = {}
        cmd_arr = [b.decode('utf-8') for b in cmd_arr]
        other_args: list = [x.lower() if isinstance(x, str) else x for x in cmd_arr[3:]]
        for cmd in SET_ARGS:
            if cmd in other_args:
                idx = other_args.index(cmd)
                args_dict[cmd] = other_args[idx+1]
        return args_dict

    
    def handle_set_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.SET, 3, cmd_arr)
        other_args = self.parse_set_args(cmd_arr)
        resp = self.storage.set(cmd_arr[1], cmd_arr[2], **other_args)
        msg = self.encoder.encode(resp, EncodedMessageType.SIMPLE_STRING)

        if hasattr(ConfigNamespace, 'master_conn') and ConfigNamespace.master_conn is socket:
            self.accum_proc(cmd_arr)
        else:
            socket.sendall(msg)

            if not ConfigNamespace.is_replica():
                for replica in self.replicas:
                    replica.sendall(self.encoder.encode(cmd_arr, EncodedMessageType.ARRAY))


    def handle_echo_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.ECHO, 2, cmd_arr)
        encoded_msg = self.encoder.encode(cmd_arr[1], EncodedMessageType.BULK_STRING)
        if encoded_msg is None:
            raise CantEncodeMessage(f'Cant encode message.')
        socket.sendall(encoded_msg)
        return encoded_msg

    def handle_ping_cmd(self, socket: socket):
        if not ConfigNamespace.is_replica():
            socket.sendall(self.encoder.encode('PONG', EncodedMessageType.SIMPLE_STRING))
        self.accum_proc([b'PING'])
        


EMPTY_DB = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
