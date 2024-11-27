import re
import threading
import threading
from enum import StrEnum
from socket import socket
from typing import Any

from app.encoder import RespEncoder, EncodedMessageType, ENCODER, QUEUED
from app.storage import RedisDB
from app.constants import SET_ARGS, BOUNDARY
from app.namespace import ConfigNamespace, server_config
from app.util import decode
from app.replicas import Replicas

accum_lock = threading.Condition()

class CommandQueue():
    ''' Used for MULTI cmd to store trx before being commited.'''
    def __init__(self) -> None:
        self.queue = []
        self.in_trx = False

    def add_command(self, cmd_arr: list[Any]):
        self.queue.append(cmd_arr)

    def start_transaction(self):
        self.in_trx = True

    def end_transaction(self):
        self.queue = []
        self.in_trx = False

    def in_transaction(self):
        return self.in_trx

    def get_commands(self):
        return self.queue

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
    MULTI = 'multi'
    EXEC = 'exec'
    DISCARD = 'discard'

class InvalidCommandCall(Exception):
    pass

class CantEncodeMessage(Exception):
    pass

class Command:

    def __init__(self, *, encoder: RespEncoder = None, storage: RedisDB = None, replicas: Replicas | None = None) -> None:
        self.encoder = ENCODER
        self.storage = RedisDB() if storage is None else storage
        self.replicas = replicas
        self.cmd_queue = CommandQueue()
        self.in_wait_cmd = False

    def handle_cmd(self, command_arr: list[bytes], socket: socket, send_to_sock = True):
        if not isinstance(command_arr, list):
            return InvalidCommandCall('Must be a list of cmd bytes')

        if len(command_arr) < 1:
            raise InvalidCommandCall('Must pass a command.')
        cmd = command_arr[0].strip().lower()

        # prevents infinite recursion because we might
        # call this method in one of the methods handling
        # one of the commands in the array
        if cmd in [CommandEnum.EXEC, CommandEnum.MULTI]:
            return

        match cmd.decode('utf-8'):
            case CommandEnum.ECHO:
                return self.handle_echo_cmd(command_arr, socket, send_to_sock)
            case CommandEnum.PING:
                return self.handle_ping_cmd(socket)
            case CommandEnum.SET:
                return self.handle_set_cmd(command_arr, socket, send_to_sock)
            case CommandEnum.GET:
                return self.handle_get_cmd(command_arr, socket, send_to_sock)
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
                return self.handle_incr_cmd(command_arr, socket, send_to_sock)
            case CommandEnum.MULTI:
                return self.handle_multi_cmd(socket)
            case CommandEnum.EXEC:
                return self.handle_exec_cmd(socket)
            case CommandEnum.DISCARD:
                return self.handle_discard_cmd(socket)

    
    def accum_proc(self, cmd_arr):
        '''Accumulates the cmd bytes that have been processed by the server.
        Currently only used when PINGing replica, REPLCONF and SET'''
        encoded = self.encoder.encode(cmd_arr, EncodedMessageType.ARRAY)
        server_config.acked_commands += len(encoded)

    def verify_args_len(self, _type, num, args):
        if len(args) < num:
            raise InvalidCommandCall(f'{_type.upper()} cmd must be called with enough argument(s). Called with only {num} argument(s).')

    def handle_multi_cmd(self, socket: socket):
        self.cmd_queue.start_transaction()
        socket.sendall(self.encoder.encode('OK', EncodedMessageType.SIMPLE_STRING))

    def handle_discard_cmd(self, socket: socket):
        if not self.cmd_queue.in_transaction():
            socket.sendall(self.encoder.encode('ERR DISCARD without MULTI', EncodedMessageType.ERROR))
        else:
            self.cmd_queue.end_transaction()
            socket.sendall(self.encoder.encode('OK', EncodedMessageType.SIMPLE_STRING))

    def handle_exec_cmd(self, socket: socket):
        if not self.cmd_queue.in_transaction():
            socket.sendall(self.encoder.encode('ERR EXEC without MULTI', EncodedMessageType.ERROR))
        else:
            response = []
            queued_cmds = self.cmd_queue.get_commands()
            self.cmd_queue.end_transaction()

            for cmd in queued_cmds:
                ret = self.handle_cmd(cmd, socket, False)
                response.append(ret)

            to_send = self.encoder.encode(response, EncodedMessageType.ARRAY, already_encoded = True)
            socket.sendall(to_send)

    def handle_wait_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.WAIT, 3, cmd_arr)
        no_of_replicas = int(cmd_arr[1])
        wait_ms = int(cmd_arr[2])
        accum_bytes = False # Flag needed below to determi
        
        # We havent sent any commands, so we always return no of replicas
        # regardless of args
        if not server_config.acked_commands:
            processed = len(self.replicas)
        else:
            # some replicas are lagging behind, check if we need to send GETACK
            if self.get_uptodate_replicas() < no_of_replicas:
                cmd_to_send = ['REPLCONF', 'GETACK', '*']
                for replica in self.replicas.get_all_replicas():
                    replica.sendall(self.encoder.encode(cmd_to_send, EncodedMessageType.ARRAY))
                accum_bytes = True
            # We use a condition here to lock this thread until time expires or required number of
            # replicas acknowledge they are up to date
            with accum_lock:
                accum_lock.wait_for(lambda : self.get_uptodate_replicas() >= no_of_replicas, wait_ms / 1000)
                processed = self.get_uptodate_replicas()

            if accum_bytes:
                self.accum_proc(cmd_to_send)
        socket.sendall(self.encoder.encode(str(processed).encode('utf-8'), EncodedMessageType.INTEGER))

    def get_uptodate_replicas(self):
        '''Check if any replicas are lagging behind, if any, we return the number, we do GETACK for all though'''
        return len([x for x in server_config.acked_replicas.values() if x >= server_config.acked_commands])

    def handle_xread_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.XREAD, 4, cmd_arr)
        
        cmd_arr = [decode(x) for x in cmd_arr]

        # In a MULTI trx, queue cmd
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)

        response = self.storage.xread(**self.parse_xread(cmd_arr))
        if response:
            msg = self.encoder.encode(response, EncodedMessageType.ARRAY)
        else:
            msg = self.encoder.encode('', EncodedMessageType.NULL_STR)
        socket.sendall(msg)
        return msg

    def handle_incr_cmd(self, cmd_arr, socket: socket, send_to_sock: bool):
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)

        self.verify_args_len(CommandEnum.INCR, 2, cmd_arr)
        key = cmd_arr[1].decode('utf-8')
        value = self.storage.get(key)
        to_send = None
        try:
            if not value:
                value = 1
            else:
                value = int(value)
                value += 1
            to_send = self.encoder.encode(value, EncodedMessageType.INTEGER)
        except:
            to_send = self.encoder.encode('ERR value is not an integer or out of range', EncodedMessageType.ERROR)
        self.storage.set(key, str(value))
        
        if send_to_sock:
            socket.sendall(to_send)
        return to_send

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
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        
        cmd_arr = [decode(x) for x in cmd_arr]
        response = self.storage.xrange(cmd_arr[1], cmd_arr[2], cmd_arr[3])
        msg = self.encoder.encode(response, EncodedMessageType.ARRAY)
        socket.sendall(msg)
        return msg

    def handle_xadd_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.XADD, 5, cmd_arr)
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        
        cmd_arr = [decode(x) for x in cmd_arr]
        success, response = self.storage.xadd(cmd_arr[1], cmd_arr[2], cmd_arr[3], cmd_arr[4])
        
        if not success:
            msg = self.encoder.encode(response, EncodedMessageType.ERROR)
        else:
            msg = self.encoder.encode(response, EncodedMessageType.BULK_STRING)
        socket.sendall(msg)
        return msg
        
    def handle_psync_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.PSYNC, 2, cmd_arr)
        res = self.encoder.encode('FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0', EncodedMessageType.SIMPLE_STRING)
        full_db = b'$' + str(len(EMPTY_DB)).encode('utf-8') + BOUNDARY + EMPTY_DB
        socket.sendall(res + full_db)

    def handle_replconf_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.REPLCONF, 2, cmd_arr)
        msg = self.encoder.encode('OK', EncodedMessageType.SIMPLE_STRING)

        if cmd_arr[1].lower() == b'listening-port':
            # A replica has connected to master, add to list of replicas,
            # also maintain a map of replica to ack bytes.
            self.replicas.add_replica(socket)
            server_config.acked_replicas[socket.getpeername()] = 0
        if cmd_arr[1].lower() == b'ack':
            acked_bytes = int(cmd_arr[2])
            # acquire lock for thread, we send GETACKS in WAIT cmd, updated with
            # latest replica offset, wakeup sleeping thread
            with accum_lock:
                peername = socket.getpeername()
                server_config.acked_replicas[peername] = acked_bytes
                accum_lock.notify_all()
            msg = None
        if msg:
            socket.sendall(msg)

    def handle_info_cmd(self, cmd_arr, socket: socket):
        return_vals = [
            f'role:{ConfigNamespace.server_type}',
            'master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb',
            'master_repl_offset:0',
        ]
        socket.sendall(self.encoder.encode('\n'.join(return_vals), EncodedMessageType.BULK_STRING))

    def handle_keys_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.KEYS, 2, cmd_arr)
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        key_arg = cmd_arr[1]
        if isinstance(key_arg, bytes):
            key_arg = key_arg.decode('utf-8')
        regex = re.compile(re.escape(key_arg).replace(r'\*', '.*').replace(r'\?', '.'))
        matches = [s for s in self.storage.get_all_keys() if regex.match(s)]
        matches = self.encoder.encode(matches, EncodedMessageType.ARRAY)
        socket.sendall(matches)
        return matches

    def handle_config_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.CONFIG, 3, cmd_arr)
        config_type = cmd_arr[1].lower()

        if config_type == b'get':
            return self.handle_config_get(cmd_arr[2].decode('utf-8'), socket)

    def handle_config_get(self, key, socket: socket):
        value = getattr(ConfigNamespace, key, None)
        res = self.encoder.encode([key, value], EncodedMessageType.ARRAY)
        socket.sendall(res)
        return res

    def handle_get_cmd(self, cmd_arr, socket: socket, send_to_sock: bool):
        self.verify_args_len(CommandEnum.GET, 2, cmd_arr)
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        msg = self.storage.get(cmd_arr[1].decode('utf-8'))
        if msg is None:
            msg = self.encoder.encode('', EncodedMessageType.NULL_STR)
        else:
            msg = self.encoder.encode(msg, EncodedMessageType.BULK_STRING)
        if send_to_sock:
            socket.sendall(msg)
        return msg
        
    def handle_get_type_cmd(self, cmd_arr, socket: socket):
        self.verify_args_len(CommandEnum.GET, 2, cmd_arr)
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        msg = self.storage.get_type(cmd_arr[1].decode('utf-8'))
        if msg is None:
            msg = self.encoder.encode('', EncodedMessageType.NULL_STR)
        else:
            msg = self.encoder.encode(msg, EncodedMessageType.SIMPLE_STRING)
        socket.sendall(msg)
        return msg
    
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

    def handle_set_cmd(self, cmd_arr, socket: socket, send_to_sock):
        self.verify_args_len(CommandEnum.SET, 3, cmd_arr)
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        other_args = self.parse_set_args(cmd_arr)
        resp = self.storage.set(cmd_arr[1], cmd_arr[2], **other_args)
        msg = self.encoder.encode(resp, EncodedMessageType.SIMPLE_STRING)
        self.accum_proc(cmd_arr)
        if send_to_sock:
            socket.sendall(msg)
        return msg

    def handle_echo_cmd(self, cmd_arr, socket: socket, send_to_sock: bool):
        self.verify_args_len(CommandEnum.ECHO, 2, cmd_arr)
        if self.cmd_queue.in_transaction():
            self.cmd_queue.add_command(cmd_arr)
            return socket.sendall(QUEUED)
        encoded_msg = self.encoder.encode(cmd_arr[1], EncodedMessageType.BULK_STRING)
        if encoded_msg is None:
            raise CantEncodeMessage(f'Cant encode message.')
        if send_to_sock:
            socket.sendall(encoded_msg)
        return encoded_msg

    def handle_ping_cmd(self, socket: socket):
        self.accum_proc([b'PING'])

class MasterCommand(Command):

    def __init__(self, *, encoder: RespEncoder = None, storage: RedisDB = None, replicas: Replicas | None = None) -> None:
        super().__init__(encoder=encoder, storage=storage, replicas=replicas)

    def handle_set_cmd(self, cmd_arr, socket: socket, send_to_sock):
        msg = super().handle_set_cmd(cmd_arr, socket, send_to_sock)
        for replica in self.replicas.get_all_replicas():
            replica.sendall(self.encoder.encode(cmd_arr, EncodedMessageType.ARRAY))
        return msg
    
    def handle_ping_cmd(self, socket: socket):
        # Initially, replica sends PING cmd to master during handshake phase,
        # we dont need to add to processed bytes for master
        if server_config.finished_handshake:
            super().handle_ping_cmd(socket)
        socket.sendall(self.encoder.encode('PONG', EncodedMessageType.SIMPLE_STRING))

class ReplicaCommand(Command):

    def __init__(self, *, encoder: RespEncoder = None, storage: RedisDB = None) -> None:
        super().__init__(encoder=encoder, storage=storage)

    def handle_replconf_cmd(self, cmd_arr, socket: socket):
        proc_bytes = server_config.acked_commands
        self.accum_proc(cmd_arr)
        if cmd_arr[1].lower() == b'getack':
            msg = self.encoder.encode([CommandEnum.REPLCONF, 'ACK', proc_bytes], EncodedMessageType.ARRAY)
            socket.sendall(msg)

    def handle_set_cmd(self, cmd_arr, socket: socket, send_to_sock):
        # SET handling cmd in replica should not send reply to master
        return super().handle_set_cmd(cmd_arr, socket, False)

EMPTY_DB = bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
