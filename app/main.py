import socket  # noqa: F401
import selectors
import argparse

from app.resp_parser import RespParser
from app.commands import Command
from app.namespace import ConfigNamespace
from app.handshake import Handshake
from app.util import decode

parser = argparse.ArgumentParser('Redis')
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
parser.add_argument ("-p", "--port", default=6379, type=int)
parser.add_argument("--replicaof")

sel = selectors.DefaultSelector()
replicas: set[socket.socket] = set()  # a list of replica sockets that are connected

def propagate(message: bytes):
    for sock in replicas:
        print(sock.getpeername())
        sock.sendall(b''.join(message))

def read(sock: socket.socket, mask, cmd_parser: Command):
    parser = RespParser()
    msg_to_propagate = []
    parsed_msg = parser.parse_all(sock, sel, msg_to_propagate)
    if parsed_msg is None:
        return

    propagate_to_replicas, to_send = cmd_parser.handle_cmd(*decode(parsed_msg), sock=sock, replicas=replicas)
    if to_send:
        sock.sendall(to_send)

    if propagate_to_replicas and not ConfigNamespace.is_replica():
        propagate(msg_to_propagate)

def accept(sock: socket.socket, mask, cmd_parser):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def handle_replica_to_master():
    pass


def handle_master_data(sock: socket.socket, mask, cmd_parser):
    parser = RespParser()
    # print(parsed_msg)
    parsed_msg = parser.parse_all(sock, sel)
    if parsed_msg is None:
        return
    res = Handshake.handle_stage(parsed_msg)
    if res:
        sock.sendall(res)

def connect_replica():
    host, port = ConfigNamespace.replicaof.split()
    
    conn = socket.create_connection((host, int(port)))
    # send PING cmd
    conn.setblocking(False)
    ping_encoded = Handshake.handle_stage()
    if ping_encoded:
        conn.sendall(ping_encoded)

    sel.register(conn, selectors.EVENT_READ, handle_master_data)

def main(cmd_parser: Command):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    cmd_parser.storage.load_db()

    with socket.create_server(("localhost", ConfigNamespace.port), reuse_port=True) as server:
        server.listen(100)
        server.setblocking(False)
        sel.register(server, selectors.EVENT_READ, accept)

        if ConfigNamespace.is_replica():
            connect_replica()

        while True:
            events = sel.select()
            # print(events)
            for key, mask in events:
                cb = key.data
                cb(key.fileobj, mask, cmd_parser)

if __name__ == "__main__":
    parser.parse_known_args(namespace=ConfigNamespace)[0]
    ConfigNamespace.set_server_type()
    cmd = Command()
    main(cmd)
