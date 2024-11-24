import socket  # noqa: F401
import selectors
import argparse
import threading

from app.resp_parser import RespParser
from app.commands import Command
from app.namespace import ConfigNamespace
from app.handshake import Handshake, HandShakeStates
from app.storage import RedisDB
from app.replicas import Replicas

parser = argparse.ArgumentParser('Redis')
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
parser.add_argument ("-p", "--port", default=6379, type=int)
parser.add_argument("--replicaof")

sel = selectors.DefaultSelector()
storage = RedisDB()
replicas = Replicas()

def handle_client(sock: socket.socket, cmd_parser: Command):
    parser = RespParser()
    msg_to_propagate = []
    parsed_msg = parser.parse_all(sock, sel, msg_to_propagate)
    # print('main read', parsed_msg)
    if parsed_msg is None:
        return

    for cmd in parsed_msg:
        cmd_parser.handle_cmd(cmd, sock)

def read(cmd_parser: Command):
    def inner(sock: socket.socket):
        thread = threading.Thread(target=handle_client, args=(sock, cmd_parser))
        thread.start()
    return inner


def accept(sock: socket.socket):
    conn, _ = sock.accept()
    conn.setblocking(False)
    cmd_parser = Command(storage=storage, replicas=replicas)
    sel.register(conn, selectors.EVENT_READ, read(cmd_parser))


def handle_master_data(sock: socket.socket):
    parser = RespParser()
    cmd_parser = Command(storage=storage)
    parsed_msg = parser.parse_all(sock, sel)

    if not parsed_msg:
        return
    res = Handshake.handle_stage(parsed_msg[0])
    if res and res != HandShakeStates.END:
        sock.sendall(res)
    else:
        for cmd in parsed_msg:
            cmd_parser.handle_cmd(cmd, sock)


def connect_replica():
    host, port = ConfigNamespace.replicaof.split()
    
    conn = socket.create_connection((host, int(port)))
    ConfigNamespace.master_conn = conn
    # send PING cmd
    conn.setblocking(False)
    ping_encoded = Handshake.handle_stage()
    if ping_encoded:
        conn.sendall(ping_encoded)

    sel.register(conn, selectors.EVENT_READ, handle_master_data)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    storage.load_db()

    with socket.create_server(("localhost", ConfigNamespace.port), reuse_port=True) as server:
        server.listen(100)
        server.setblocking(False)
        sel.register(server, selectors.EVENT_READ, accept)

        if ConfigNamespace.is_replica():
            connect_replica()

        while True:
            events = sel.select()
            # print(events)
            for key, _ in events:
                cb = key.data
                cb(key.fileobj)

if __name__ == "__main__":
    parser.parse_known_args(namespace=ConfigNamespace)[0]
    ConfigNamespace.set_server_type()
    main()
