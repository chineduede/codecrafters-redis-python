import socket  # noqa: F401
import selectors
import argparse

from app.resp_parser import RespParser
from app.commands import Command
from app.namespace import ConfigNamespace

parser = argparse.ArgumentParser('Redis')
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
parser.add_argument ("-p", "--port", default=6379, type=int)

sel = selectors.DefaultSelector()
TEM = b'\r\n'

def read(sock: socket.socket, mask, cmd_parser: Command):
    parser = RespParser()
    recvd = sock.recv(1024).decode()

    if not recvd:
        return

    parser.set_type(recvd[0])
    parsed_msg = parser.parse(recvd)
    if parsed_msg is None:
        while recvd := sock.recv(1024).decode():
            parsed_msg = parser.parse(recvd)
            if parsed_msg is not None:
                break

    # print('parsed_msg', parsed_msg)
    to_send = cmd_parser.handle_cmd(*parsed_msg)
    sock.sendall(to_send)

def accept(sock: socket.socket, mask, cmd_parser):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def main(cmd_parser: Command):
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    cmd_parser.storage.load_db()
    with socket.create_server(("localhost", ConfigNamespace.port), reuse_port=True) as server:
        server.listen(100)
        server.setblocking(False)
        sel.register(server, selectors.EVENT_READ, accept)

        while True:
            events = sel.select()
            
            for key, mask in events:
                cb = key.data
                cb(key.fileobj, mask, cmd_parser)

if __name__ == "__main__":
    parser.parse_known_args(namespace=ConfigNamespace)[0]
    cmd = Command()
    main(cmd)
