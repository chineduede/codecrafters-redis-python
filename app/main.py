import socket  # noqa: F401
import selectors
import argparse

from app.resp_parser import RespParser
from app.commands import Command, CommandEnum
from app.namespace import ConfigNamespace
from app.encoder import RespEncoder, EncodedMessageType

parser = argparse.ArgumentParser('Redis')
parser.add_argument("--dir")
parser.add_argument("--dbfilename")
parser.add_argument ("-p", "--port", default=6379, type=int)
parser.add_argument("--replicaof")

sel = selectors.DefaultSelector()
TEM = b'\r\n'

def read(sock: socket.socket, mask, cmd_parser: Command):
    parser = RespParser()
    recvd = sock.recv(1024).decode()

    if not recvd:
        sel.unregister(sock)
        sock.close()
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

def handle_master_data(sock: socket.socket, mask, cmd_parser):
    recvd = sock.recv(1024).decode()

    if not recvd:
        # Handle the case when the master server closes the connection
        print("Lost connection to master server")
        sel.unregister(sock)
        sock.close()
        return

    # Here you can add logic to process the data received from the master
    # e.g., syncing database updates from the master
    print(f"Received from master: {recvd}")

def connect_replica():
    host, port = ConfigNamespace.replicaof.split()
    encoder = RespEncoder()
    
    conn = socket.create_connection((host, int(port)))
        # send PING cmd
    ping_encoded = encoder.encode([CommandEnum.PING.upper()], EncodedMessageType.ARRAY)
    if ping_encoded:
        conn.sendall(ping_encoded)

    conn.setblocking(False)
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
            
            for key, mask in events:
                cb = key.data
                cb(key.fileobj, mask, cmd_parser)

if __name__ == "__main__":
    parser.parse_known_args(namespace=ConfigNamespace)[0]
    ConfigNamespace.set_server_type()
    cmd = Command()
    main(cmd)
