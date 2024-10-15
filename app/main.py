import socket  # noqa: F401
import select
import selectors

sel = selectors.DefaultSelector()
response = b'+PONG\r\n'

def read(sock: socket.socket, mask):
    if sock.recv(1024):
        sock.sendall(response)

def accept(sock: socket.socket, mask):
    conn, _ = sock.accept()
    conn.setblocking(False)
    sel.register(conn, selectors.EVENT_READ, read)

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    with socket.create_server(("localhost", 6379), reuse_port=True) as server:
        server.listen(100)
        server.setblocking(False)
        sel.register(server, selectors.EVENT_READ, accept)

        while True:
            events = sel.select()
            
            for key, mask in events:
                cb = key.data
                cb(key.fileobj, mask)

if __name__ == "__main__":
    main()
