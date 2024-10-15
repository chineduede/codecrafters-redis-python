import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    response = b'+PONG\r\n'
    connections: list[socket.socket] = []

    # Uncomment this to pass the first stage
    #
    with socket.create_server(("localhost", 6379), reuse_port=True) as server:
        # while True:
        conn, _ = server.accept() # wait for client
        connections.append(conn)

        while conn.recv(1024):
            conn.sendall(response)

if __name__ == "__main__":
    main()
