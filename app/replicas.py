import socket

class Replicas:

    def __init__(self) -> None:
        self.replicas: list[socket.socket] = []

    def add_replica(self, sock: socket.socket):
        self.replicas.append(sock)

    def get_all_replicas(self):
        for replica in self.replicas:
            yield replica

