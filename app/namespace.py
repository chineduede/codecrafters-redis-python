class ConfigNamespace:
    server_type = 'master'
    replica_just_conn = None

    @staticmethod
    def set_server_type():
        if hasattr(ConfigNamespace, 'replicaof') and getattr(ConfigNamespace, 'replicaof', None):
            ConfigNamespace.server_type = 'slave'

    @staticmethod
    def is_replica():
        return ConfigNamespace.server_type == 'slave'

class ServerConfig:
    acked_replicas = {}
    acked_commands = 0
    finished_handshake = False

server_config = ServerConfig()
