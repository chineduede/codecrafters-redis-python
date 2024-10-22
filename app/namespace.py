class ConfigNamespace:
    server_name = 'master'

    @staticmethod
    def set_server_type():
        if hasattr(ConfigNamespace, 'replicaof') and getattr(ConfigNamespace, 'replicaof', None):
            ConfigNamespace.server_name = 'slave'
