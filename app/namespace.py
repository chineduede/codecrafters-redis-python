class ConfigNamespace:
    server_type = 'master'

    @staticmethod
    def set_server_type():
        if hasattr(ConfigNamespace, 'replicaof') and getattr(ConfigNamespace, 'replicaof', None):
            ConfigNamespace.server_type = 'slave'

    @staticmethod
    def is_replica():
        return ConfigNamespace.server_type == 'slave'
