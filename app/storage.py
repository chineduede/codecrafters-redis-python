class RedisDB:

    def __init__(self) -> None:
        self.store = {}

    def get(self, key):
        return self.store.get(key)
    
    def set(self, key, value):
        self.store[key] = value
        return 'OK'