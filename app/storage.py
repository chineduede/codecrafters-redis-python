from datetime import datetime, timedelta

class RedisDB:

    def __init__(self) -> None:
        self.store = {}

    def get(self, key):
        obj = self.store.get(key)
        expired = 'expires' in obj and obj['expires'] < datetime.now()

        if not expired:
            return obj['value']
        return None
    
    def set(self, key, value, **kwargs):
        obj = {
            'value': value
        }

        if 'px' in kwargs:
            expires_in = datetime.now() + timedelta(milliseconds=int(kwargs['px']))
            obj['expires'] = expires_in

        self.store[key] = obj
        return 'OK'