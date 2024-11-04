import pathlib

from datetime import datetime, timedelta
from app.namespace import ConfigNamespace
from app.rdb_parser import RDBParser

class RedisStream:
    SEP = '-'

    def __init__(self, name: str) -> None:
        self.id = name
        self.items = []
        
    def append(self, **kwargs):
        obj = {
            'key': kwargs['key'],
            'value': kwargs['value']
        }
        self.items.append({'id': kwargs['id'], 'item': obj})

class RedisDB:

    def __init__(self) -> None:
        self.store = {}

    def load_db(self):
        if ConfigNamespace.dir is None or ConfigNamespace.dbfilename is None:
            return
        rdb_path = pathlib.Path(ConfigNamespace.dir + '/' + ConfigNamespace.dbfilename)
        rdb_parser = RDBParser(rdb_path)
        if rdb_path.exists():
            rdb_parser.read_file()
            self.store = rdb_parser.get_database()
        else:
            rdb_parser.create_rdb_file()

    def get(self, key):
        obj = self.store.get(key)
        if not obj:
            return None
        expired = 'expires' in obj and obj['expires'] < datetime.now().timestamp()

        if not expired:
            return obj['value']
        else:
            del self.store[key]
    
    def set(self, key, value, **kwargs):
        if isinstance(key, bytes):
            key = key.decode('utf-8')
        if isinstance(value, bytes):
            value = value.decode('utf-8')
        obj = {
            'value': value
        }

        if 'px' in kwargs:
            expires_in = datetime.now() + timedelta(milliseconds=int(kwargs['px']))
            obj['expires'] = expires_in.timestamp()

        self.store[key] = obj
        return 'OK'
    
    def get_type(self, key):
        val = self.get(key)
        if val is None:
            return 'none'
        if isinstance(val, str):
            return 'string'
        if isinstance(val, RedisStream):
            return 'stream'
        return'none'

    def get_all_keys(self):
        return list(self.store.keys())
    
    def xadd(self, stream_name, id, key, value):
        item_id = id
        stream = self.store.get(stream_name, None)
        if not stream or not isinstance(stream, RedisStream):
            stream = RedisStream(stream_name)
        if not self.validate_stream_id(item_id, stream):
            return None
        stream.append(key=key, value=value, id=item_id)
        self.store[stream_name] = {'value': stream}
        return item_id
    
    def validate_stream_id(self, id: str, stream: RedisStream):
        latest_part_1, _ = id.split(RedisStream.SEP)
        latest_part_1 = int(latest_part_1)
        if len(stream.items) > 0:
            last_entry = stream.items[-1]
            last_entry_part_1, _ = last_entry['id'].split(RedisStream.SEP)
            if latest_part_1 < int(last_entry_part_1):
                return False
        elif len(stream.items) == 0:
            return latest_part_1 > 0
        return False