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
        stream = self.get(stream_name)

        if not stream or not isinstance(stream, RedisStream):
            stream = RedisStream(stream_name)
        success, err_msg = self.validate_stream_id(item_id, stream)
        if not success:
            return False, err_msg
        stream.append(key=key, value=value, id=item_id)
        self.store[stream_name] = {'value': stream }
        return True, item_id
    
    def validate_stream_id(self, id: str, stream: RedisStream):
        err_msg = f'ERR The ID specified in XADD is equal or smaller than the target stream top item'
        err_msg_2 = 'ERR The ID specified in XADD must be greater than 0-0'
        if id == '0-0':
            return err_msg_2
        ms_latest, seq_latest = id.split(RedisStream.SEP)
        ms_latest, seq_latest = int(ms_latest), int(seq_latest)
        if len(stream.items) > 0:
            last_entry = stream.items[-1]['id']
            ms_earlier, seq_earlier = last_entry.split(RedisStream.SEP)
            ms_earlier, seq_earlier = int(ms_earlier), int(seq_earlier)
            if ms_latest < ms_earlier:
                return False, err_msg
            if ms_latest == ms_earlier:
                return seq_latest > seq_earlier, err_msg
        elif len(stream.items) == 0:
            if ms_latest == 0:
                return seq_latest > 0, err_msg
        return True, err_msg