import pathlib
from time import time
from itertools import chain

from datetime import datetime, timedelta
from app.namespace import ConfigNamespace
from app.rdb_parser import RDBParser

class RedisStream:
    SEP = '-'
    FORBIDDEN = '0-0'
    ANY = '*'
    ERR_MSG = 'ERR The ID specified in XADD is equal or smaller than the target stream top item'
    ERR_MSG_1 = 'ERR The ID specified in XADD must be greater than 0-0'

    def __init__(self, name: str) -> None:
        self.id = name
        self.items = []
        
    def append(self, **kwargs):
        obj = {
            'key': kwargs['key'],
            'value': kwargs['value']
        }
        self.items.append({'id': kwargs['id'], 'item': obj})
        
    @staticmethod
    def stream_is_empty(stream: 'RedisStream'):
        return not bool(len(stream.items))
    
    @staticmethod
    def get_last_id(stream: 'RedisStream'):
        if not RedisStream.stream_is_empty(stream):
            return stream.items[-1]['id']
                
    @staticmethod
    def is_between_range(low, high, id_, exclusive):
        id_ = str(id_)
        high = str(high)
        low = str(low)
        
        if low == '-':
            low = '0-0'
        else:
            low = low + '-0' if low.find(RedisStream.SEP) == -1 else low

        higher_than_low = id_ > low if exclusive else id_ >= low
        lower_than_high = id_ <= high
        
        if high == '+':
            lower_than_high = True
        elif high.find(RedisStream.SEP) == -1:
            lower_than_high = id_.split(RedisStream.SEP)[0] <= high
        
        return lower_than_high and higher_than_low
    
    @staticmethod
    def build_obj(obj):
        response = [obj['id']]
        kv_pairs = []
        for _, v in obj['item'].items():
            kv_pairs.append(v)
        response.append(kv_pairs)
        return response

    def get_items_in_range(self, low, high, exclusive=False):
        response = []
        if RedisStream.stream_is_empty(self):
            return response
        
        for item in self.items:
            in_range = RedisStream.is_between_range(low, high, item['id'], exclusive)
            if in_range:
                response.append(RedisStream.build_obj(item))
        return response    


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
        
        # constant id
        if item_id.find(RedisStream.ANY) == -1:
            success = self.validate_entry_id(item_id, stream)
            if not success:
                to_ret = False
                if item_id == RedisStream.FORBIDDEN:
                    return to_ret, RedisStream.ERR_MSG_1
                return to_ret, RedisStream.ERR_MSG
        # generated id
        else:
            item_id = self.generate_id(item_id, stream)
        stream.append(key=key, value=value, id=item_id)
        self.store[stream_name] = {'value': stream }
        return True, item_id
    
    def validate_entry_id(self, id: str, stream: RedisStream):
        if id == RedisStream.FORBIDDEN:
            return False
        ms_latest, seq_latest = id.split(RedisStream.SEP)
        ms_latest, seq_latest = int(ms_latest), int(seq_latest)
        last_id = RedisStream.get_last_id(stream)
        if last_id is None:
            if ms_latest == 0:
                return seq_latest > 0
        else:
            ms_earlier, seq_earlier = last_id.split(RedisStream.SEP)
            ms_earlier, seq_earlier = int(ms_earlier), int(seq_earlier)
            if ms_latest < ms_earlier:
                return False
            if ms_latest == ms_earlier:
                return seq_latest > seq_earlier
        return True
    
    def xrange(self, stream_name, start_id, end_id):
        stream = self.get(stream_name)
        return stream.get_items_in_range(start_id, end_id)
    
    def xread(self, stream_name, start_id):
        stream = self.get(stream_name)
        result = stream.get_items_in_range(start_id, '+', True)
        return [stream_name, result]
        
    def generate_fresh_id(self, last_id: None | str):
        auto_gen_id = [int(time() * 1000), 0]
        if last_id is None:
            return auto_gen_id
        ms_no, _ = last_id.split(RedisStream.SEP)
        ms_no = int(ms_no)
        if ms_no == auto_gen_id[0]:
            auto_gen_id[-1] += 1
            return auto_gen_id
        return auto_gen_id 

    def generate_id(self, id: str, stream: RedisStream):
        def join_parts(*args):
            return f'{RedisStream.SEP}'.join([str(x) for x in args])
        
        last_id = RedisStream.get_last_id(stream)

        if id == RedisStream.ANY:
            return join_parts(*self.generate_fresh_id(last_id))

        ms_curr, _ = id.split(RedisStream.SEP)
        ms_curr = int(ms_curr)
        

        # No elements in stream
        if last_id is None:
            if ms_curr == 0:
                return join_parts(ms_curr, 1)
            else:
                return join_parts(ms_curr, 0)
        else:
            ms_no, seq_no = last_id.split(RedisStream.SEP)
            ms_no, seq_no = int(ms_no), int(seq_no)
            
            if ms_no == ms_curr:
                return join_parts(ms_curr, seq_no + 1)
            else:
                return join_parts(ms_curr, 0)
            
        