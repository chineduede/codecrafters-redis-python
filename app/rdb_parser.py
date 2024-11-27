import os
import io
from pathlib import Path

from app.constants import MAGIC_STR

class WrongFile(Exception):
    pass

class RDBParser:

    def __init__(self, path: Path) -> None:
        self.rdb_path = path
        self.buffer: io.BytesIO = None
        self.metadata = {}
        self.hash_table_sizes = {}
        self.database = {}

    def get_database(self):
        return self.database
    
    def create_rdb_file(self):
        with open(self.rdb_path, 'wb') as f:
            f.write(MAGIC_STR)

    def read_file(self):
        with open(self.rdb_path, 'rb') as f:
            self.buffer = io.BytesIO(f.read())

        redis_mgc = self.buffer.read(5)

        if redis_mgc != MAGIC_STR:
            raise WrongFile(f'{self.rdb_path} is not in redis format.')

        # read verion and discard
        self.buffer.seek(4, os.SEEK_CUR)

        # read magic \xFA
        while self.buffer.read(1) == b'\xFA':
            self.read_metadata()

        # we need to go back once, because the while loop overshoots
        # read magic '\xFE', database no and resizedb field
        # intentionally discarded some fields, might look into later
        # also handling single database, should be easy to extend to handle
        # multiple. go forward 3 times, backwards once
        self.buffer.seek(2, os.SEEK_CUR)

        # read size of hash tables
        self.read_hash_tables_sizes()

        # read hash tables
        self.read_hash_table()


    def read_hash_table(self):
        maps_read = 0
        while True:
            if maps_read >= self.hash_table_sizes['hash-table-size']:
                break
            obj_to_store = {}
            has_expiriy = self.buffer.read(1)
            value_t = has_expiriy
            if has_expiriy == b'\xFD':
                ttl = int.from_bytes(self.buffer.read(4), 'little')
                obj_to_store['expires'] = ttl
                value_t = self.buffer.read(1)
            elif has_expiriy == b'\xFC':
                ttl = int.from_bytes(self.buffer.read(8), 'little')
                obj_to_store['expires'] = ttl // 1000
                value_t = self.buffer.read(1)

            # read key
            key_bytes_to_read, as_int = self.len_encode_read_bytes(self.buffer.read(1))
            key = self.buffer.read(key_bytes_to_read)
            if not as_int:
                key = key.decode()
            else:
                key = int.from_bytes(key, 'big')

            # read value
            value_read = self.get_value_by_t(value_t)
            obj_to_store['value'] = value_read

            self.database[key] = obj_to_store
            maps_read += 1
            
    def get_value_by_t(self, value_t: bytes):
        if value_t == b'\x00':
            v2r, as_int = self.len_encode_read_bytes(self.buffer.read(1))
            key = self.buffer.read(v2r)
            if not as_int:
                key = key.decode()
            else:
                key = int.from_bytes(key, 'big')
            return key
        
    def read_hash_tables_sizes(self):
        # read hash-table size
        bytes_read, _ = self.len_encode_read_bytes(self.buffer.read(1))
        # self.database['hash-table-size'] = int.from_bytes(bytes_read, 'big')
        self.hash_table_sizes['hash-table-size'] = bytes_read

        # read expire hash-table size
        bytes_read, _ = self.len_encode_read_bytes(self.buffer.read(1))
        # self.database['hash-table-expire-size'] = int.from_bytes(bytes_read, 'big')
        self.hash_table_sizes['hash-table-expire-size'] = bytes_read

    def read_metadata(self):
        # read key
        key_len, key_as_int = self.len_encode_read_bytes(self.buffer.read(1))
        key = self.buffer.read(key_len)
        if not key_as_int:
            key = key.decode()
        else:
            key = int.from_bytes(key, 'big')

        # read value
        val_len, val_as_int = self.len_encode_read_bytes(self.buffer.read(1))

        val = self.buffer.read(val_len)
        if not val_as_int:
            val = val.decode()
        else:
            val = int.from_bytes(val, 'big')

        # store metadata
        self.metadata[key] = val

    def len_encode_read_bytes(self, _byte: bytes):
        """Returns a tuple of values. First value determines the lenght of bytes to read
        . Second value tells us if we interprete as integer."""

        first_two_bits = (_byte[0] & 0xC0) >> 6
        last_6_bits = _byte[0] & 0x3F
        if first_two_bits == 0x00:
            return last_6_bits, False
        elif first_two_bits == 0x01:
            another_byte = self.buffer.read(1)[0]
            last_6_bits <<= 8
            return last_6_bits | another_byte, False
        elif first_two_bits == 0x02:
            return int.from_bytes(self.buffer.read(4)), False
        elif first_two_bits == 0x03:
            if last_6_bits == 0x00:
                return 1, True
            if last_6_bits == 0x01:
                return 2, True
            if last_6_bits == 0x00:
                return 4, True
