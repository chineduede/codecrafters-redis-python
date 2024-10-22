from enum import ReprEnum

def decode(msg: bytes | None | list | int):
    if msg is None:
        return
    if isinstance(msg, int):
        return msg
    if isinstance(msg, list):
        return [decode(m) for m in msg]
    return msg.decode(encoding='utf-8')

def encode(msg: bytes | None | list | int):
    if msg is None:
        return
    if isinstance(msg, (int, str, ReprEnum)):
        return str(msg).encode('utf-8')
    if isinstance(msg, list):
        return [encode(m) for m in msg]
    return msg

