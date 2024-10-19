from app.encoder import *

encoder = RespEncoder()
print(encoder.encode([['red', 'blue'], ['orange']], EncodedMessageType.ARRAY, encode_type = EncodedMessageType.SIMPLE_STRING))
