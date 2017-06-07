from enum import Enum
import time
from collections import UserList
from hashlib import blake2b
from itertools import chain, islice
from json import dumps, loads
from pprint import pprint, pformat
import websockets
import asyncio

class Block(object):
    """docstring for Block."""
    def __init__(self, index=None, prev_hash=None, timestamp=None, data=None, prev_block=None, **kwargs):
        super().__init__()
        self.index = index if index is not None else prev_block.index + 1
        self.prev_hash = prev_hash if prev_hash is not None else prev_block.hash
        self.timestamp = timestamp if timestamp is not None else time.time()
        self.data = data
        self.hash = self.calculate_hash()

    def is_valid(self, prev_block):
        if self.index != prev_block.index + 1:
            print(self, 'invalid index')
            return False
        elif self.prev_hash != prev_block.hash:
            print(self, 'invalid prev hash')
            return False
        elif self.hash != self.calculate_hash():
            print(self, 'invalid hash')
            return False
        return True

    def __repr__(self):
        return pformat(self.asdict())

    def asdict(self):
        return {'__type__': 'Block',
                'index': self.index,
                'prev_hash': self.prev_hash,
                'timestamp': self.timestamp,
                'data': self.data,
                'hash': self.hash}

    def calculate_hash(self):
        data_string = str(self.index) + str(self.prev_hash) + str(self.timestamp) + str(self.data)
        return blake2b(data_string.encode('utf-8'), digest_size=20).hexdigest()



class Blockchain(UserList):
    def __init__(self, blocks=None, **kwargs):
        super().__init__(blocks)
        if blocks is None:
            genesis = Block(index=0,
                            prev_hash="0",
                            timestamp=1496859503.033272,
                            data="Genesis Block!!")
            self.data.append(genesis)

    def add_block(self, block_data):
        self.data.append(Block(data=block_data, prev_block=self.latest_block))

    @property
    def genesis_block(self):
        return self.data[0]

    @property
    def latest_block(self):
        return self.data[-1]

    def is_valid(self):
        prev_current_zip = islice(zip(self.data, chain(self.data[1:], (None))), 1)
        for prev_block, block in prev_current_zip:
            if not block.is_valid(prev_block):
                return False
        return True

    def __repr__(self):
        return pformat(self.asdict())

    def asdict(self):
        return {'__type__': 'Blockchain', 'blocks': list(map(lambda b: b.asdict(), self.data))}

class MessageType(Enum):
    QUERY_LATEST = 0
    QUERY_ALL = 1
    RESPONSE_BLOCKCHAIN = 2
    RESPONSE_LATEST_BLOCK = 3

class Message(object):
    def __init__(self, msg_type, data=None, **kwargs):
        super().__init__()
        self.msg_type = msg_type if isinstance(msg_type, MessageType) else MessageType(msg_type)
        self.data = data

    def asdict(self):
        d = {'__type__': 'Message', 'msg_type': self.msg_type.value}
        if self.msg_type in (MessageType.QUERY_LATEST, MessageType.QUERY_ALL):
            return d
        elif self.msg_type is MessageType.RESPONSE_BLOCKCHAIN:
            return {**d, 'data': blockchain.asdict()}
        elif self.msg_type is MessageType.RESPONSE_LATEST_BLOCK:
            return {**d, 'data': blockchain.latest_block.asdict()}


def replace_chain(new_chain):
    global blockchain
    if new_chain.is_valid() and len(new_chain) > len(blockchain):
        print('Received blockchain is valid and longer. Replacing original')
        blockchain = new_chain
        # TODO: broadcast latest block to all peers
    else:
        print('Received blockchain invalid! Discarding')

def json_decoder(json):
    obj = loads(json)
    if obj['__type__'] == 'Message':
        return Message(**obj)
    elif obj['__type__'] == 'Block':
        return Block(**obj)
    elif obj['__type__'] == 'Blockchain':
        obj['blocks'] = [Block(**b) for b in obj['blocks']]
        return Blockchain(**obj)

async def handle_message(message, connection):
    if message.msg_type == MessageType.QUERY_ALL:
        await connection.msg_queue.put(Message(MessageType.RESPONSE_BLOCKCHAIN))
    elif message.msg_type == MessageType.QUERY_LATEST:
        await connection.msg_queue.put(Message(MessageType.RESPONSE_LATEST_BLOCK))
    elif message.msg_type in (MessageType.RESPONSE_BLOCKCHAIN, MessageType.RESPONSE_LATEST_BLOCK):
        await handle_response(message.data)

async def handle_response(data):
    if isinstance(data, Blockchain):
        recv_latest_block = data.latest_block
        recv_blockchain = data
    else:
        recv_latest_block = data
        recv_blockchain = None
    latest_block = blockchain.latest_block
    if recv_latest_block.index > latest_block.index:
        if recv_latest_block.prev_hash == latest_block.hash:
            blockchain.append(recv_latest_block)
        elif recv_blockchain is None:
            await message_queue.put(Message(MessageType.QUERY_ALL))
        else:
            replace_chain(recv_blockchain)


class Connection(object):
    def __init__(self, websocket):
        super().__init__()
        self.ws = websocket
        self.msg_queue = asyncio.Queue()

    async def consumer_handler(self):
        while True:
            json_message = await self.ws.recv()
            print("recv", json_message)
            message = json_decoder(json_message)
            pprint(message.asdict())
            await handle_message(message, self)

    async def producer_handler(self):
        while True:
            message = await self.msg_queue.get()
            json_message = dumps(message.asdict())
            print("send", json_message)
            await self.ws.send(json_message)

    async def handler(self):
        consumer_task = asyncio.ensure_future(self.consumer_handler())
        producer_task = asyncio.ensure_future(self.producer_handler())
        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        for task in pending:
            task.cancel()


connections = set()

async def conn_handler(ws, path):
    print("WOW", path, ws)
    global connections
    conn = Connection(ws)
    connections.add(conn)
    try:
        await asyncio.wait([c.handler() for c in connections])
    finally:
        connections.remove(conn)

message_queue = asyncio.Queue()

blockchain = Blockchain()
blockchain.add_block("Memes are real")
blockchain.add_block("Pepe is the shit")
print(blockchain)

print()
print("Valid blockchain() =>", blockchain.is_valid())

chain_json = dumps(blockchain.asdict())
print(chain_json)
newchain = json_decoder(chain_json)

print()
print("Valid blockchain() =>", newchain.is_valid())

print()
print(Message(MessageType.RESPONSE_BLOCKCHAIN).asdict())

p2pserver = websockets.serve(conn_handler, 'localhost', 6001)

asyncio.get_event_loop().run_until_complete(p2pserver)
asyncio.get_event_loop().run_forever()
