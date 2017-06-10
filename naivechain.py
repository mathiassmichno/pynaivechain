from enum import Enum
import sys
import time
from collections import UserList
from hashlib import blake2b
from itertools import chain, islice
from json import dumps, loads
from pprint import pprint, pformat
import asyncio
import websockets
from sanic import Sanic
from sanic.response import json, text

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
        print("Adding block with data:\n\t", block_data)
        self.data.append(Block(data=block_data, prev_block=self.latest))

    @property
    def genesis(self):
        return self.data[0]

    @property
    def latest(self):
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
    RESPONSE_LATEST = 3

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
        elif self.msg_type is MessageType.RESPONSE_LATEST:
            return {**d, 'data': blockchain.latest.asdict()}


def replace_chain(new_chain):
    global blockchain
    if new_chain.is_valid() and len(new_chain) > len(blockchain):
        print('Received blockchain is valid and longer. Replacing original')
        blockchain = new_chain
        broadcast_queue.put(Message(MessageType.RESPONSE_LATEST))
    else:
        print('Received blockchain invalid! Discarding')

def dict_decoder(obj):
    if obj['__type__'] == 'Message':
        return Message(**obj)
    elif obj['__type__'] == 'Block':
        return Block(**obj)
    elif obj['__type__'] == 'Blockchain':
        obj['blocks'] = [Block(**b) for b in obj['blocks']]
        return Blockchain(**obj)

def json_decoder(json):
    obj = loads(json)
    return dict_decoder(obj)

async def response_data_decoder(data_dict):
    data = dict_decoder(data_dict)
    if isinstance(data, Blockchain):
        return data.latest
        recv_blockchain = data
    elif isinstance(data, Block):
        recv_latest = data
        recv_blockchain = None
    else:
        print("Unknown response data")
        return

async def handle_message(message, connection):
    if message.msg_type is MessageType.QUERY_ALL:
        await connection.msg_queue.put(Message(MessageType.RESPONSE_BLOCKCHAIN))
    elif message.msg_type is MessageType.QUERY_LATEST:
        await connection.msg_queue.put(Message(MessageType.RESPONSE_LATEST))
    elif message.msg_type is MessageType.RESPONSE_BLOCKCHAIN:
        await handle_blockchain_response(dict_decoder(message.data), connection)
    elif message.msg_type is MessageType.RESPONSE_LATEST:
        await handle_latest_block_response(dict_decoder(message.data), connection)

async def handle_blockchain_response(recv_blockchain, conn):
    if await handle_latest_block_response(recv_blockchain.latest, conn):
        return
    elif recv_blockchain.is_valid():
        print("blockchain out of sync! Replacing with received")
        replace_chain(recv_blockchain)
    else:
        print('Received invalid blockchain!')

async def handle_latest_block_response(recv_latest, conn):
    latest = blockchain.latest
    pprint(recv_latest.asdict())
    pprint(latest.asdict())
    if recv_latest.hash == latest.hash:
        print("Identical hashes, blockchains in sync")
    elif recv_latest.index > latest.index:
        print(f'index larger recv:{recv_latest.index} > latest:{latest.index}')
        if recv_latest.prev_hash == latest.hash:
            print("good block, appending to blockchain")
            print(f'recv.prev_hash:{recv_latest.prev_hash} == latest.hash:{latest.hash}')
            blockchain.append(recv_latest)
        else:
            await broadcast_queue.put(Message(MessageType.QUERY_ALL))
            return False
    elif recv_latest.index < latest.index:
        ahead = latest.index - recv_latest.index
        print(f'Local blockchain ahead by {ahead} blocks')
        if ahead > 1:
            await conn.msg_queue.put(Message(MessageType.RESPONSE_BLOCKCHAIN))
        else:
            await conn.msg_queue.put(Message(MessageType.RESPONSE_LATEST))
    return True


class Connection(object):
    def __init__(self, websocket):
        super().__init__()
        self.ws = websocket
        self.msg_queue = asyncio.Queue()

    def __str__(self):
        return pformat({**self.asdict(), 'msg_queue_empty' : self.msg_queue.empty})

    def asdict(self):
        return {'remote_address': '{}:{}'.format(*self.ws.remote_address),
                'local_address': '{}:{}'.format(*self.ws.local_address)}

    async def consumer_handler(self):
        while True:
            json_message = await self.ws.recv()
            message = json_decoder(json_message)
            print("RECV ->", self.asdict(), message.msg_type)
            await handle_message(message, self)

    async def producer_handler(self):
        while True:
            message = await self.msg_queue.get()
            print("SEND ->", self.asdict(), message.msg_type)
            json_message = dumps(message.asdict())
            await self.ws.send(json_message)

    async def handler(self):
        consumer_task = asyncio.ensure_future(self.consumer_handler())
        producer_task = asyncio.ensure_future(self.producer_handler())
        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        pprint(done)
        for task in pending:
            task.cancel()

connections = set()

async def connect_to_peer(remote_address, sync=False):
    print("Trying to connect to", remote_address)
    ws = await websockets.connect(f'ws://{remote_address}')
    await conn_handler(None, ws, sync=sync)

app = Sanic()

@app.route('/block/<index:int>', methods=['GET'])
async def get_blockchain(request, index):
    return json(blockchain[index].asdict())

@app.route('/block/<hash>', methods=['GET'])
async def get_blockchain(request, hash):
    return json(next((b.asdict() for b in blockchain if b.hash == hash), None))

@app.route('/block', methods=['GET'])
async def get_blockchain(request):
    return json(blockchain.asdict())

@app.route('/block', methods=['POST'])
async def add_block(request):
    data = request.json['data']
    assert isinstance(data, str)
    blockchain.add_block(data)
    await broadcast_queue.put(Message(MessageType.RESPONSE_LATEST))
    return json(blockchain.latest.asdict())

@app.route('/peer', methods=['GET'])
async def list_peers(request):
    return json([conn.asdict() for conn in connections])

@app.route('/peer', methods=['POST'])
async def add_peer(request):
    print(type(request.json))
    print(request.json)
    asyncio.ensure_future(connect_to_peer(request.json['remote_address'], sync=True))
    return text("peer connected")

@app.websocket('/')
async def conn_handler(request, ws, sync=False):
    global connections
    conn = Connection(ws)
    if sync:
        await conn.msg_queue.put(Message(MessageType.QUERY_LATEST))
    connections.add(conn)
    print("peer connected ", ws.remote_address, "total connections", len(connections))
    try:
        await asyncio.wait([c.handler() for c in connections])
    finally:
        print("Closed connection:", conn.asdict())
        connections.remove(conn)

@app.listener('after_server_start')
async def init_peers_and_broadcast(app, loop):
    global broadcast_queue
    broadcast_queue = asyncio.Queue(loop=loop)
    await connect_to_initial_peers()
    await broadcast()


async def connect_to_initial_peers():
    if len(sys.argv) > 2:
        for peer in sys.argv[2].split(';'):
            asyncio.ensure_future(connect_to_peer(peer, sync=True))

async def broadcast():
    while True:
        message = await broadcast_queue.get()
        print("Message in broadcast queue")
        pprint(message.asdict())
        for conn in connections:
            try:
                print("pushing message to", conn.asdict())
                await conn.msg_queue.put(message)
            except Exception as e:
                pass


blockchain = Blockchain()

app.run(host="0.0.0.0", port=str(sys.argv[1]))
