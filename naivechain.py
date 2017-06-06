from enum import Enum
import time
from collections import UserList

class Block(object):
    """docstring for Block."""
    def __init__(self, index, prev_hash, timestamp, data):
        super().__init__()
        self.index = index
        self.prev_hash = prev_hash
        self.timestamp = timestamp
        self.data = data
        self.hash = self.calculate_hash()

    @classmethod
    def is_valid_new_block(cls, new_block, prev_block):
        if new_block.index != prev_block.index + 1:
            print('invalid index')
            return False
        elif new_block.prev_hash != prev_block.hash:
            print('invalid prev hash')
            return False
        elif

    def __repr__(self):
        return "{} ({}) [{}]\n{}".format(self.index, self.timestamp, self.hash, self.data)

    def calculate_hash(self):



class Blockchain(UserList):
    def __init__(self, *args):
        super().__init__()
        self.data.append(Block(0, "0", time.time(), "this is the genesis block!! Wuhuu", "SOME HASH"))

    @property
    def latest(self):
        return self.data[-1]


class MessageType(Enum):
    QUERY_LATEST = 0
    QUERY_ALL = 1
    RESPONSE_BLOCKCHAIN = 2


sockets = list()

blockchain = Blockchain()
print(blockchain)
for block in blockchain:
    print(block)
