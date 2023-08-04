from hypergo.utility import Utility
from hypergo.storage import Storage

class Transaction:
    def __init__(self):
        self._id = Utility.uuid()
        print(f"Transaction.__init__.self._id = {self._id}")

    @property
    def id(self):
        return self._id
    
class TransactionManager(object):
    _instance = None

    def __new__(cls, storage: Storage):
        if not hasattr(cls, 'instance'):
            cls.instance = super(TransactionManager, cls).__new__(cls)
        return cls.instance

    def __init__(self, storage: Storage) -> None:
        self._storage = storage.use_sub_path("transaction/")

    def load(self, transactionkey: str) -> Transaction:
        serialized = self._storage.load(transactionkey)
        print(f"load.serialized: {serialized}")
        ret = Utility.deserialize(serialized, None)
        print(f"load.ret: {ret}")
        return ret

    def save(self, transaction: Transaction) -> str:
        serialized = Utility.serialize(transaction, None)
        print(f"save.serialized: {serialized}")
        ret = self._storage.save(transaction.id, serialized)
        print(f"save.ret: {ret}")
        return ret


