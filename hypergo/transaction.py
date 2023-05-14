
from hypergo.utility import Utility
from typing import Optional, Dict, List, Any

class TransactionType(TypedDictType):
    parent: str
    data: Dict[str, Any]

class Transaction:
    def __init__(self, parent: Optional["Transaction"] = None, data: Dict[str, Any] = {}) -> None:
        self._parent: "Transaction" = parent
        self._data: Dict[str, Any] = data
        self._id = Utility.uuid()
        # transaction id
        # storage
        # request response
        # map reduce
        # transaction data ALSO in payload

    def push(self) -> "Transaction":
        return Transaction(self)

    def pop(self) -> "Transaction":
        return self._parent

    def set(self, key: str, val: Any) -> None:
        Utility.deep_set(self._data, key, val)
    
    def get(self, key: str) -> Any:
        return Utility.deep_get(self._data, key)

    def lineage(self) -> List[str]:
        if self._parent:
            return [self._id] + self._parent.lineage()
        return [self._id]
    
    def serialize(self) -> TransactionType:
        return {
            "parent": self._parent._id,
            "data": self._data
        }

    @staticmethod
    def deserialize(self, transaction_dict: TransactionType) -> "Transaction":
        return Transaction(transaction_dict.get("parent"), transaction_dict.get("data"))
