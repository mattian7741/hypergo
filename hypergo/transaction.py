import sys
from typing import Any, Dict, List, Optional

from hypergo.custom_types import TypedDictType
from hypergo.utility import Utility

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


class TransactionType(TypedDictType):
    parent_txid: NotRequired[Optional[str]]
    data: Dict[str, Any]


class Transaction:
    def __init__(self, parent_txid: Optional[str] = None, existing_data: Optional[Dict[str, Any]] = None) -> None:
        data: Dict[str, Any] = existing_data or {}
        self._parent: Optional["Transaction"] = Transaction.restore(parent_txid)
        self._data: Dict[str, Any] = data
        self._txid: str = Utility.uuid()
        # transaction id
        # storage
        # request response
        # map reduce
        # transaction data ALSO in payload

    @property
    def txid(self) -> str:
        return self._txid

    @property
    def parent(self) -> Optional["Transaction"]:
        return self._parent

    def push(self) -> "Transaction":
        return Transaction(self)

    def pop(self) -> Optional["Transaction"]:
        return self._parent

    def set(self, key: str, val: Any) -> None:
        Utility.deep_set(self._data, key, val)

    def get(self, key: str) -> Any:
        return Utility.deep_get(self._data, key)

    def lineage(self) -> List[str]:
        if self._parent:
            return [self._txid] + self._parent.lineage()
        return [self._txid]

    def serialize(self) -> TransactionType:
        return {"parent_txid": self._parent.txid if self._parent else None, "data": self._data}

    @staticmethod
    def deserialize(transaction_dict: TransactionType) -> "Transaction":
        return Transaction(transaction_dict.get("parent_txid"), transaction_dict.get("data"))
