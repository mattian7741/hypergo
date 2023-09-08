import json
from typing import Any, Dict, Optional

from hypergo.utility import Utility


class Transaction:
    def __init__(self, txid: Optional[str] = None, data: Optional[Dict[str, Any]] = None, parentid: Optional[str] = None) -> None:
        self._txid: str = txid or Utility.unique_identifier()
        self._data: Dict[str, Any] = data or {}
        self._parentid: Optional[str] = parentid

    @staticmethod
    def from_str(txstr: str) -> "Transaction":
        return Transaction(**(json.loads(txstr)))

    @property
    def txid(self) -> str:
        return self._txid

    def serialize(self) -> Any:
        return str(self)

    def __str__(self) -> str:
        return json.dumps(Utility.serialize({"txid": self._txid, "data": self._data, "parentid": self._parentid}, None))

    def set(self, key: str, value: Any) -> None:
        Utility.deep_set(self._data, key, value)

    def get(self, key: str, default: Any) -> Any:
        return Utility.deep_get(self._data, key, default)

    def push(self):
        return Transaction(parentid=self._txid)

if __name__ == "__main__":
    tx = Transaction()
    print(str(tx))
    tx = tx.push()
    print(str(tx))
    tx2 = Transaction.from_str('{"txid": "20230908125629539310017da948", "data": {}, "parentid": "2023090812562953908673e5d2ee"}')
    print(str(tx2))