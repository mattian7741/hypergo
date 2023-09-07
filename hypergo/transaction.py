import json
from typing import Any, Dict, Optional

from hypergo.utility import Utility


class Transaction:
    def __init__(self, txid: Optional[str] = None, data: Optional[Dict[str, Any]] = None, parent: Optional["Transaction"] = None) -> None:
        self._txid: str = txid or Utility.unique_identifier()
        self._data: Dict[str, Any] = data or {}
        self._parent: Optional[str] = parent

    @staticmethod
    def from_str(txstr: str) -> "Transaction":
        return Transaction(**(json.loads(txstr)))

    @property
    def txid(self) -> str:
        return self._txid

    def serialize(self) -> Any:
        return str(self)

    def __str__(self) -> str:
        return json.dumps(Utility.serialize({"txid": self._txid, "data": self._data, "parentid": self._parent.txid if self._parent else None}, None))

    def set(self, key: str, value: Any) -> None:
        Utility.deep_set(self._data, key, value)

    def get(self, key: str, default: Any) -> Any:
        return Utility.deep_get(self._data, key, default)

    def push(self):
        return Transaction(parent=self)

if __name__ == "__main__":
    tx = Transaction()
    print(str(tx))
    tx = tx.push()
    print(str(tx))