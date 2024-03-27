import json
from typing import Any, Dict, Optional

from hypergo.utility import Utility


class Transaction:
    @staticmethod
    def create_tx(txid: Optional[str] = None, data: Optional[Any] = None) -> Dict[str, Any]:
        ret = {"txid": txid or Utility.unique_identifier()}
        if data:
            ret["data"] = data
        return ret

    def __init__(
        self,
        txid: Optional[str] = None,
        data: Optional[Any] = None,
        parentid: Optional[str] = None,
    ) -> None:
        self._stack: Dict[str, Any] = {}
        if txid:
            self._stack = {txid: data}
        else:
            self.push()

    def push(self) -> None:
        new_tx = Transaction.create_tx()
        self._stack[new_tx["txid"]] = new_tx

    def pop(self) -> Any:
        return self._stack.pop(self.txid)

    def peek(self) -> Any:
        return self._stack.get(self.txid)

    @staticmethod
    def from_str(txstr: str) -> "Transaction":
        return Transaction(**(json.loads(txstr)))

    @property
    def txid(self) -> str:
        return list(self._stack.keys())[-1]

    def serialize(self) -> Any:
        return str(self)

    def __str__(self) -> str:
        return json.dumps(Utility.serialize({"txid": self.txid, "data": self.peek()}, None))

    def set(self, key: str, value: Any) -> None:
        Utility.deep_set(self.peek(), key, value)

    def get(self, key: str, default: Optional[Any] = None) -> Any:
        return Utility.deep_get(self.peek(), key, default)


if __name__ == "__main__":
    tx = Transaction()
    tx.set("tx", "Transaction")
    print(tx.get("tx"))
    tx.push()
    tx.set("childtx", "Child Transaction")
    print(tx.get("childtx"))
    print(str(tx))
    tx.pop()
    tx.set("tx2", "Transaction2")
    print(tx.get("tx2"))
    print(str(tx))
    tx2 = Transaction.from_str(
        '{"txid": "202309081814459840042fba40d5", "data": {"txid": "202309081814459840042fba40d5", "tx": "Transaction", "tx2": "Transaction2"}}'
    )
    print("\n\n", str(tx2))
