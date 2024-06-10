import json
from typing import Any, Dict, List, Optional

from hypergo.storage import Storage
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
        data: Optional[Any] = {},
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

    def add(self, txid, routingkey, data) -> Any:
        print(f"stack: {self._stack}")
        self._stack[txid][routingkey] = data

    def retrieve(self, routingkey) -> Any:
        print(f"stack: {self._stack}")
        return self.peek().get(routingkey, None)

    @staticmethod
    def from_str(txstr: str) -> "Transaction":
        return Transaction(**(json.loads(txstr)))

    @staticmethod
    def from_file_list(txid: str, files: Dict[str, str]) -> "Transaction":
        new_tx = Transaction(txid)

        print(f"files: {files}")

        for file_content in files.values():
            print(f"new_tx: {new_tx}")
            print(f"file_content: {file_content}")
            new_tx.add(txid, file_content["routingkey"], file_content["body"])

        return new_tx

    @property
    def txid(self) -> str:
        return list(self._stack.keys())[-1]

    def serialize(self) -> Any:
        return str(self)

    def __str__(self) -> str:
        print(f"body: {self.peek()}")
        return json.dumps(Utility.serialize({"txid": self.txid, "data": self.peek()}, None))

    def set(self, key: str, value: Any) -> None:
        self._stack[self.txid][key] = {"routingkey": key, "body": value}

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
