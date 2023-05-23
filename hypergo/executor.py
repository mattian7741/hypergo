import importlib
import inspect
import json
import re
from typing import Any, Callable, Generator, List, Mapping, Optional, cast

from hypergo.config import ConfigType
from hypergo.context import ContextType
from hypergo.local_storage import LocalStorage
from hypergo.message import MessageType
from hypergo.storage import Storage
from hypergo.transaction import Transaction
from hypergo.utility import Utility


class Executor:
    @staticmethod
    def func_spec(fn_name: str) -> Callable[..., Any]:
        tokens: List[str] = fn_name.split(".")
        return cast(Callable[..., Any], (getattr(importlib.import_module(".".join(tokens[:-1])), tokens[-1])))

    @staticmethod
    def arg_spec(func: Callable[..., Any]) -> List[type]:
        params: Mapping[str, inspect.Parameter] = inspect.signature(func).parameters
        return [params[k].annotation for k in list(params.keys())]

    def __init__(self, config: ConfigType, storage: Optional[Storage] = None) -> None:
        self._config: ConfigType = config
        self._func_spec: Callable[..., Any] = Executor.func_spec(config["lib_func"])
        self._arg_spec: List[type] = Executor.arg_spec(self._func_spec)
        self._storage: Optional[Storage] = storage

    def get_args(self, context: ContextType) -> List[Any]:
        args: List[Any] = []

        for arg, argtype in zip(self._config["input_bindings"], self._arg_spec):
            # determine if arg binding is a literal denoted by '<literal>'
            val: Any = ((match := re.match(r"'(.*)'", arg)) and match.group(1)) or Utility.deep_get(context, arg)

            if argtype == inspect.Parameter.empty:  # inspect._empty:
                args.append(val)
            else:
                args.append(Utility.safecast(argtype, val))

        return args

    def retrieve(self, key: str) -> MessageType:
        if not self._storage:
            raise AttributeError("No hypergo.storage implemenation provided")
        return cast(MessageType, json.loads(self._storage.load(key)))

    def store(self, key: str, message: MessageType) -> None:
        if not self._storage:
            raise AttributeError("No hypergo.storage implemenation provided")
        self._storage.save(key, json.dumps(message))

    def open_envelope(self, envelope: MessageType) -> MessageType:
        # retrieve
        message: MessageType = envelope

        if self._storage and "pass_by_reference" in self._config.get("input_operations", []):
            message = self.retrieve(message["storagekey"])

        if self._storage:
            message["transaction"] = self.restore_transaction(message.get("transaction"))

        return message
        # decompress
        # deserialize
        # input_validation
        # input_mapping
        # bind_input_arguments

    def seal_envelope(self, message: MessageType) -> MessageType:
        # encrypt/decrypt
        # streaming
        # transaction

        # bind_output_arguments
        # output_mapping
        # output_validation
        # serialize
        # compress
        # store
        envelope: MessageType = message
        if self._storage:
            envelope["transaction"] = self.persist_transaction(message.get("transaction"))
        if self._storage and "pass_by_reference" in self._config.get("output_operations", []):
            envelope["storagekey"] = f"passbyreference/{Utility.hash(json.dumps(envelope))}"
            self.store(envelope["storagekey"], message)
            envelope["body"] = {}
        return envelope

    def persist_transaction(self, transaction: Transaction) -> str:
        this_transaction: Transaction = transaction
        while this_transaction:
            self._storage.save(f"transactions/{this_transaction.txid}", this_transaction.serialize())
            this_transaction = this_transaction.parent

        return transaction.lineage()

    def restore_transaction(self, lineage: List[str]) -> Transaction:
        parent: Optional[Transaction] = None
        for transaction_id in lineage:
            transaction: Transaction = Transaction.deserialize(self._storage.load(f"transactions/{transaction_id}"))
            transaction.parent = parent
            parent = transaction
        return parent

    def execute(self, input_envelope: MessageType) -> Generator[MessageType, None, None]:
        input_message: MessageType = self.open_envelope(input_envelope)
        context: ContextType = {"message": input_message, "config": self._config}
        if self._storage:
            context["storage"] = self._storage.use_sub_path(f"component/private/{self._config['name']}")
        args: List[Any] = self.get_args(context)
        execution: Any = self._func_spec(*args)
        return_values: List[Any] = list(execution) if inspect.isgenerator(execution) else [execution]

        for return_value in return_values:
            output_message: MessageType = {"routingkey": self.organize_tokens(self._config["output_keys"]), "body": {}}
            output_context: ContextType = {"message": output_message, "config": self._config}

            def handle_tuple(dst: ContextType, src: Any) -> None:
                for binding, tuple_elem in zip(self._config["output_bindings"], src):
                    Utility.deep_set(dst, binding, tuple_elem)

            def handle_default(dst: ContextType, src: Any) -> None:
                for binding in self._config["output_bindings"]:
                    Utility.deep_set(dst, binding, src)

            if isinstance(return_value, tuple):
                handle_tuple(output_context, return_value)
            else:
                handle_default(output_context, return_value)

            output_envelope: MessageType = self.seal_envelope(output_message)
            yield output_envelope

    def organize_tokens(self, keys: List[str]) -> str:
        return ".".join(sorted(set(".".join(keys).split("."))))


def main() -> None:
    cfg: ConfigType = {"namespace": "datalink", "name": "csvconverter", "package": "ldp-csv-to-json-converter", "lib_func": "csv_to_json_converter_appliance.__main__.csv_to_json_appliance", "input_keys": ["batch.csv"], "output_keys": ["batch.json"], "input_bindings": ["message.body.data_blob_path"], "output_bindings": ["message.body.json_data"]}
    stg: Storage = LocalStorage()
    executor = Executor(cfg, stg)
    print(executor.retrieve("hypergo/json_test_data.json"))


if __name__ == "__main__":
    main()
