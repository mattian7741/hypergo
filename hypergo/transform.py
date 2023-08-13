from functools import wraps
from typing import Any, Callable, Generator, List, Tuple, TypeVar, cast

from hypergo.custom_types import JsonDict
from hypergo.storage import Storage
from hypergo.utility import Utility, root_node

T = TypeVar("T")

ENCRYPTIONKEY = "KRAgZMBXbP1OQQEJPvMTa6nfkVq63sgL2ULJIaMgfLA="


def config_v0_v1_passbyreference_backward_compatible(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(data: Any, key: str, *args: Tuple[Any, ...]) -> Any:
        use_key = key
        if key == "__root__":
            popped = data["__root__"].pop("body") if Utility.deep_has(data, "__root__.body") else None
            if popped:
                use_key = "__root__.storagekey"
                data["__root__"]["storagekey"] = popped
            else:
                popped = data["__root__"].pop("storagekey") if Utility.deep_has(data, "__root__.storagekey") else None
                if popped:
                    use_key = "__root__.body"
                    data["__root__"]["body"] = popped
        return func(data, use_key, *args)

    return wrapper


class Transform:
    @staticmethod
    def operation(op_name: str) -> Callable[..., Any]:
        def decorator(func: Callable[..., Generator[T, None, None]]) -> Callable[..., Generator[T, None, None]]:
            @wraps(func)
            # type: ignore
            def wrapper(self, data: Any) -> Generator[T, None, None]:
                args: List[List[Any]] = {
                    "compression": [[Utility.uncompress], [Utility.compress]],
                    "serialization": [[Utility.deserialize], [Utility.serialize]],
                    "pass_by_reference": [
                        [Transform.fetchbyreference, self.storage],
                        [Transform.storebyreference, self.storage],
                    ],
                    "encryption": [[Utility.decrypt, ENCRYPTIONKEY], [Utility.encrypt, ENCRYPTIONKEY]],
                }[op_name]
                input_operations = Utility.deep_get(self.config, "input_operations", {})
                output_operations = Utility.deep_get(self.config, "output_operations", {})
                if op_name in input_operations:
                    for key in input_operations[op_name] or [None]:
                        tokens = key.split(".")
                        if tokens[0] == "message":
                            key = ".".join(tokens[1:])
                        data = args[0][0](data, key, *args[0][1:])

                for result in func(self, data):
                    if op_name in output_operations:
                        for key in output_operations[op_name] or [None]:
                            tokens = key.split(".")
                            if tokens[0] == "message":
                                key = ".".join(tokens[1:])
                            result = args[1][0](result, key, *args[1][1:])
                    yield result

            return wrapper

        return decorator

    @staticmethod
    @root_node
    @config_v0_v1_passbyreference_backward_compatible
    def storebyreference(data: Any, key: str, base_storage: Storage) -> Any:
        storage: Storage = base_storage.use_sub_path("passbyreference/")
        str_result = Utility.stringify(Utility.deep_get(data, key))
        out_storage_key = f"storagekey_{Utility.hash(str_result)}"
        storage.save(out_storage_key, str_result)
        Utility.deep_set(data, key, out_storage_key)
        return data

    @staticmethod
    @root_node
    @config_v0_v1_passbyreference_backward_compatible
    def fetchbyreference(data: Any, key: str, base_storage: Storage) -> Any:
        storage = base_storage.use_sub_path("passbyreference/")
        storage_key = Utility.deep_get(cast(JsonDict, data), key)
        loaded = storage.load(storage_key)
        the_data = Utility.objectify(loaded)
        Utility.deep_set(data, key, the_data)
        return data
