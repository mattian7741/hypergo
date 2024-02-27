import importlib
import inspect
import re
from functools import wraps
from typing import (Any, Callable, Dict, Generator, List, Mapping, Match,
                    Optional, Set, TypeVar, Union, cast)

from hypergo import hyperdash as _
from hypergo.config import ConfigType
from hypergo.local_storage import LocalStorage
from hypergo.storage import Storage
from hypergo.transaction import Transaction

from copy import deepcopy

T = TypeVar("T")
ENCRYPTIONKEY = "KRAgZMBXbP1OQQEJPvMTa6nfkVq63sgL2ULJIaMgfLA="


def generatorize(func: Callable[..., T]) -> Callable[..., Generator[T, None, None]]:
    def generator_function(*args: Any, **kwargs: Any) -> Generator[T, None, None]:
        result: Any = func(*args, **kwargs)
        return result if inspect.isgenerator(result) else (elem for elem in [result])

    return generator_function


def chunker(collection: Any, chunk_size: int = 1) -> Generator[List[Any], None, None]:
    chunk = []
    for item in [collection, [collection]][not _.is_array(collection)]:
        chunk.append(item)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def chunking(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
    # TODO: new functionality
    @wraps(func)
    def wrapper(data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        for chunk in chunker(_.deep_get(data, "message.body")):
            for result in func(_.deep_set(data, "message.body", chunk), *args, **kwargs):
                yield from (
                    _.deep_set(result, "message.body", chunk) for chunk in chunker(_.deep_get(result, "message.body"))
                )

    return wrapper


def streamer(collection: Any) -> Generator[Any, None, None]:
    yield from (item for item in [collection, [collection]][not _.is_array(collection)])


def streaming(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
    # TODO: new functionality
    @wraps(func)
    def wrapper(data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        for item in streamer(_.deep_get(data, "message.body")):
            for result in func(_.deep_set(data, "message.body", item), *args, **kwargs):
                yield from streamer(result)

    return wrapper


##########################################################################


def the_function(farfunc: Callable[[float, float], float], float1: float, float2: float, trans: Transaction) -> float:
    count = trans.get("count", 1)
    trans.set("count", count + 1)
    print("this is where I fail")
    result: float = farfunc(float1, float2) / count
    return result


# def the_function(arr: List[Any]) -> float:
#     print("arr: ", arr)
#     return 4321


the_config: Dict[str, Any] = {
    "lib_func": "hypergo.hypertest.the_function",
    "input_keys": ["x.y.z", "v.u.w"],
    "output_keys": ["m.n.?"],
    "input_bindings": ["{config.custom_properties.?}", "{message.body.a.x}", "{message.body.a.y}", "{transaction}"],
    # "input_bindings": ["{message.body}"],
    "output_bindings": [{"message": {"body": {"p": {"q": {"r": "{output}"}}}}}],
    "custom_properties": {"x": "{message.body.abc.fn}", "w": "{message.body.a.c}"},
}

# the_generator: Callable[..., Generator[Any, None, None]] = generatorize(the_function)
the_storage: Storage = LocalStorage()
the_context: Dict[str, Any] = {"config": the_config, "message": None, "output": None}

##########################################################################


def main() -> None:
    # message = {
    #     "transaction": "transactionkey_202308161628349844546ab7b199",
    #     "routingkey": "x.y.z",
    #     "body": {
    #         "a": {
    #             "fn": lambda m, n: (m + n) / (m * n),
    #             "x": 23,
    #             "y": 41
    #         }
    #     }
    # }

    message = {
        "routingkey": "x.y.z",
        "body": {"abc": {"fn": lambda m, n: (m + n) / (m * n), "xyz": 23, "yza": 41}}
        # ,{
        #     "a": {
        #         "fn": lambda m, n: (m + n) / (m - n),
        #         "x": 31,
        #         "y": 15
        #     }
        # }]
    }
    serialized_message = _.serialize(message, "body")
    compressed_message = _.compress(serialized_message, "body")
    encrypted_message = _.encrypt(compressed_message, "body", ENCRYPTIONKEY)
    stored_message = Executor.storebyreference(encrypted_message, "body", the_storage.use_sub_path("passbyreference"))
    import json  # pylint: disable=import-outside-toplevel

    config = ConfigType(
        {
            "lib_func": "hypergo.hypertest.the_function",
            "input_keys": ["x.y.z", "v.u.w"],
            "output_keys": ["m.n.?"],
            "input_bindings": [
                "{config.custom_properties.?}",
                "{message.body.abc.xyz}",
                "{message.body.abc.yza}",
                "{transaction}",
            ],
            # "input_bindings": ["{message.body}"],
            "output_bindings": [{"message": {"body": {"p": {"q": {"r": "{output}"}}}}}],
            "custom_properties": {"x": "{message.body.abc.fn}", "w": "{message.body.a.c}"},
        }
    )

    executor = Executor(config, the_storage)

    for i in executor.execute(stored_message):
        loaded_message = Executor.fetchbyreference(i, "body", the_storage.use_sub_path("passbyreference"))
        unencrypted = _.decrypt(loaded_message, "body", ENCRYPTIONKEY)
        decompressed = _.decompress(unencrypted, "body")
        print(json.dumps(decompressed))


# Make a copy of config to hold in state, never change it. Make a deep
# copy to use and mutate in Executor's decorators and only ever refer to
# that


class Executor:
    @staticmethod
    def func_spec(fn_name: str) -> Callable[..., Any]:
        tokens: List[str] = fn_name.split(".")
        return cast(Callable[..., Any], (getattr(importlib.import_module(".".join(tokens[:-1])), tokens[-1])))

    def __init__(self, config: ConfigType, storage: Optional[Storage] = None) -> None:
        self._config: ConfigType = config
        self._func_spec: Callable[..., Any] = Executor.func_spec(config["lib_func"])
        self._storage: Optional[Storage] = storage

    @property
    def storage(self) -> Optional[Storage]:
        return self._storage

    @property
    def config(self) -> ConfigType:
        return self._config

    @config.setter
    def config(self, config: ConfigType) -> None:
        self._config = config

    @staticmethod
    def exceptions(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            try:
                yield from func(*args, **kwargs)
            except Exception as exc:  # pylint: disable=broad-except
                print(exc)

        return wrapper

    def contextualize(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, message: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in contextualize. self: {self} Data: {message}\n")
            # _.deep_set(the_context, "input", data)
            yield from (
                _.deep_get(result, "message")
                for result in func(
                    self,
                    {"config": deepcopy(self.config), "message": message, "storage": self.storage, "output": None},
                    *args,
                    **kwargs,
                )
            )

        return wrapper

    @staticmethod
    def _handle_substitution(value: Any, data: Dict[str, Any]) -> Any:
        @_.traverse_datastructures
        def substitute(string: str, data: Dict[str, Any]) -> Any:
            result = string
            if isinstance(string, str):
                match: Optional[Match[str]] = re.match(r"^{([^}]+)}$", string)
                result = (
                    _.deep_get(data, Executor._replace_wildcard_from_routingkey(data, match.group(1)), match.group(0))
                    if match
                    else re.sub(
                        r"{([^}]+)}",
                        lambda match: str(
                            _.deep_get(data, Executor._replace_wildcard_from_routingkey(data, match.group(1)), "")
                        ),
                        string,
                    )
                )

            if result != string:
                result = substitute(result, data)
            return result

        return substitute(value, data)

    @staticmethod
    def substitutions(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in substitutions. self: {self} data: {data}\n")
            results = func(self, Executor._handle_substitution(data, data), *args, **kwargs)
            for result in results:
                yield Executor._handle_substitution(result, result)

        return wrapper

    @staticmethod
    def passbyreference(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        # TODO: This doesn't work if the data isn't already in storage. It needs
        # to read the config to determine what to passbyreference and what not
        # to
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in passbyreference self: {self} data: {data}\n")
            results = func(
                self,
                Executor.fetchbyreference(data, "message.body", _.deep_get(data, "storage").use_sub_path("passbyreference/")),
                *args,
                **kwargs,
            )
            for result in results:
                yield Executor.storebyreference(
                    result, "message.body", _.deep_get(data, "storage").use_sub_path("passbyreference/")
                )

        return wrapper


    @_.root_node
    @staticmethod
    def storebyreference(data: Any, key: str, storage: Storage) -> Any:
        out_storage_key = f"{_.unique_identifier('storagekey')}"
        storage.save(out_storage_key, _.stringify(_.deep_get(data, key)))
        return _.deep_set(data, key, out_storage_key)


    @_.root_node
    @staticmethod
    def fetchbyreference(data: Union[List[Any], Dict[str, Any]], key: str, storage: Storage) -> Any:
        return _.deep_set(data, key, _.objectify(storage.load(_.deep_get(data, key))))

    @staticmethod
    def encryption(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            input_operations = _.deep_get(data, "config.input_operations.encryption", [])

            print(f"I'm in encryption. data: {data}\n")

            for datum_to_decrypt in input_operations:
               data = _.decrypt(data, datum_to_decrypt, ENCRYPTIONKEY)

            results = func(self, data, *args, *kwargs)

            for result in results:
                output_operations = _.deep_get(data, "config.output_operations.encryption", [])
                for datum_to_encrypt in output_operations:
                    result = _.encrypt(data, datum_to_encrypt, ENCRYPTIONKEY)

                yield result
                

        return wrapper

    @staticmethod
    def compression(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in compression. self: {self} data: {data}\n")
            results = func(self, _.decompress(data, "message.body"), *args, **kwargs)
            for result in results:
                yield _.compress(result, "message.body")

        return wrapper

    @staticmethod
    def serialization(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in serialization. self: {self} data: {data}\n")
            deserialized = _.deserialize(data, "message.body")
            results = func(self, deserialized, *args, **kwargs)
            for result in results:
                yield _.serialize(result, "message.body")

        return wrapper

    @staticmethod
    def transactions(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in transactions. self: {self} data: {data}\n")
            results = func(
                self,
                Executor._load_transaction(data, _.deep_get(data, "storage").use_sub_path("transactions")),
                *args,
                **kwargs,
            )

            for result in results:
                yield Executor._save_transaction(result, _.deep_get(data, "storage").use_sub_path("transactions"))

        return wrapper

    @staticmethod
    def _load_transaction(data: Any, storage: Storage) -> Any:
        txkey: str = _.deep_get(data, "message.transaction", None)
        transaction: Transaction = Transaction.from_str(storage.load(txkey)) if txkey else Transaction()
        return _.deep_set(data, "transaction", transaction)

    @staticmethod
    def _save_transaction(data: Any, storage: Storage) -> Any:
        transaction: Transaction = _.deep_get(data, "transaction")
        txkey: str = f"transactionkey_{transaction.txid}"
        storage.save(txkey, str(transaction))
        return _.deep_set(data, "message.transaction", txkey)

    @staticmethod
    def _replace_wildcard_from_routingkey(data: Union[List[Any], Dict[str, Any]], input_string: Any) -> str:
        def find_best_key(field_path: List[str], routingkey: str) -> str:
            rk_set: Set[str] = set(routingkey.split("."))
            matched_key: str = ""
            maxlen: int = 0
            for key in _.deep_get(data, ".".join(field_path)):
                key_set: Set[str] = set(key.split("."))
                if key_set.intersection(rk_set) == key_set and len(key_set) > maxlen:
                    maxlen = len(key_set)
                    matched_key = key
            return re.sub(r"\.", "\\.", matched_key)

        node_path: List[str] = []
        for node in input_string.split("."):
            node_path.append(find_best_key(node_path, _.deep_get(data, "message.routingkey")) if node == "?" else node)
        return ".".join(node_path)

    @staticmethod
    def _get_args(args: List[Any]) -> List[Any]:
        def fn_spec(fn_name: str) -> Callable[..., Any]:
            tokens: List[str] = fn_name.split(".")
            return cast(Callable[..., Any], (getattr(importlib.import_module(".".join(tokens[:-1])), tokens[-1])))

        def a_spec(func: Callable[..., Any]) -> List[type]:
            params: Mapping[str, inspect.Parameter] = inspect.signature(func).parameters
            return [params[k].annotation for k in list(params.keys())]

        func_spec = fn_spec(_.deep_get(the_config, "lib_func"))
        arg_spec = a_spec(func_spec)
        params = []
        for arg, argtype in zip(args, arg_spec):
            val = arg
            if argtype == inspect.Parameter.empty:
                params.append(val)
            else:
                params.append(_.safecast(argtype, val))
        return params

    @staticmethod
    def bind_arguments(func: Callable[..., Generator[Any, None, None]]) -> Callable[..., Generator[Any, None, None]]:
        @wraps(func)
        def wrapper(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
            print(f"I'm in bind_arguments, self: {self} data is {data}\n")
            arguments = Executor._get_args(_.deep_get(data, "config.input_bindings"))
            print(f"I'm in bind_arguments. Args are: {arguments}\n")

            for result in func(self, Executor._get_args(_.deep_get(data, "config.input_bindings")), *args, **kwargs):
                print(f"bind arguments result: {result}")
                # TODO: new functionality
                for binding in zip(_.deep_get(result, "config.output_bindings")):
                    data.update(binding)
                    # TODO: this does NOT zip together output_keys and results and
                    # send one message to each. Instead, for each result, it sends
                    # a message to all output_keys. in other words, we get MxN
                    # messages
                    for output_key in _.deep_get(data, "config.output_keys"):
                        yield _.deep_set(data, "message.routingkey", output_key)

        return wrapper

    @exceptions
    # @unbatching
    # @batching
    @contextualize
    @substitutions
    @passbyreference
    @encryption
    @compression
    @serialization
    # @chunking
    # @streaming
    # @validation
    @transactions
    @substitutions
    # @mapping
    @bind_arguments
    def execute(self, data: Any, *args: Any, **kwargs: Any) -> Generator[Any, None, None]:
        print(f"execute self: {self} data: {data}")

        # yield from generatorize(the_function)(*data, *args, **kwargs)
        for result in generatorize(self._func_spec)(*data, *args, **kwargs):
            yield result


if __name__ == "__main__":
    main()
