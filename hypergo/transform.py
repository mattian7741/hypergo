from typing import Any, Callable, Generator, List, TypeVar, cast

from hypergo.custom_types import JsonDict, JsonType
from hypergo.storage import Storage
from hypergo.utility import Utility

T = TypeVar("T")


class Transform:
    @staticmethod
    def operations(func):
        opmap = {
            "uncompress": Utility.uncompress,
            "deserialize": Utility.deserialize,
            "serialize": Utility.serialize,
            "compress": Utility.compress
        }
        def sort_a_like_b(A, B):
            B_index = {b: i for i, b in enumerate(B)}
            return sorted(A, key=lambda x: B_index.get(x[0], len(B)))

        def wrapper(self, data):

            inops = sort_a_like_b(Utility.deep_get(self._config, "input_operations"), list(opmap.keys()))
            outops = sort_a_like_b(Utility.deep_get(self._config, "output_operations"), list(opmap.keys()))
            mod_data = data
            for op in inops:
                mod_data = opmap[op[0]](mod_data, *op[1:])
            for result in func(self, mod_data):
                mod_data = result             
                for op in outops:
                    mod_data = opmap[op[0]](mod_data, *op[1:])

                yield mod_data
        return wrapper

    @staticmethod
    def serialization(func: Callable[..., Generator[T, None, None]]) -> Callable[..., Generator[T, None, None]]:
        def serialized_func(self: Any, data: T) -> Generator[T, None, None]:
            serialized_data = (Utility.serialize(result) for result in func(self, Utility.deserialize(data)))
            return serialized_data

        return serialized_func

    @staticmethod
    def compression(
        key: str,
    ) -> Callable[[Callable[..., Generator[T, None, None]]], Callable[..., Generator[T, None, None]]]:
        def decorator(func: Callable[..., Generator[T, None, None]]) -> Callable[..., Generator[T, None, None]]:
            def wrapped_func(self: Any, data: T) -> Generator[T, None, None]:
                input_operations = Utility.deep_get(self.config, "input_operations")
                output_operations = Utility.deep_get(self.config, "output_operations")

                if "compression" in input_operations:
                    data = Utility.uncompress(data, key)

                for result in func(self, data):
                    if "compression" in output_operations:
                        result = Utility.compress(result, key)
                    yield result

            return wrapped_func

        return decorator

    @staticmethod
    def pass_by_reference(
        func: Callable[..., Generator[T, None, None]]
    ) -> Callable[..., Generator[JsonDict, None, None]]:
        def wrapped_func(self: Any, data: T) -> Generator[JsonDict, None, None]:
            storage: Storage = self.storage.use_sub_path("passbyreference")
            the_data: JsonType = cast(JsonType, data)
            input_operations: List[str] = Utility.deep_get(self.config, "input_operations")
            output_operations: List[str] = Utility.deep_get(self.config, "output_operations")

            if "pass_by_reference" in input_operations:
                storage_key = Utility.deep_get(cast(JsonDict, data), "storagekey")
                the_data = Utility.objectify(storage.load(storage_key))

            for result in func(self, the_data):
                out_message = cast(JsonDict, result)
                if "pass_by_reference" in output_operations:
                    str_result = Utility.stringify(out_message)
                    out_storage_key = Utility.hash(str_result)
                    storage.save(out_storage_key, str_result)
                    out_message = {"storagekey": out_storage_key}
                yield out_message

        return wrapped_func
