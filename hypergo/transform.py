from typing import Any, Callable, Generator, List, TypeVar, cast

from hypergo.custom_types import JsonDict, JsonType
from hypergo.storage import Storage
from hypergo.utility import Utility

T = TypeVar("T")


class Transform:
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
                input_operations = Utility.deep_get(self.config, "input_operations", [])
                output_operations = Utility.deep_get(self.config, "output_operations", [])

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
            input_operations: List[str] = Utility.deep_get(self.config, "input_operations", [])
            output_operations: List[str] = Utility.deep_get(self.config, "output_operations", [])

            if "pass_by_reference" in input_operations:
                storage_key = Utility.deep_get(cast(JsonDict, data), "storagekey")
                the_data = Utility.objectify(storage.load(storage_key))

            for result in func(self, the_data):
                out_message = cast(JsonDict, result)
                if "pass_by_reference" in output_operations:
                    str_result = Utility.stringify(out_message)
                    out_storage_key = Utility.hash(str_result)
                    storage.save(out_storage_key, str_result)
                    out_message = {"body": {}, "routingkey": out_message['routingkey'], "storagekey": out_storage_key}
                yield out_message

        return wrapped_func
