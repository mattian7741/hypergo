import base64
import binascii
import hashlib
import inspect
import json
import lzma
from typing import (Any, Callable, Dict, Mapping, Optional, Tuple, Union, cast,
                    get_origin)

import dill
import glom
import pydash
import yaml

from hypergo.custom_types import JsonType, TypedDictType


def traverse_datastructures(func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(value: Any, *args: Tuple[Any, ...]) -> Any:
        handlers: Dict[type, Callable[[Any], Any]] = {
            dict: lambda _dict, *args: {wrapper(key, *args): wrapper(val, *args) for key, val in _dict.items()},
            list: lambda _list, *args: [wrapper(item, *args) for item in _list],
            tuple: lambda _tuple, *args: tuple(wrapper(item, *args) for item in _tuple),
        }
        return handlers.get(type(value), func)(value, *args)

    return wrapper


class Utility:
    @staticmethod
    def deep_get(dic: Union[TypedDictType, Dict[str, Any]], key: str, default_sentinel: Optional[Any] = object) -> Any:
        if not pydash.has(dic, key) and default_sentinel == object:
            raise KeyError(f"Spec \"{key}\" not found in the dictionary {json.dumps(dic)}")
        return pydash.get(dic, key, default_sentinel)

    @staticmethod
    def deep_set(dic: Union[TypedDictType, Dict[str, Any]], key: str, val: Any) -> None:
        glom.assign(dic, key, val, missing=dict)

    @staticmethod
    def yaml_read(file_name: str) -> Mapping[str, Any]:
        with open(file_name, "r", encoding="utf-8") as file_handle:
            return cast(Mapping[str, Any], yaml.safe_load(file_handle))

    @staticmethod
    def yaml_write(file_name: str, dic: Union[TypedDictType, Dict[str, Any]]) -> None:
        pass

    @staticmethod
    def json_read(file_name: str) -> Mapping[str, Any]:
        with open(file_name, "r", encoding="utf-8") as file_handle:
            return cast(Mapping[str, Any], json.load(file_handle))

    @staticmethod
    def json_write(file_name: str, dic: Dict[str, Any]) -> None:
        pass

    @staticmethod
    def hash(content: str) -> str:
        return hashlib.md5(content.encode("utf-8")).hexdigest()

    @staticmethod
    def safecast(expected_type: type, provided_value: Any) -> Any:
        ret: Any = provided_value
        value_type: Any = get_origin(expected_type) or expected_type

        if value_type not in [
            int,
            float,
            complex,
            bool,
            str,
            bytes,
            bytearray,
            memoryview,
            list,
            tuple,
            range,
            set,
            frozenset,
            dict,
        ]:
            return cast(value_type, provided_value)

        if value_type != inspect.Parameter.empty:
            ret = value_type(provided_value)

        return ret

    @staticmethod
    def stringify(obj: Any) -> str:
        try:
            return json.dumps(obj)
        except (TypeError, ValueError):
            return str(obj)

    @staticmethod
    def objectify(string: str) -> JsonType:
        return cast(JsonType, json.loads(string))

    @staticmethod
    @traverse_datastructures
    def serialize(obj: Any) -> Union[None, bool, int, float, str]:
        if type(obj) in [None, bool, int, float, str]:
            return cast(Union[None, bool, int, float, str], obj)

        serialized: bytes = dill.dumps(obj)
        encoded: bytes = base64.b64encode(serialized)
        utfdecoded: str = encoded.decode("utf-8")
        return utfdecoded

    @staticmethod
    @traverse_datastructures
    def deserialize(serialized: str) -> Any:
        if not serialized:
            return serialized
        try:
            utfencoded: bytes = serialized.encode("utf-8")
            decoded: bytes = base64.b64decode(utfencoded)
            deserialized: Any = dill.loads(decoded)
            return deserialized
        except (binascii.Error, dill.UnpicklingError, AttributeError, ValueError, MemoryError):
            return serialized

    @staticmethod
    def compress(data: Any, key: Optional[str] = None) -> Any:
        root_data = {"root": data}
        root_key = f"root.{key}" if key else "root"
        Utility.deep_set(
            root_data,
            root_key,
            base64.b64encode(lzma.compress(json.dumps(Utility.deep_get(root_data, root_key)).encode("utf-8"))).decode(
                "utf-8"
            ),
        )
        return data

    @staticmethod
    def uncompress(compressed_data: Any, key: Optional[str] = None) -> Any:
        root_data = {"root": compressed_data}
        root_key = f"root.{key}" if key else "root"
        Utility.deep_set(
            root_data,
            root_key,
            json.loads(lzma.decompress(base64.b64decode(Utility.deep_get(root_data, root_key))).decode("utf-8")),
        )
        return compressed_data
