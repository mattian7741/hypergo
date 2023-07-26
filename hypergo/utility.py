import base64
import binascii
import hashlib
import inspect
import json
from typing import Any, Callable, Dict, Mapping, Union, cast, get_origin

import dill
import glom
import pydash
import yaml

from hypergo.custom_types import TypedDictType


def traverse_datastructures(func: Callable[[Any], Any]) -> Callable[[Any], Any]:
    def wrapper(value: Any) -> Any:
        handlers: Dict[type, Callable[[Any], Any]] = {
            dict: lambda _dict: {wrapper(key): wrapper(val) for key, val in _dict.items()},
            list: lambda _list: [wrapper(item) for item in _list],
            tuple: lambda _tuple: tuple(wrapper(item) for item in _tuple),
        }
        return handlers.get(type(value), func)(value)

    return wrapper


class Utility:
    @staticmethod
    def deep_get(dic: Union[TypedDictType, Dict[str, Any]], key: str) -> Any:
        result = pydash.get(dic, key)
        if not result:
            raise KeyError(f"Spec {key}  not found in the dictionary")
        return result

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
        except (binascii.Error, dill.UnpicklingError, AttributeError):
            return serialized
