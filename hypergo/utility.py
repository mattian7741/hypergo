import hashlib
import inspect
import json
import uuid
from typing import Any, Dict, Mapping, Union, cast, get_origin

import glom
import yaml

from hypergo.custom_types import TypedDictType


class Utility:
    @staticmethod
    def deep_get(dic: Union[TypedDictType, Dict[str, Any]], key: str) -> Any:
        try:
            return glom.glom(dic, key)
        except KeyError:
            return None

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

        if value_type not in [int, float, complex, bool, str, bytes, bytearray, memoryview, list, tuple, range, set, frozenset, dict]:
            return cast(value_type, provided_value)

        if value_type != inspect.Parameter.empty:
            ret = value_type(provided_value)

        return ret

    @staticmethod
    def uuid() -> str:
        return str(uuid.uuid4())
