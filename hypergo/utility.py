import base64
import binascii
import hashlib
import inspect
import json
import lzma
import os
import uuid
from datetime import datetime
from functools import wraps
from importlib import import_module
from types import ModuleType
from typing import (Any, Callable, Dict, Mapping, Optional, Tuple, Union, cast,
                    get_origin)

import dill
import glom
import pydash
import yaml
from cryptography.fernet import Fernet

from hypergo.custom_types import JsonType, TypedDictType


def traverse_datastructures(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(value: Any, *args: Tuple[Any, ...]) -> Any:
        handlers: Dict[type, Callable[[Any], Any]] = {
            dict: lambda _dict, *args: {wrapper(key, *args): wrapper(val, *args) for key, val in _dict.items()},
            list: lambda _list, *args: [wrapper(item, *args) for item in _list],
            tuple: lambda _tuple, *args: tuple(wrapper(item, *args) for item in _tuple),
        }
        return handlers.get(type(value), func)(value, *args)

    return wrapper


def root_node(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(value: Any, key: str, *args: Tuple[Any, ...]) -> Any:
        return func(
            {"__root__": value},
            f"__root__.{key}" if key else "__root__",
            *args,
        ).get("__root__")

    return wrapper


class Utility:  # pylint: disable=too-many-public-methods
    @staticmethod
    def create_folders_for_file(file_path: str) -> str:
        directory: str = os.path.dirname(file_path)
        try:
            os.makedirs(directory)
        except OSError as error:
            if not os.path.isdir(directory):
                raise error
        return file_path

    @staticmethod
    def unique_identifier() -> str:
        # return str(uuid.uuid4())
        return f"{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}{str(uuid.uuid4())[:8]}"

    @staticmethod
    def deep_del(dic: Dict[str, Any], key: str) -> None:
        tokens = key.split(".")
        deep_key = ".".join(tokens[:-1])
        del_key = tokens[-1]
        if not deep_key:
            del dic[del_key]
        else:
            obj = Utility.deep_get(dic, deep_key)
            del obj[del_key]

    @staticmethod
    def deep_has(dic: Union[TypedDictType, Dict[str, Any]], key: str) -> bool:
        return pydash.has(dic, key)

    @staticmethod
    def deep_get(
        dic: Union[TypedDictType, Dict[str, Any]],
        key: str,
        default_sentinel: Optional[Any] = object,
    ) -> Any:
        if not pydash.has(dic, key) and default_sentinel == object:
            raise KeyError(f"Spec \"{key}\" not found in the dictionary {json.dumps(Utility.serialize(dic, None))}")
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
    @root_node
    @traverse_datastructures
    def serialize(obj: Any, key: Optional[str] = None) -> Any:
        if type(obj) in [None, bool, int, float, str]:
            return cast(Union[None, bool, int, float, str], obj)

        try:
            return obj.serialize()
        except AttributeError:
            pass

        serialized: bytes = dill.dumps(obj)
        encoded: bytes = base64.b64encode(serialized)
        utfdecoded: str = encoded.decode("utf-8")
        return utfdecoded

    @staticmethod
    @root_node
    @traverse_datastructures
    def deserialize(serialized: str, key: Optional[str] = None) -> Any:
        if not serialized:
            return serialized
        try:
            utfencoded: bytes = serialized.encode("utf-8")
            decoded: bytes = base64.b64decode(utfencoded)
            deserialized: Any = dill.loads(decoded)
            return deserialized
        except (
            binascii.Error,
            dill.UnpicklingError,
            AttributeError,
            MemoryError,
        ):
            return serialized

    @staticmethod
    @root_node
    def compress(data: Any, key: Optional[str] = None) -> Any:
        if not key:
            return base64.b64encode(lzma.compress(json.dumps(data).encode("utf-8"))).decode("utf-8")

        Utility.deep_set(
            data,
            key,
            base64.b64encode(lzma.compress(json.dumps(Utility.deep_get(data, key)).encode("utf-8"))).decode("utf-8"),
        )
        return data

    @staticmethod
    @root_node
    def uncompress(data: Any, key: Optional[str] = None) -> Any:
        if not key:
            return json.loads(lzma.decompress(base64.b64decode(data)).decode("utf-8"))

        Utility.deep_set(
            data,
            key,
            json.loads(lzma.decompress(base64.b64decode(Utility.deep_get(data, key))).decode("utf-8")),
        )
        return data

    @staticmethod
    @root_node
    def encrypt(data: Any, key: str, encryptkey: str) -> Any:
        key_bytes = encryptkey.encode("utf-8")
        data_bytes = Utility.stringify(Utility.deep_get(data, key)).encode("utf-8")
        encrypted_bytes = Fernet(key_bytes).encrypt(data_bytes)
        encrypted = encrypted_bytes.decode("utf-8")
        Utility.deep_set(data, key, encrypted)
        return data

    @staticmethod
    @root_node
    def decrypt(encrypted_data: Any, key: str, encryptkey: str) -> Any:
        key_bytes = encryptkey.encode("utf-8")
        encrypted_bytes = Utility.deep_get(encrypted_data, key).encode("utf-8")
        decrypted_bytes = Fernet(key_bytes).decrypt(encrypted_bytes)
        decrypted = decrypted_bytes.decode("utf-8")
        Utility.deep_set(encrypted_data, key, Utility.objectify(decrypted))
        return encrypted_data

    @staticmethod
    def generate_fernet_key() -> bytes:
        # Generate a random 32-byte key
        key = Fernet.generate_key()
        # Ensure the key is URL-safe base64 encoded and exactly 32 bytes long
        if len(key) != 44:
            raise ValueError("Failed to generate a valid Fernet key.")
        url_safe_key = base64.urlsafe_b64encode(key)
        return url_safe_key


class DynamicImports:
    def __init__(self, path: str, package_prefix: str):
        self.path = path
        self.package_prefix = package_prefix

    @staticmethod
    def dynamic_imp_module(package: str, module_name: str) -> ModuleType:
        name = package + "." + module_name
        return import_module(name=name)

    @staticmethod
    def dynamic_imp_class(package: str, module_name: str, class_name: str) -> Any:
        module = DynamicImports.dynamic_imp_module(package=package, module_name=module_name)
        return getattr(module, class_name)
