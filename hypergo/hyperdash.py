import base64
import binascii
import hashlib
import inspect
import json
import lzma
import types
import uuid as uuid_lib
from datetime import datetime
from functools import wraps
from typing import (Any, Callable, Dict, List, Mapping, Optional, Tuple, Union,
                    cast, get_origin)

import dill
import glom
import pydash
import yaml
from cryptography.fernet import Fernet


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
    def wrapper(value: Any, key: Optional[str] = None, *args: Tuple[Any, ...]) -> Any:
        return func({"__root__": value}, f"__root__.{key}" if key else "__root__", *args).get("__root__")

    return wrapper


def deep_get(
    data: Union[List[Any], Dict[str, Any]], key: Union[int, str], default_sentinel: Optional[Any] = object
) -> Any:
    if not pydash.has(data, str(key)) and default_sentinel == object:
        raise KeyError(f"Spec \"{key}\" not found in the dictionary {json.dumps(serialize(data, None))}")
    return pydash.get(data, str(key), default_sentinel)


def deep_set(data: Union[List[Any], Dict[str, Any]], key: str, val: Any) -> Union[List[Any], Dict[str, Any]]:
    glom.assign(data, key, val, missing=dict)
    return data


def deep_unset(data: Dict[str, Any], key: str) -> None:
    tokens: List[Any] = key.split(".")
    deep_key: str = ".".join(tokens[:-1])
    del_key: str = tokens[-1]
    if not deep_key:
        del data[del_key]
    else:
        obj: Dict[str, Any] = deep_get(data, deep_key)
        del obj[del_key]


def unique_identifier(prefix: str = "") -> str:
    return f"{prefix}{['','_'][bool(prefix)]}{utc_string()}{uuid()[:8]}"


def utc_string(prefix: str = "") -> str:
    return f"{prefix}{['','_'][bool(prefix)]}{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}"


def uuid(prefix: str = "") -> str:
    return f"{prefix}{['','_'][bool(prefix)]}{str(uuid_lib.uuid4())}"


def hash(string: str, prefix: str = "") -> str:
    return f"{prefix}{['','_'][bool(prefix)]}{hashlib.md5(string.encode('utf-8')).hexdigest()}"


def stringify(objectified: Any) -> str:
    try:
        return json.dumps(objectified)
    except (TypeError, ValueError):
        return str(objectified)


def objectify(stringified: str) -> Union[List[Any], Dict[str, Any]]:
    return cast(Union[List[Any], Dict[str, Any]], json.loads(stringified))


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
    except (binascii.Error, dill.UnpicklingError, AttributeError, MemoryError):
        return serialized


@root_node
def compress(data: Any, key: Optional[str] = None) -> Any:
    if not key:
        return base64.b64encode(lzma.compress(json.dumps(data).encode("utf-8"))).decode("utf-8")

    deep_set(
        data,
        key,
        base64.b64encode(lzma.compress(json.dumps(deep_get(data, key)).encode("utf-8"))).decode("utf-8"),
    )
    return data


@root_node
def decompress(data: Any, key: Optional[str] = None) -> Any:
    if not key:
        return json.loads(lzma.decompress(base64.b64decode(data)).decode("utf-8"))

    deep_set(
        data,
        key,
        json.loads(lzma.decompress(base64.b64decode(deep_get(data, key))).decode("utf-8")),
    )
    return data


@root_node  # change encrypt to operate on a value and move the key handling out into a decorator
def encrypt(data: Any, key: str, encryptkey: str) -> Any:
    key_bytes = encryptkey.encode("utf-8")
    data_bytes = stringify(deep_get(data, key)).encode("utf-8")
    encrypted_bytes = Fernet(key_bytes).encrypt(data_bytes)
    encrypted = encrypted_bytes.decode("utf-8")
    deep_set(data, key, encrypted)
    return data


@root_node
def decrypt(encrypted_data: Any, key: str, encryptkey: str) -> Any:
    key_bytes = encryptkey.encode("utf-8")
    encrypted_bytes = deep_get(encrypted_data, key).encode("utf-8")
    decrypted_bytes = Fernet(key_bytes).decrypt(encrypted_bytes)
    decrypted = decrypted_bytes.decode("utf-8")
    deep_set(encrypted_data, key, objectify(decrypted))
    return encrypted_data


def is_array(obj: Any) -> bool:
    return isinstance(obj, (list, set, tuple, types.GeneratorType))


def json_read(file_name: str) -> Mapping[str, Any]:
    with open(file_name, "r", encoding="utf-8") as file_handle:
        return cast(Mapping[str, Any], json.load(file_handle))


def yaml_read(file_name: str) -> Mapping[str, Any]:
    with open(file_name, "r", encoding="utf-8") as file_handle:
        return cast(Mapping[str, Any], yaml.safe_load(file_handle))


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
