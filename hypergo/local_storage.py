import os
from functools import wraps
from typing import Any, Callable, Union, cast

from hypergo.storage import Storage
from hypergo.utility import Utility


def addsubfolder(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self: Any, file_name: str, *args: Any) -> Any:
        return func(self, os.path.join(os.path.expanduser("~"), ".hypergo_storage", file_name), *args)

    return wrapper


class LocalStorage(Storage):
    @addsubfolder
    def load(self, file_name: str) -> bytes | str:
        content: Union[bytes, str] = ""
        try:
            with open(file_name, mode="r", encoding="utf-8") as file:
                content = file.read()
        except UnicodeDecodeError:
            with open(file_name, mode="rb") as file:
                content = file.read()
        return content

    @addsubfolder
    def save(self, file_name: str, content: bytes | str) -> None:
        Utility.create_folders_for_file(file_name)
        try:
            with open(file_name, "w", encoding="utf-8") as file:
                file.write(cast(str, content))
        except UnicodeEncodeError:
            with open(file_name, mode="wb") as file:
                file.write(cast(bytes, content))
