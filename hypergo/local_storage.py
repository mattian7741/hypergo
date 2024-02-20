from functools import wraps
from typing import Any, Callable, TypeVar

from hypergo.storage import Storage
from hypergo.utility import Utility

T = TypeVar("T")


def addsubfolder(func: Callable[..., T]) -> Callable[..., T]:
    @wraps(func)
    def wrapper(self: Any, file_name: str, *args: Any) -> T:
        return func(self, f".hypergo_storage/{file_name}", *args)

    return wrapper


class LocalStorage(Storage):
    @addsubfolder
    def load(self, file_name: str) -> str:
        with open(file_name, "r", encoding="utf-8") as file:
            content: str = file.read()
        return content

    @addsubfolder
    def save(self, file_name: str, content: str) -> None:
        Utility.create_folders_for_file(file_name)

        with open(file_name, "w", encoding="utf-8") as file:
            file.write(content)
