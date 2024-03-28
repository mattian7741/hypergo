import os
from functools import wraps
from typing import Any, Callable, TypeVar

from hypergo.storage import Storage
from hypergo.utility import Utility

T = TypeVar("T")

class LocalStorage(Storage):
    def __init__(self, storage_root: str = None):
        self.storage_root = storage_root if storage_root else os.path.join(os.path.expanduser("~"))
        super().__init__()

    def load(self, file_name: str) -> str:
        file_path = os.path.join(self.storage_root, ".hypergo_storage", file_name)

        with open(file_path, "r", encoding="utf-8") as file:
            content: str = file.read()
        return content

    def save(self, file_name: str, content: str) -> None:
        file_path = os.path.join(self.storage_root, ".hypergo_storage", file_name)
        Utility.create_folders_for_file(file_path)

        with open(file_path, "w", encoding="utf-8") as file:
            file.write(content)
