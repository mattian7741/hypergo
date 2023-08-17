import os

from hypergo.storage import Storage
from typing import Any, Callable
from functools import wraps

def addsubfolder(func: Callable[[...], Any]):
    @wraps(func)
    def wrapper(self, file_name, *args):
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
        def create_folders_for_file(file_path: str) -> None:
            directory: str = os.path.dirname(file_path)
            try:
                os.makedirs(directory)
            except OSError as error:
                if not os.path.isdir(directory):
                    raise error

        create_folders_for_file(file_name)

        with open(file_name, "w", encoding="utf-8") as file:
            file.write(content)
