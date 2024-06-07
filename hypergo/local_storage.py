import json
import os
from functools import wraps
from typing import Any, Callable, List

from hypergo.storage import Storage
from hypergo.utility import Utility


def addsubfolder(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self: Any, file_name: str, *args: Any) -> Any:
        return func(self, os.path.join(os.path.expanduser("~"), ".hypergo_storage", file_name), *args)

    return wrapper


class LocalStorage(Storage):
    @addsubfolder
    def load(self, file_name: str) -> str:
        with open(file_name, "r", encoding="utf-8") as file:
            content: str = file.read()
        return content

    @addsubfolder
    def load_directory(self, path: str) -> List[str]:
        print(f"path: {path}")
        contents = {}
        all_filenames = [f for f in os.listdir(path) if not f.startswith('.')]

        print(f"all_filenames: {all_filenames}")

        for file_name in all_filenames:
            with open(f"{path}/{file_name}", "r", encoding="utf-8") as file:
                contents[file_name] = json.loads(file.read())

        return contents

    @addsubfolder
    def save(self, file_name: str, content: str) -> None:
        Utility.create_folders_for_file(file_name)

        with open(file_name, "w", encoding="utf-8") as file:
            file.write(content)

    @addsubfolder
    def create_directory(self, file_name: str) -> None:
        Utility.create_folders_for_file(file_name)
