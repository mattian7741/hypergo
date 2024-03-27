import os
from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def load(self, file_name: str) -> str:
        pass

    @abstractmethod
    def save(self, file_name: str, content: str) -> None:
        pass

    def use_sub_path(self, sub_path: str) -> "Storage":
        return SubStorage(self, sub_path)


class SubStorage(Storage):
    def __init__(self, base_storage: Storage, sub_path: str) -> None:
        self._base_storage: Storage = base_storage
        self._sub_path: str = sub_path

    def load(self, file_name: str) -> str:
        return self._base_storage.load(os.path.join(self._sub_path, file_name))

    def save(self, file_name: str, content: str) -> None:
        return self._base_storage.save(os.path.join(self._sub_path, file_name), content)
