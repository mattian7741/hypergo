import os
from abc import ABC, abstractmethod
from typing import Any


class Secrets(ABC):
    @classmethod
    @abstractmethod
    def get(cls, key: str) -> Any:
        raise NotImplementedError()


class LocalSecrets(Secrets):
    @classmethod
    def get(cls, key: str) -> Any:
        return os.environ[key]
