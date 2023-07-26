from typing import Callable, Generator, TypeVar

from hypergo.utility import Utility

T = TypeVar('T')


class Transform:
    @staticmethod
    def serialization(func: Callable[..., Generator[T, None, None]]) -> Callable[..., Generator[T, None, None]]:
        return lambda self, data: (Utility.serialize(result) for result in func(self, Utility.deserialize(data)))
