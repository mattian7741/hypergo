from abc import abstractmethod
from typing import Any

from hypergo.connection import Connection
from hypergo.message import MessageType


class ServiceBusConnection(Connection):
    def general_consume(
        self,
        message: MessageType,
        **kwargs: Any,
    ) -> None:
        super().general_consume(message=message, **kwargs)

    @abstractmethod
    def send(self, message: MessageType, namespace: str) -> None:
        super().send(message=message, namespace=namespace)
