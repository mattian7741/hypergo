from abc import ABC, abstractmethod
from typing import Union, cast

from hypergo.config import ConfigType
from hypergo.executor import Executor
from hypergo.message import MessageType
from hypergo.storage import Storage


class ServiceBusConnection(ABC):
    def general_consume(self, message: MessageType, config: ConfigType, storage: Union[Storage, None]) -> None:
        executor: Executor = Executor(config, storage)
        for execution in executor.execute(message):
            self.send(cast(MessageType, execution), config["namespace"])

    @abstractmethod
    def send(self, message: MessageType, namespace: str) -> None:
        ...
