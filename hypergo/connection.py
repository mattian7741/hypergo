from abc import ABC, abstractmethod
from typing import Any, cast

from hypergo.config import ConfigType
from hypergo.executor import Executor
from hypergo.message import MessageType
from hypergo.monitor import collect_metrics
from hypergo.utility import Utility
from hypergo.validation import Ignorable


class Connection(ABC):

    def general_consume(self, message: MessageType, **kwargs: Any) -> None:
        config: ConfigType = kwargs.pop("config")
        executor: Executor = Executor(config, **kwargs)
        self.__send_message(executor=executor, message=message, config=config)

    @collect_metrics
    def __send_message(self, executor: Executor, message: MessageType, config: ConfigType) -> None:
        for execution_result in executor.execute(message):
            exception = Utility.deep_get(execution_result, 'exception')
            if exception:
                if isinstance(exception, Ignorable) and exception.should_be_ignored:
                    print(f"Skipping message with exception type {type(exception)}: {exception}")
                    continue
                else:
                    raise exception
            self.send(cast(MessageType, Utility.deep_get(execution_result, "message")), config["namespace"])

    @abstractmethod
    def send(self, message: MessageType, namespace: str) -> None:
        pass
