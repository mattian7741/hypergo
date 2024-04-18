from abc import ABC, abstractmethod
from typing import Any, cast

from hypergo.config import ConfigType
from hypergo.executor import Executor
from hypergo.message import MessageType
from hypergo.monitor import collect_metrics

from hypergo.validation import OutputValidationError

from hypergo.utility import Utility


class Connection(ABC):
    def general_consume(self, message: MessageType, **kwargs: Any) -> None:
        config: ConfigType = kwargs.pop("config")
        executor: Executor = Executor(config, **kwargs)
        self.__send_message(executor=executor, message=message, config=config)

    @collect_metrics
    def __send_message(self, executor: Executor, message: MessageType, config: ConfigType) -> None:
        for execution_result in executor.execute(message):
            print(f"in connection execution_result: {execution_result}\n")
            exception = Utility.deep_get(execution_result, 'exception')
            should_continue = Utility.deep_get(execution_result, 'exception').should_continue
            print(f"in connection exception: {should_continue}\n")
            print(f"in connection type: {type(exception)}\n")

            if isinstance(Utility.deep_get(execution_result, "exception"), OutputValidationError):
                exception = Utility.deep_get(execution_result, 'exception')
                if exception.should_continue:
                    print("should continue\n")
                    continue
                else:
                    print("shouldnt continue\n")
                    raise exception
            self.send(cast(MessageType, execution_result), config["namespace"])

    @abstractmethod
    def send(self, message: MessageType, namespace: str) -> None:
        pass
