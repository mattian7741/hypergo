import inspect
import json
import logging
from functools import wraps
from typing import Any, Callable, Dict, cast

from hypergo.executor import Executor
from hypergo.loggers.base_logger import BaseLogger as Logger
from hypergo.utility import find_class_instance

CALLER_DEPTH: int = 10


class JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        stack_item = inspect.stack()[CALLER_DEPTH]
        log_data: Dict[str, Any] = {
            "timestamp": self.formatTime(record),
            "name": f"""{stack_item.filename}:{stack_item.lineno} {stack_item.function}""",
            "level": record.levelname,
            "message": record.msg,
        }
        return json.dumps(log_data)


logger: Logger = Logger(name="hypergo", log_level=logging.DEBUG, log_format=JSONFormatter())


def function_log(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        executor: Executor = cast(Executor, find_class_instance(Executor, *args, **kwargs))
        function_logger: Logger = cast(Logger, executor.logger) if executor else logger
        function_logger.name = executor.callback.__name__ if executor else f"hypergo.{func.__name__}"
        function_logger.format = JSONFormatter()
        try:
            function_logger.info(f"Invoking function: {function_logger.name}")
            result: Any = func(*args, **kwargs)
            return result
        except Exception as e:
            function_logger.error(f"""Function {function_logger.name} encountered error: {str(e)}""")
            raise e

    return wrapper
