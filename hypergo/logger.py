import inspect
import json
import logging
from functools import wraps
from typing import Any, Callable, Dict

from hypergo.loggers.base_logger import BaseLogger as Logger

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
    def wrapper(self: Any, data: Any) -> Any:
        function_logger: Logger = self.logger
        function_logger.name = self.callback.__name__
        function_logger.format = JSONFormatter()
        try:
            function_logger.info(f"Invoking function: {function_logger.name}")
            # if func is an instance method of self then func(data) is called.
            # Else it's a decorated function
            result: Any = func(data) if inspect.ismethod(func) and self == func.__self__ else func(self, data)
            return result
        except Exception as e:
            function_logger.error(f"""Function {function_logger.name} encountered error: {str(e)}""")
            raise e

    return wrapper


def sdk_log(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        name: str = f"hypergo.{func.__name__}"
        sdk_logger: Logger = Logger(name=name, log_level=logging.DEBUG, log_format=JSONFormatter())
        try:
            sdk_logger.info(f"Invoking function: {name}")
            result: Any = func(*args, **kwargs)
            sdk_logger.info(f"Function {name} completed successfully")
            return result
        except Exception as e:
            sdk_logger.error(f"""Function {name} encountered error: {str(e)}""")
            raise e

    return wrapper
