from typing import Union, Optional
import logging


class BaseLogger:
    def __init__(
        self,
        name: Optional[str] = None,
        log_level: int = logging.INFO,
        log_format: Optional[Union[str, logging.Formatter]] = None,
    ):
        self._name: Optional[str] = name
        self.log_level = log_level
        self._format: Union[logging.Formatter, None] = self.__get_formatter(log_format)
        self.logger = logging.getLogger(self.name)
        self.logger.addHandler(self.get_handler())

    def get_handler(self) -> logging.Handler:
        handler: logging.Handler = logging.StreamHandler()
        if self.format:
            handler.setFormatter(self.format)
        return handler

    def log(self, message: str, level: Optional[int] = None, **kwargs) -> None:
        if level is None:
            level = self.log_level
        self.logger.setLevel(level)
        # Log the message with the specified level
        self.logger.log(level, message, **kwargs)

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = name

    @property
    def format(self) -> Union[logging.Formatter, None]:
        return self._format

    @format.setter
    def format(self, formatter: Union[str, logging.Formatter]) -> None:
        self._format = self.__get_formatter(formatter=formatter)

    def __get_formatter(self, formatter: Union[str, logging.Formatter, None]) -> Union[logging.Formatter, None]:
        return logging.Formatter(formatter) if isinstance(formatter, str) else formatter

    def debug(self, message: str, **kwargs) -> None:
        self.log(message, level=logging.DEBUG, **kwargs)

    def info(self, message: str, **kwargs) -> None:
        self.log(f"BEN IM LOGGING in info, kwargs: {kwargs}", level=logging.INFO)
        self.log(message, level=logging.INFO, **kwargs)

    def warning(self, message: str, **kwargs) -> None:
        self.log(message, level=logging.WARNING, **kwargs)

    def error(self, message: str, **kwargs) -> None:
        self.log(message, level=logging.ERROR, **kwargs)

    def critical(self, message: str, **kwargs) -> None:
        self.log(message, level=logging.CRITICAL, **kwargs)
