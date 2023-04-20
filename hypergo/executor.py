import importlib
import inspect
import re
from typing import Any, Callable, Generator, List, Mapping, cast, get_origin

from hypergo.config import ConfigType
from hypergo.context import ContextType
from hypergo.message import MessageType
from hypergo.utility import Utility


class Executor:
    @staticmethod
    def func_spec(fn_name: str) -> Callable[..., Any]:
        tokens: List[str] = fn_name.split(".")
        return cast(Callable[..., Any], (getattr(importlib.import_module(".".join(tokens[:-1])), tokens[-1])))

    @staticmethod
    def arg_spec(func: Callable[..., Any]) -> List[type]:
        params: Mapping[str, inspect.Parameter] = inspect.signature(func).parameters
        return [params[k].annotation for k in list(params.keys())]

    def __init__(self, config: ConfigType) -> None:
        self._config: ConfigType = config
        self._func_spec: Callable[..., Any] = Executor.func_spec(config["lib_func"])
        self._arg_spec: List[type] = Executor.arg_spec(self._func_spec)

    def get_args(self, context: ContextType) -> List[Any]:
        args: List[Any] = []

        def safecast(some_type: type) -> Callable[..., Any]:
            if some_type == inspect.Parameter.empty:
                return lambda value: value
            return get_origin(some_type) or some_type

        for arg, argtype in zip(self._config["input_bindings"], self._arg_spec):
            # determine if arg binding is a literal denoted by '<literal>'
            val: Any = ((match := re.match(r"'(.*)'", arg)) and match.group(1)) or Utility.deep_get(context, arg)

            if argtype == inspect.Parameter.empty:  # inspect._empty:
                args.append(val)
            else:
                args.append(safecast(argtype)(val))

        return args

    def execute(self, input_message: MessageType) -> Generator[MessageType, None, None]:
        context: ContextType = {"message": input_message, "config": self._config}
        args: List[Any] = self.get_args(context)
        execution: Any = self._func_spec(*args)
        return_values: List[Any] = list(execution) if inspect.isgenerator(execution) else [execution]

        for return_value in return_values:
            output_message: MessageType = {"routingkey": self.organize_tokens(self._config["output_keys"]), "body": {}}
            output_context: ContextType = {"message": output_message, "config": self._config}

            def handle_tuple(dst: ContextType, src: Any) -> None:
                for binding, tuple_elem in zip(self._config["output_bindings"], src):
                    Utility.deep_set(dst, binding, tuple_elem)

            def handle_default(dst: ContextType, src: Any) -> None:
                for binding in self._config["output_bindings"]:
                    Utility.deep_set(dst, binding, src)

            def handle_list(dst: ContextType, src: Any) -> None:
                for binding in self._config["output_bindings"]:
                    # src[:3] is a debugging hack !!REMOVE!!!
                    Utility.deep_set(dst, binding, src[:3])

            if isinstance(return_value, tuple):
                handle_tuple(output_context, return_value)
            elif isinstance(return_value, list):
                handle_list(output_context, return_value)
            else:
                handle_default(output_context, return_value)

            yield output_message

    def organize_tokens(self, keys: List[str]) -> str:
        return ".".join(sorted(set(".".join(keys).split("."))))
