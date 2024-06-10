import importlib
import inspect
import re
from functools import wraps
from typing import (Any, Callable, Dict, Generator, List, Mapping, Match,
                    Optional, Set, cast)

from hypergo.config import ConfigType
from hypergo.context import ContextType
from hypergo.local_storage import LocalStorage
from hypergo.loggers.base_logger import BaseLogger as Logger
from hypergo.message import MessageType
from hypergo.secrets import LocalSecrets, Secrets
from hypergo.storage import Storage
from hypergo.transform import Transform
from hypergo.utility import Utility, traverse_datastructures


def do_question_mark(context: Dict[str, Any], input_string: Any) -> str:
    def find_best_key(field_path: List[str], routingkey: str) -> str:
        rk_set: Set[str] = set(routingkey.split("."))
        matched_key: str = ""
        maxlen: int = 0
        for key in Utility.deep_get(context, ".".join(field_path)):
            key_set: Set[str] = set(key.split("."))
            if key_set.intersection(rk_set) == key_set and len(key_set) > maxlen:
                maxlen = len(key_set)
                matched_key = key
        return re.sub(r"\.", "\\.", matched_key)

    node_path: List[str] = []
    for node in input_string.split("."):
        node_path.append(
            find_best_key(node_path, Utility.deep_get(context, "message.routingkey")) if node == "?" else node
        )
    return ".".join(node_path)


def do_substitution(value: Any, data: Dict[str, Any]) -> Any:
    @traverse_datastructures
    def substitute(string: str, data: Dict[str, Any]) -> Any:
        result = string
        if isinstance(string, str):
            matched_regex: Optional[Match[str]] = re.match(r"^{([^}]+)}$", string)
            result = (
                Utility.deep_get(
                    data,
                    do_question_mark(data, matched_regex.group(1)),
                    matched_regex.group(0),
                )
                # version 2.0.0 and above
                if matched_regex
                # backward compatibility
                else re.sub(
                    r"{([^}]+)}",
                    lambda match: str(
                        Utility.deep_get(
                            data,
                            do_question_mark(data, match.group(1)),
                            match.group(0),
                        )
                    ),
                    string,
                )
            )

            # We were substituting message.* in the string with the actual
            # payload
            if re.match(r"^.*\{message\.[^\}]+\}.*$", string):
                return result

        if result != string:
            result = substitute(result, data)
        return result

    return substitute(value, data)


def configsubstitution(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self: Any, data: Any) -> Any:
        input_bindings = Utility.deep_get(self.config, "input_bindings")
        self.config = do_substitution(self.config, {"config": self.config, "message": data})
        Utility.deep_set(self.config, "input_bindings", input_bindings)
        return func(self, data)

    return wrapper


class Executor:
    @staticmethod
    def func_spec(fn_name: str) -> Callable[..., Any]:
        tokens: List[str] = fn_name.split(".")
        return cast(
            Callable[..., Any],
            (getattr(importlib.import_module(".".join(tokens[:-1])), tokens[-1])),
        )

    @staticmethod
    def arg_spec(func: Callable[..., Any]) -> List[type]:
        params: Mapping[str, inspect.Parameter] = inspect.signature(func).parameters
        return [params[k].annotation for k in list(params.keys())]

    def __init__(self, config: ConfigType, **kwargs: Any) -> None:
        self._config: ConfigType = config
        self._func_spec: Callable[..., Any] = Executor.func_spec(config["lib_func"])
        self._arg_spec: List[type] = Executor.arg_spec(self._func_spec)
        self._storage: Optional[Storage] = kwargs.pop("storage", LocalStorage())
        self._secrets: Optional[Secrets] = kwargs.pop("secrets", LocalSecrets())
        self._logger: Optional[Logger] = kwargs.pop("logger", Logger())
        self.__dict__.update(kwargs)

    @property
    def storage(self) -> Optional[Storage]:
        return self._storage

    @property
    def secrets(self) -> Optional[Secrets]:
        return self._secrets

    @property
    def logger(self) -> Optional[Logger]:
        return self._logger

    @property
    def callback(self) -> Callable[..., Any]:
        return self._func_spec

    @property
    def config(self) -> ConfigType:
        return self._config

    @config.setter
    def config(self, config: ConfigType) -> None:
        self._config = config

    def get_args(self, context: ContextType) -> List[Any]:
        return [
            val if argtype == inspect.Parameter.empty else Utility.safecast(argtype, val)
            for val, argtype in zip(
                do_substitution(
                    Utility.deep_get(self.config, "input_bindings"),
                    cast(Dict[str, Any], context),
                ),
                self._arg_spec,
            )
        ]

    def get_output_routing_key(self, input_message_routing_key: str) -> str:
        routing_key_set: Set[str] = set(input_message_routing_key.split("."))
        tokens: List[str] = []
        for input_key in self.config["input_keys"]:
            # hypergo-144 dynamic routing key only for generic components
            # output key will contain context derived from the previous
            # producer routing key
            input_key_set: Set[str] = set(input_key.split("."))
            intersection_set: Set[str] = routing_key_set.intersection(input_key_set)
            # check if the routing key is in the input_key
            if intersection_set == input_key_set:
                # set difference operation to remove the subset of the routing key captured by the component
                # from its input_key and append it to tokens
                tokens.append(".".join(routing_key_set.difference(intersection_set)))
        token: str = self.organize_tokens(tokens)
        output_tokens: List[str] = [
            re.sub(r"(?<=\.)\?(?=\.)|^\?|(?<=\.)\?$|^\?$", token, output_key)
            for output_key in self.config["output_keys"]
        ]
        return self.organize_tokens(output_tokens)

    @configsubstitution
    @Transform.operation("contextualization")
    @Transform.operation("pass_by_reference")
    @Transform.operation("compression")
    @Transform.operation("encryption")
    @Transform.operation("serialization")
    @Transform.operation("transaction")
    def execute(self, context: Any) -> Generator[MessageType, None, None]:
        # This mutates config with substitutions - not necessary for input binding substitution
        # Unclear which approach is better - do we want the original config with references?  Or
        # Do we want to mutate config and replace values with substitutions?
        # This is useful if we want to configure routingkeys with paramaterized values - So
        # We should keep it
        context["config"] = do_substitution(context["config"], cast(Dict[str, Any], context))
        args: List[Any] = self.get_args(context)
        execution: Any = self._func_spec(*args)

        output_routing_key: str = self.get_output_routing_key(Utility.deep_get(context, "message.routingkey"))
        if not inspect.isgenerator(execution):
            execution = [execution]
        for return_value in execution:
            # if not return_value:
            #     continue

            output_message: MessageType = {
                "routingkey": output_routing_key,
                "body": {},
                "transaction": Utility.deep_get(context, "message.transaction"),
                # "__txid__": Utility.deep_get(context, "message.__txid__"),
            }
            output_context: ContextType = {
                "message": output_message,
                "config": self.config,
            }

            def handle_tuple(dst: ContextType, src: Any) -> None:
                for binding, tuple_elem in zip(self.config["output_bindings"], src):
                    Utility.deep_set(dst, binding, tuple_elem)

            def handle_default(dst: ContextType, src: Any) -> None:
                for binding in self.config["output_bindings"]:
                    Utility.deep_set(dst, binding, src)

            if isinstance(return_value, tuple):
                handle_tuple(output_context, return_value)
            else:
                handle_default(output_context, return_value)

            yield output_context

    def organize_tokens(self, keys: List[str]) -> str:
        return ".".join(sorted(set(".".join(keys).split("."))))
