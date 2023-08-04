
from typing import List, Dict, Any, Union, Optional, Callable, Generator, Tuple
import pydash
import glom
from functools import wraps
import inspect
import re

JsonRootStruct = Union[Dict[str, Any], List[Any]]

class Hyperdash:
    @staticmethod
    def has(dic: JsonRootStruct, key: str) -> bool:
        return pydash.has(dic, key)

    @staticmethod
    def get(dic: JsonRootStruct, key: str, default_sentinel: Optional[Any] = object) -> Any:
        if not pydash.has(dic, key) and default_sentinel == object:
            raise KeyError(f"Spec \"{key}\" not found in the dictionary {str(dic)}")
        return pydash.get(dic, key, default_sentinel)

    @staticmethod
    def set(dic: JsonRootStruct, key: str, val: Any) -> None:
        glom.assign(dic, key, val, missing=dict)



def sample_function(a, b, c):
    return a + b / c

_ = Hyperdash


# def traverse_datastructures(func: Callable[..., Any]) -> Callable[..., Any]:
#     @wraps(func)
#     def wrapper(value: Any, *args: Tuple[Any, ...]) -> Any:
#         handlers: Dict[type, Callable[[Any], Any]] = {
#             dict: lambda _dict, *args: {wrapper(key, *args): wrapper(val, *args) for key, val in _dict.items()},
#             list: lambda _list, *args: [wrapper(item, *args) for item in _list],
#             tuple: lambda _tuple, *args: tuple(wrapper(item, *args) for item in _tuple),
#         }
#         return handlers.get(type(value), func)(value, *args)

#     return wrapper


# def do_question_mark(input_string: str) -> str:
#     def find_best_key(field_path: List[str], routingkey: str) -> str:
#         rk_set: Set[str] = set(routingkey.split("."))
#         matched_key: str = ""
#         maxlen: int = 0
#         for key in Utility.deep_get(context, ".".join(field_path)):
#             key_set: Set[str] = set(key.split("."))
#             if key_set.intersection(rk_set) == key_set and len(key_set) > maxlen:
#                 maxlen = len(key_set)
#                 matched_key = key
#         return re.sub(r"\.", "\\.", matched_key) or "?"

#     node_path: List[str] = []
#     for node in input_string.split("."):
#         node_path.append(
#             find_best_key(node_path, Utility.deep_get(context, "message.routingkey")) if node == "?" else node
#         )
#     return ".".join(node_path)

# def do_substitution(value: Any, data: Dict[str, Any]) -> Any:
#     @traverse_datastructures
#     def substitute(string: str, data: Dict[str, Any]) -> Any:
#         match: Optional[Match[str]] = re.match(r"^{([^}]+)}$", string)
#         return (
#             Utility.deep_get(data, do_question_mark(match.group(1)), match.group(0))
#             if match
#             else re.sub(
#                 r"{([^}]+)}",
#                 lambda match: str(Utility.deep_get(data, do_question_mark(match.group(1)), match.group(0))),
#                 string,
#             )
#         )

#     return substitute(value, data)


def traverse_datastructures(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self, *args, **kwargs) -> Any:
        
        value = args[0]

        handlers: Dict[type, Callable[[Any], Any]] = {
            dict: lambda self, _dict, *args: {wrapper(self, key, *args[1:]): wrapper(self, val, *args[1:]) for key, val in _dict.items()},
            list: lambda self, _list, *args: [wrapper(self, item, *args[1:]) for item in _list],
            tuple: lambda self, _tuple, *args: tuple(wrapper(self, item, *args[1:]) for item in _tuple),
        }
        fn = handlers.get(type(value), func)
        ret = fn(self, value, *args[1:])
        return ret

    return wrapper

def bind_arguments(func):
    @wraps(func)
    @traverse_datastructures
    def wrapper(self, *args, **kwargs):
        # print(args[0])
        binding = _.get(args, "0.config.input_bindings.0")
        match: Optional[Match[str]] = re.match(r"^{([^}]+)}$", binding)
        param2 = _.get(args[0], match.group(1), match.group(0)) if match else re.sub(r"{([^}]+)}", lambda match: str(_.get(args[0], match.group(1), match.group(0))), binding)

        # print(param2)

        params = [1, 2, 3]
        return func(self, *params, **kwargs)
    return wrapper
 
class Function:
    def __init__(self, invoke):
        self._invoke: Generator[Any, None, None] = self.generatorize(invoke)

    def generatorize(self, func):
        def generator_function(*args: Tuple[Any, ...], **kwargs) -> Generator[Any, None, None]:
            result: Any = func(*args, **kwargs)
            return result if inspect.isgenerator(result) else [result]
        return generator_function

    @bind_arguments
    def execute(self, *args, **kwargs) -> Generator[Any, None, None]:
        yield from self._invoke(*args, **kwargs)
        

f = Function(sample_function)
for x in f.execute({
    "config": {
        "input_bindings": [{"some_key": "_ _{message.x}_ _{message.z}_ _"}, "{message.y}", "{message.z}"]
    },
    "message": {
        "x": 5,
        "y": 7,
        "z": 11
    },
}):
    print(x)

