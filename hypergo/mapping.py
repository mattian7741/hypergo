from functools import partial
from typing import Any, Dict

from hypergo.utility import Utility, traverse_datastructures


class Mapping:
    def __init__(self, definition: Dict[str, Any]):
        self._definition: Dict[str, Any] = {key: value for key, value in definition.items() if value is not None}

    def map(self, src: Dict[str, Any]) -> Any:
        @traverse_datastructures
        def do_mapping(target: Any, source: Any) -> Any:
            return target(source) if callable(target) else target

        return do_mapping(self._definition, partial(Utility.deep_get, src))
