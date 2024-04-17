from functools import wraps
from typing import Any, Callable

import jsonschema

from hypergo.utility import Utility


def validation(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self: Any, data: Any) -> Any:
        print(f"in validation self: {self} data: {data}\n\n")

        input_validation = Utility.deep_get(data, "config.input_validation", None)
        if input_validation:
            jsonschema.validate(Utility.deep_get(data, "message.body"), input_validation["schema"])

        result = func(self, data)

        output_validation = Utility.deep_get(data, "config.output_validation", None)
        if output_validation:
            jsonschema.validate(Utility.deep_get(data, "message.body"), output_validation["schema"])

        return result

    return wrapper
