from functools import wraps
from typing import Any, Callable

from jsonschema import ValidationError, validate

from hypergo.utility import Utility


class OutputValidationError(ValidationError):
    def __init__(self, should_continue=False):
        self.should_continue = should_continue


def validation(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(self: Any, data: Any) -> Any:
        print(f"in validation self: {self} data: {data}\n\n")

        input_validation = Utility.deep_get(data, "config.input_validation", None)
        if input_validation:
            validate(Utility.deep_get(data, "message.body"), input_validation["schema"])

        result = func(self, data)

        output_validation = Utility.deep_get(data, "config.output_validation", None)
        if output_validation:
            try:
                validate(Utility.deep_get(data, "message.body"), output_validation["schema"])
            except ValidationError:
                return OutputValidationError(
                    output_validation["skip_if_invalid"] if "skip_if_invalid" in output_validation else False
                )

        return result

    return wrapper
