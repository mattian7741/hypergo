from functools import wraps
from typing import Any

from jsonschema import ValidationError, validate

from hypergo.utility import root_node, Utility


class OutputValidationError(ValidationError):
    def __init__(self, parent_error, should_continue=False):
        print(f"in init. parent: {parent_error.__dict__}\n")
        super().__init__(parent_error.message)
        self.should_continue = should_continue
        print(f"exiting init. self: {self.__dict__}\n")


@staticmethod
def validate_input(data: Any, key: str) -> Any:
    print(f"in validation data: {data}\n\n")

    input_validation = Utility.deep_get(data, "config.input_validation", None)
    if input_validation:
        validate(Utility.deep_get(data, "message.body"), input_validation["schema"])

    return data

@staticmethod
def validate_output(data: Any, key: str) -> Any:
    print(f"in output validation data: {data}\n\n")
    output_validation = Utility.deep_get(data, "config.output_validation", None)

    if output_validation:
        try:
            print(f"output_validation {output_validation}")
            validate(Utility.deep_get(data, "message.body"), output_validation["schema"])
            print("it passed\n")
        except ValidationError as error:
            print("it failed\n")
            Utility.deep_set(
                data,
                "exception",
                OutputValidationError(
                    error,
                    output_validation["skip_if_invalid"] if "skip_if_invalid" in output_validation else False
                )
            )

    return data
