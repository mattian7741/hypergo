from typing import Any

from jsonschema import ValidationError, validate

from hypergo.utility import Utility


class Ignorable:
    def __init__(self, should_be_ignored=False):
        self.should_be_ignored = should_be_ignored


class OutputValidationError(ValidationError, Ignorable):
    def __init__(self, parent_error, should_be_ignored=False):
        super().__init__(parent_error.message)
        Ignorable.__init__(self, should_be_ignored)


@staticmethod
def validate_input(data: Any, key: str) -> Any:
    input_validation = Utility.deep_get(data, "config.input_validation", None)
    if input_validation:
        validate(Utility.deep_get(data, "message.body"), input_validation["schema"])

    return data


@staticmethod
def validate_output(data: Any, key: str) -> Any:
    output_validation = Utility.deep_get(data, "config.output_validation", None)

    if output_validation:
        try:
            validate(Utility.deep_get(data, "message.body"), output_validation["schema"])
        except ValidationError as error:
            Utility.deep_set(
                data,
                "exception",
                OutputValidationError(
                    error, output_validation["skip_if_invalid"] if "skip_if_invalid" in output_validation else False
                ),
            )

    return data
