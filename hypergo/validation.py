from typing import Any

from jsonschema import validate

from hypergo.utility import Utility

@staticmethod
def validate_input(data: Any, key: str) -> Any:
    input_schema = Utility.deep_get(data, "config.input_schema", None)
    if input_schema:
        validate(Utility.deep_get(data, "message.body"), input_schema)

    return data


@staticmethod
def validate_output(data: Any, key: str) -> Any:
    output_schema = Utility.deep_get(data, "config.output_schema", None)

    if output_schema:
        validate(Utility.deep_get(data, "message.body"), output_schema)

    return data
