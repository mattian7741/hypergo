import unittest

import mock
from jsonschema import ValidationError

from hypergo.config import ConfigType
from hypergo.executor import Executor


class TestExecutor(unittest.TestCase):
    def _get_standard_configs(self) -> None:
        return ConfigType({
            "namespace": "datalink",
            "name": "testappliance",
            "package": "test-package",
            "lib_func": "testappliance.__main__.test",
            "input_keys": ["testi.boi.input"],
            "output_keys": ["testi.boi.output"],
            "input_bindings": ["message.body.testinput"],
            "output_bindings": ["message.body.testoutput"],
            "input_operations": [],
            "input_json_schema": {
                "$schema": "http://json-schema.org/draft-04/schema#",
                "type": "object",
                "properties": {
                    "string_field": {
                        "type": "string"
                    },
                    "int_field": {
                        "type": "integer"
                    },
                    "nested_field": {
                        "type": "object",
                        "properties": {
                            "nested_string_field": {
                                "type": "string"
                            }
                        },
                        "required": ["nested_string_field"]
                    },
                    "array_field": {
                        "type": "array",
                        "items": [
                            {
                                "type": "string"
                            }
                        ]
                    },
                    "not_required_field": {
                        "type": "string"
                    }
                },
                "required": [
                    "string_field",
                    "int_field",
                    "nested_field",
                    "array_field"
                ]
            },
            "output_json_schema": {
                "type": "object",
                "properties": {
                    "string_field": {
                        "type": "string"
                    },
                    "int_field": {
                        "type": "integer"
                    },
                    "nested_field": {
                        "type": "object",
                        "properties": {
                            "nested_string_field": {
                                "type": "string"
                            }
                        },
                        "required": ["nested_string_field"]
                    },
                    "array_field": {
                        "type": "array",
                        "items": [
                            {
                                "type": "string"
                            }
                        ]
                    },
                    "not_required_field": {
                        "type": "string"
                    }
                },
                "required": [
                    "string_field",
                    "int_field",
                    "nested_field",
                    "array_field"
                ]
            }
        })

    def test_input_json_schema_happy_path(self) -> None:
        with (
                mock.patch.object(Executor, 'func_spec'),
                mock.patch.object(Executor, 'open_envelope')
            ):
            config = self._get_standard_configs()
            # We're not testing the output schema here - just get rid of it
            config.pop("output_json_schema")

            executor = Executor(config)
            input_envelope = mock.MagicMock()
            input_envelope.body = {
                "string_field": "some_string",
                "int_field": 1,
                "nested_field": {
                    "nested_string_field": "some_string_2"
                },
                "array_field": ["array_string"],
                "extra_field": "extra_value"
            }
            next(executor.execute(input_envelope))


    def test_input_json_schema_bad_input(self) -> None:
        with (
                mock.patch.object(Executor, 'func_spec'),
                mock.patch.object(Executor, 'open_envelope')
            ):
            config = self._get_standard_configs()
            # We're not testing the output schema here - just get rid of it
            config.pop("output_json_schema")

            executor = Executor(config)
            input_envelope = mock.MagicMock()
            input_envelope.body = {
                "int_field": 1,
                "nested_field": {
                    "nested_string_field": "some_string_2"
                },
                "array_field": ["array_string"],
                "extra_field": "extra_value"
            }
            with self.assertRaises(ValidationError):
                next(executor.execute(input_envelope))
            

if __name__ == '__main__':
    unittest.main()
