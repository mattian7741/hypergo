import unittest
from unittest.mock import Mock

from hypergo.validation import validation

from hypergo.config import ConfigType


class TestValidation(unittest.TestCase):
    @validation
    @staticmethod
    def _test_func(data):
        message_body = data["message"]["body"]["some"]
        data["message"]["body"]["some"] = f"modified {message_body}"
        return data

    def test_happy_path(self) -> None:
        data = {
             "message": {
                  "body": {
                       "some": "data"
                  }
             },
             "config": {
                "input_json_schema": {"$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"some": {"type": "string"}},
                    "required": ["some"]
                },
                "output_json_schema": {"$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"some": {"type": "string"}},
                    "required": ["some"]
                }
            }
        }
        result_generator = self._test_func(Mock(), data)
        result_data = next(result_generator)

        self.assertDictEqual(
             result_data,
             {
                {
                    "message": {
                        "body": {
                            "some": "modified data"
                        }
                    },
                    "config": {
                        "input_json_schema": {
                            "$schema": "http://json-schema.org/draft-04/schema#",
                            "type": "object",
                            "properties": {"some": {"type": "string"}},
                            "required": ["some"]},
                        "output_json_schema": {"$schema": "http://json-schema.org/draft-04/schema#",
                            "type": "object",
                            "properties": {"some": {"type": "string"}},
                            "required": ["some"]}
                    }
                }
             }
        )
