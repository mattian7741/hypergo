import unittest
from unittest.mock import Mock, patch

from hypergo.validation import validation

from jsonschema import ValidationError


class TestValidation(unittest.TestCase):
    @validation
    @staticmethod
    def _test_func(self, data):
        message_body = data["message"]["body"]["some"]
        data["message"]["body"]["some"] = f"modified {message_body}"
        return data
    
    @patch("jsonschema.validate")
    def test_no_validation(self, mock_validate) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {}
        }
        result_data = TestValidation._test_func(Mock(), data)

        mock_validate.assert_not_called()

        self.assertDictEqual(
            result_data,
            {
                "message": {
                    "body": {
                        "some": "modified data"
                    }
                },
                "config": {}
            }
        )

    def test_happy_path(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "input_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"some": {"type": "string"}},
                        "required": ["some"]
                    },
                },
                "output_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"some": {"type": "string"}},
                        "required": ["some"]
                    }
                }
            }
        }
        result_data = TestValidation._test_func(Mock(), data)

        print(type(result_data))
        print(result_data)

        self.assertDictEqual(
            result_data,
            {
                "message": {
                    "body": {
                        "some": "modified data"
                    }
                },
                "config": {
                    "input_validation": {
                        "schema": {
                            "$schema": "http://json-schema.org/draft-04/schema#",
                            "type": "object",
                            "properties": {"some": {"type": "string"}},
                            "required": ["some"]
                        },
                    },
                    "output_validation": {
                        "schema": {
                            "$schema": "http://json-schema.org/draft-04/schema#",
                            "type": "object",
                            "properties": {"some": {"type": "string"}},
                            "required": ["some"]
                        }
                    }
                }
            }
        )

    def test_bad_input(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": -1
                }
            },
            "config": {
                "input_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"some": {"type": "string"}},
                        "required": ["some"]
                    },
                },
                "output_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"some": {"type": "string"}},
                        "required": ["some"]
                    }
                }
            }
        }

        with self.assertRaises(ValidationError) as assert_context:
            TestValidation._test_func(Mock(), data)

    def test_bad_output(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "input_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"some": {"type": "string"}},
                        "required": ["some"]
                    },
                },
                "output_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"missing": {"type": "string"}},
                        "required": ["missing"]
                    }
                }
            }
        }

        with self.assertRaises(ValidationError) as error:
            TestValidation._test_func(Mock(), data)

                
if __name__ == '__main__':
    # Run the unit tests
    unittest.main()