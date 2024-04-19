import unittest
from unittest.mock import patch

from jsonschema import ValidationError

from hypergo.validation import OutputValidationError, validate_input, validate_output


class TestValidateInput(unittest.TestCase):    
    @patch("hypergo.validation.validate")
    def test_no_validation(self, mock_validate) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {}
        }
        validate_input(data, None)

        mock_validate.assert_not_called()

    @patch("hypergo.validation.validate")
    def test_validate(self, mock_validate) -> None:
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
                }
            }
        }
        validate_input(data, None)

        mock_validate.assert_called_once()


class TestValidateOutput(unittest.TestCase):  
    @patch("hypergo.validation.validate")
    def test_no_validation(self, mock_validate) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {}
        }
        validate_output(data, None)

        mock_validate.assert_not_called()

    def test_bad_input_dont_ignore(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
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

        result = validate_output(data, None)
        self.assertFalse(result["exception"].should_continue)

    def test_bad_input_ignore(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "output_validation": {
                    "schema": {
                        "$schema": "http://json-schema.org/draft-04/schema#",
                        "type": "object",
                        "properties": {"missing": {"type": "string"}},
                        "required": ["missing"]
                    },
                    "skip_if_invalid": True
                }
            }
        }

        result = validate_output(data, None)
        exception = result["exception"]

        self.assertIsInstance(exception, OutputValidationError)
        self.assertTrue(result["exception"].should_continue)

if __name__ == '__main__':
    # Run the unit tests
    unittest.main()