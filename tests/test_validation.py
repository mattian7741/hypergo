import unittest
from unittest.mock import patch
from jsonschema.exceptions import ValidationError

from hypergo.validation import validate_input, validate_output


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

    def test_valid_data(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "input_schema": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"some": {"type": "string"}},
                    "required": ["some"]
                },
            }
        }
        result = validate_input(data, None)

        self.assertDictEqual(data, result)

    def test_invalid_data(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "input_schema": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"missing": {"type": "string"}},
                    "required": ["missing"]
                },
            }
        }

        with self.assertRaises(ValidationError):
            validate_input(data, None)


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

    def test_valid_data(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "output_schema": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"some": {"type": "string"}},
                    "required": ["some"]
                },
            }
        }
        result = validate_output(data, None)

        self.assertDictEqual(data, result)

    def test_invalid_data(self) -> None:
        data = {
            "message": {
                "body": {
                    "some": "data"
                }
            },
            "config": {
                "output_schema": {
                    "$schema": "http://json-schema.org/draft-04/schema#",
                    "type": "object",
                    "properties": {"missing": {"type": "string"}},
                    "required": ["missing"]
                }
            }
        }

        with self.assertRaises(ValidationError):
            validate_output(data, None)

if __name__ == '__main__':
    # Run the unit tests
    unittest.main()