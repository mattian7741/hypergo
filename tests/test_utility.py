import os
import sys
import unittest
from unittest.mock import MagicMock

from typing import Dict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.utility import Utility

import json
import yaml


class TestUtility(unittest.TestCase):
    def test_deep_get(self) -> None:
        # Test when key exists in dictionary
        input_dict: Dict[str, Dict[str, int]] = {"a": {"b": 1}}
        key: str = "a.b"
        expected_output: int = 1
        self.assertEqual(Utility.deep_get(input_dict, key), expected_output)

        # Test for keys with . and no nested structure
        input_dict: Dict[str, Dict[str, int]] = {"a.b": 1}
        key: str = "a.b"
        expected_output: int = 1
        self.assertEqual(Utility.deep_get(input_dict, key.replace('.', '\\.')), expected_output)

        # Test when key does not exist in dictionary
        input_dict: Dict[str, Dict[str, int]] = {"a": {"b": 1}}
        key: str = "a.c"
        with self.assertRaises(KeyError):
            Utility.deep_get(input_dict, key)

    def test_deep_set(self) -> None:
        # Test when key exists in dictionary
        input_dict: Dict[str, Dict[str, int]] = {"a": {"b": 1}}
        key: str = "a.b"
        val: int = 2
        expected_output: Dict[str, Dict[str, int]] = {"a": {"b": 2}}
        Utility.deep_set(input_dict, key, val)
        self.assertEqual(input_dict, expected_output)

        # Test when key does not exist in dictionary
        input_dict: Dict[str, Dict[str, int]] = {"a": {"b": 1}}
        key: str = "a.c"
        val: int = 2
        expected_output: Dict[str, Dict[str, int]] = {"a": {"b": 1, "c": 2}}
        Utility.deep_set(input_dict, key, val)
        self.assertEqual(input_dict, expected_output)

    def test_yaml_read(self) -> None:
        # Mock the yaml.safe_load method
        yaml.safe_load = MagicMock(return_value={"a": 1})
        file_name: str = "tests/test.yaml"
        expected_output: Dict[str, int] = {"a": 1}
        self.assertEqual(Utility.yaml_read(file_name), expected_output)

    def test_yaml_write(self) -> None:
        # TODO: Implement test case for yaml_write method
        pass

    def test_json_read(self) -> None:
        # Mock the json.load method
        json.load = MagicMock(return_value={"a": 1})
        file_name: str = "tests/test.json"
        expected_output: Dict[str, int] = {"a": 1}
        self.assertEqual(Utility.json_read(file_name), expected_output)

    def test_json_write(self) -> None:
        # TODO: Implement test case for json_write method
        pass

    def test_hash(self) -> None:
        content: str = "test content"
        expected_output: str = "9473fdd0d880a43c21b7778d34872157"
        self.assertEqual(Utility.hash(content), expected_output)

    def test_safecast(self) -> None:
        # Test for int type
        expected_output: int = 1
        provided_value: str = "1"
        value_type: type = int
        self.assertEqual(Utility.safecast(value_type, provided_value), expected_output)

        # Test for float type
        expected_output: float = 1.0
        provided_value: int = 1
        value_type: type = float
        self.assertEqual(Utility.safecast(value_type, provided_value), expected_output)

        # Test for str type
        expected_output: str = "1"
        provided_value: int = 1
        value_type: type = str
        self.assertEqual(Utility.safecast(value_type, provided_value), expected_output)

        # Test for ABC.Meta type
        from abc import ABC
        class TestClass(ABC):
            pass
        class TestSubClass(TestClass):
            pass
        test_sub_class: TestSubClass = TestSubClass()
        expected_output: TestClass = test_sub_class
        provided_value: TestSubClass = test_sub_class
        value_type: ABC.Meta = TestClass
        self.assertEqual(Utility.safecast(value_type, provided_value), expected_output)
        
if __name__ == '__main__':
    # Run the unit tests
    unittest.main()