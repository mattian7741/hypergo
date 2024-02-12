import os
import sys
import unittest
from typing import Dict
from unittest.mock import MagicMock

import hypergo.hyperdash as _

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import json

import yaml


class TestSerialization(unittest.TestCase):
    def test_scalar_values(self):
        # Test scalar values (str, int, float, bool, None)
        test_values = [
            ("Hello, World!", "Hello, World!"),
            (42, 42),
            (3.14, 3.14),
            (True, True),
            (None, None),
        ]
        for py_value, json_value in test_values:
            with self.subTest(py_value=py_value):
                self.assertEqual(
                    _.deserialize(_.serialize(py_value, None), None), json_value
                )

    def test_nested_dicts_and_lists(self):
        # Test nested dicts and lists
        test_dict = {
            "name": "John",
            "age": 30,
            "address": {"city": "New York", "zip_code": 10001},
            "hobbies": ["Reading", "Hiking"],
        }

        json_dict = _.serialize(test_dict, None)
        restored_dict = _.deserialize(json_dict, None)
        self.assertEqual(restored_dict, test_dict)

    def test_singular_function(self):
        # Test a singular function
        def add(x, y):
            return x + y

        json_func = _.serialize(add, None)
        restored_func = _.deserialize(json_func, None)
        self.assertEqual(restored_func(3, 4), 7)

    def test_singular_class(self):
        # Test a singular class
        class TestClass:
            def __init__(self, name):
                self.name = name

            def greet(self):
                return f"Hello, {self.name}!"

        instance = TestClass("Alice")
        json_instance = _.serialize(instance, None)
        restored_instance = _.deserialize(json_instance, None)

        self.assertEqual(restored_instance.greet(), "Hello, Alice!")

    # New test method using the fixture function
    def test_comprehensive_dict(self):
        # Load the comprehensive testing dictionary from the fixture
        comprehensive_dict = get_fixture()

        # Test serialization and deserialization of the comprehensive dictionary
        json_comprehensive = _.serialize(comprehensive_dict, None)
        restored_comprehensive = _.deserialize(json_comprehensive, None)

        self.assertEqual(
            comprehensive_dict["function"](4), restored_comprehensive["function"](4)
        )

        comprehensive_dict.pop("function")
        restored_comprehensive.pop("function")

        comprehensive_instance = comprehensive_dict["instance"]
        restored_instance = restored_comprehensive["instance"]

        self.assertEqual(comprehensive_instance.greet(), restored_instance.greet())

        comprehensive_dict.pop("instance")
        restored_comprehensive.pop("instance")

        comprehensive_instantiated_class_instance = comprehensive_dict["class"](
            "testiboi"
        )
        restored_instantiated_class_instance = restored_comprehensive["class"](
            "testiboi"
        )

        self.assertEqual(
            comprehensive_instantiated_class_instance.greet(),
            restored_instantiated_class_instance.greet(),
        )

        comprehensive_dict.pop("class")
        restored_comprehensive.pop("class")

        self.assertDictEqual(restored_comprehensive, comprehensive_dict)


def get_fixture():
    # Importing required modules for binary data and bytes
    import os

    # Sample function
    def sample_function(x):
        return x * x

    # Sample class
    class SampleClass:
        def __init__(self, name):
            self.name = name

        def greet(self):
            return f"Hello, {self.name}!"

    # Sample instance of a class
    sample_instance = SampleClass("Alice")

    # Sample binary data
    binary_data = os.urandom(10)

    # Expanded comprehensive dictionary
    return {
        "string": "Hello, world!",
        "integer": 42,
        "float": 3.14,
        "boolean": True,
        "list": [1, 2, 3, 4, 5],
        "tuple": (6, 7, 8),
        "set": {9, 10, 11},
        "dictionary": {
            "name": "John Doe",
            "age": 30,
            "address": {
                "street": "123 Main St",
                "city": "New York",
                "zip_code": "10001",
            },
        },
        "nested_list": [
            12,
            "nested string",
            [13, 14, 15],
            {"key": "value"},
            [{"nested_key": "nested_value"}],
        ],
        "nested_dict": {
            "key1": "value1",
            "key2": {
                "inner_key1": 16,
                "inner_key2": [17, 18, 19],
                "inner_key3": {
                    "inner_inner_key": "nested value",
                    "inner_inner_list": [20, 21, 22],
                },
            },
        },
        "empty_dict": {},
        "empty_list": [],
        "empty_set": set(),
        "empty_string": "",
        "none_value": None,
        "function": sample_function,  # Sample function
        "class": SampleClass,  # Sample class
        "instance": sample_instance,  # Sample instance of a class
        "binary_data": binary_data,  # Sample binary data
        "bytes": b"hello",  # Sample bytes
    }


class TestDeepGet(unittest.TestCase):
    def test_key_exists(self) -> None:
        input_dict: Dict[str, Dict[str, int]] = {"abc": {"def": 1}}
        key: str = "abc.def"
        expected_output: int = 1
        self.assertEqual(_.deep_get(input_dict, key), expected_output)

    def test_periods(self) -> None:
        input_dict: Dict[str, Dict[str, int]] = {"abc.def": 1}
        key: str = "abc.def"
        expected_output: int = 1
        self.assertEqual(
            _.deep_get(input_dict, key.replace(".", "\\.")), expected_output
        )

    def test_key_does_not_exist(self) -> None:
        input_dict: Dict[str, Dict[str, int]] = {"abc": {"def": 1}}
        key: str = "abc.ghi"
        with self.assertRaises(KeyError):
            _.deep_get(input_dict, key)


class TestDeepSet(unittest.TestCase):
    def test_key_exists(self) -> None:
        input_dict: Dict[str, Dict[str, int]] = {"abc": {"def": 1}}
        key: str = "abc.def"
        val: int = 2
        expected_output: Dict[str, Dict[str, int]] = {"abc": {"def": 2}}
        _.deep_set(input_dict, key, val)
        self.assertDictEqual(input_dict, expected_output)

    def test_key_does_not_exist(self) -> None:
        input_dict: Dict[str, Dict[str, int]] = {"abc": {"def": 1}}
        key: str = "abc.ghi"
        val: int = 2
        expected_output: Dict[str, Dict[str, int]] = {"abc": {"def": 1, "ghi": 2}}
        _.deep_set(input_dict, key, val)
        self.assertDictEqual(input_dict, expected_output)

    def test_deep_key(self) -> None:
        input_dict: Dict[str, Dict[str, int]] = {"abc": {"def": {"ghi": 1}, "jkl": 2}}
        key: str = "abc.def"
        val: int = 3
        expected_output = {"abc": {"jkl": 2, "def": 3}}

        actual_output = _.deep_set(input_dict, key, val)

        self.assertDictEqual(actual_output, expected_output)


class TestYamlRead(unittest.TestCase):
    def test_yaml_read(self) -> None:
        # Mock the yaml.safe_load method
        yaml.safe_load = MagicMock(return_value={"a": 1})
        file_name: str = "tests/test.yaml"
        expected_output: Dict[str, int] = {"a": 1}
        self.assertEqual(_.yaml_read(file_name), expected_output)


class TestYamlWrite(unittest.TestCase):
    def test_yaml_write(self) -> None:
        # TODO: Implement test case for yaml_write method
        pass


class TestJsonRead(unittest.TestCase):
    def test_json_read(self) -> None:
        # Mock the json.load method
        json.load = MagicMock(return_value={"a": 1})
        file_name: str = "tests/test.json"
        expected_output: Dict[str, int] = {"a": 1}
        self.assertEqual(_.json_read(file_name), expected_output)


class TestJsonWrite(unittest.TestCase):
    def test_json_write(self) -> None:
        # TODO: Implement test case for json_write method
        pass


class TestHash(unittest.TestCase):
    def test_hash(self) -> None:
        content: str = "test content"
        expected_output: str = "9473fdd0d880a43c21b7778d34872157"
        self.assertEqual(_.hash(content), expected_output)


class TestSafeCast(unittest.TestCase):
    def test_int(self) -> None:
        expected_output: int = 1
        provided_value: str = "1"
        value_type: type = int
        self.assertEqual(_.safecast(value_type, provided_value), expected_output)

    def test_float(self) -> None:
        expected_output: float = 1.0
        provided_value: int = 1
        value_type: type = float
        self.assertEqual(_.safecast(value_type, provided_value), expected_output)

    def test_str(self) -> None:
        expected_output: str = "1"
        provided_value: int = 1
        value_type: type = str
        self.assertEqual(_.safecast(value_type, provided_value), expected_output)

    def test_meta(self) -> None:
        from abc import ABC

        class TestClass(ABC):
            pass

        class TestSubClass(TestClass):
            pass

        test_sub_class: TestSubClass = TestSubClass()
        expected_output: TestClass = test_sub_class
        provided_value: TestSubClass = test_sub_class
        value_type: ABC.Meta = TestClass
        self.assertEqual(_.safecast(value_type, provided_value), expected_output)


class TestCompress(unittest.TestCase):
    def test_compress_no_key(self):
        result = _.compress({"abc": "def"})

        self.assertEqual(
            result,
            "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQANeyJhYmMiOiAiZGVmIn0AAABJEUiEEamvOAABJg4IG+AEH7bzfQEAAAAABFla",
        )

    def test_compress_with_key(self):
        result = _.compress({"abc": {"def": {"ghi": 1}, "jkl": 2}}, "abc.def")

        self.assertEqual(
            result,
            {
                "abc": {
                    "def": "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQAJeyJnaGkiOiAxfQAAAIMiUr9PqR/CAAEiChUa4WcftvN9AQAAAAAEWVo=",
                    "jkl": 2,
                }
            },
        )


class TestUncompress(unittest.TestCase):
    def test_uncompress_no_key(self):
        result = _.uncompress(
            "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQANeyJhYmMiOiAiZGVmIn0AAABJEUiEEamvOAABJg4IG+AEH7bzfQEAAAAABFla"
        )

        self.assertEqual(result, {"abc": "def"})

    def test_uncompress_with_key(self):
        result = _.uncompress(
            {
                "abc": {
                    "def": "/Td6WFoAAATm1rRGAgAhARYAAAB0L+WjAQAJeyJnaGkiOiAxfQAAAIMiUr9PqR/CAAEiChUa4WcftvN9AQAAAAAEWVo=",
                    "jkl": 2,
                }
            },
            "abc.def",
        )

        self.assertEqual(result, {"abc": {"def": {"ghi": 1}, "jkl": 2}})


if __name__ == "__main__":
    unittest.main()
