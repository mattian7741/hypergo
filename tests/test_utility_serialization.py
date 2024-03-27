import os
import sys
import unittest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.utility import Utility


class TestUtility(unittest.TestCase):

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
                self.assertEqual(Utility.deserialize(Utility.serialize(py_value, None), None), json_value)

    def test_nested_dicts_and_lists(self):
        # Test nested dicts and lists
        test_dict = {
            "name": "John",
            "age": 30,
            "address": {
                "city": "New York",
                "zip_code": 10001
            },
            "hobbies": ["Reading", "Hiking"]
        }

        json_dict = Utility.serialize(test_dict, None)
        restored_dict = Utility.deserialize(json_dict, None)
        self.assertEqual(restored_dict, test_dict)

    def test_singular_function(self):
        # Test a singular function
        def add(x, y):
            return x + y

        json_func = Utility.serialize(add, None)
        restored_func = Utility.deserialize(json_func, None)
        self.assertEqual(restored_func(3, 4), 7)

    def test_singular_class(self):
        # Test a singular class
        class TestClass:
            def __init__(self, name):
                self.name = name

            def greet(self):
                return f"Hello, {self.name}!"

        instance = TestClass("Alice")
        json_instance = Utility.serialize(instance, None)
        restored_instance = Utility.deserialize(json_instance, None)

        self.assertEqual(restored_instance.greet(), "Hello, Alice!")

    # New test method using the fixture function
    def test_comprehensive_dict(self):
        # Load the comprehensive testing dictionary from the fixture
        comprehensive_dict = get_fixture()

        # Test serialization and deserialization of the comprehensive dictionary
        json_comprehensive = Utility.serialize(comprehensive_dict, None)
        restored_comprehensive = Utility.deserialize(json_comprehensive, None)
        self.assertEqual(restored_comprehensive, comprehensive_dict)


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
        # "function": sample_function,  # Sample function
        # "class": SampleClass,  # Sample class
        # "instance": sample_instance,  # Sample instance of a class
        "binary_data": binary_data,  # Sample binary data
        "bytes": b"hello",  # Sample bytes
    }

if __name__ == "__main__":
    unittest.main()
