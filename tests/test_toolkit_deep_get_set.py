import unittest
from typing import List, Dict, Any, Union
import pydash
import glom
from hypergo.hyperdash import Hyperdash as _

class TestFunctions(unittest.TestCase):

    def setUp(self):
        self.test_dict = {
            "key1": "value1",
            "key2": {
                "key3": "value3",
            },
        }
        self.test_list = [1, 2, 3]

    def test_has(self):
        # Test with a dictionary where the key exists.
        self.assertTrue(_.has(self.test_dict, "key1"))
        
        # Test with a dictionary where the key does not exist.
        self.assertFalse(_.has(self.test_dict, "key4"))
        
        # Test with a nested dictionary where the key exists.
        self.assertTrue(_.has(self.test_dict, "key2.key3"))
        
        # Test with a nested dictionary where the key does not exist.
        self.assertFalse(_.has(self.test_dict, "key2.key4"))
        
        # Test with a list where the index exists.
        self.assertTrue(_.has(self.test_list, "0"))
        
        # Test with a list where the index does not exist.
        self.assertFalse(_.has(self.test_list, "3"))
        
        # Test with an empty dictionary.
        self.assertFalse(_.has({}, "key1"))
        
        # Test with an empty list.
        self.assertFalse(_.has([], "0"))

    def test_pop(self):
        # Test popping an existing key from a dictionary.
        self.assertEqual(_.pop(self.test_dict, "key1"), "value1")
        self.assertNotIn("key1", self.test_dict)
        
        # Test popping a non-existing key from a dictionary with the default sentinel.
        self.assertEqual(_.pop(self.test_dict, "key4", "default_value"), "default_value")
        
        # Test popping a non-existing key from a dictionary without the default sentinel (should raise a KeyError).
        with self.assertRaises(KeyError):
            _.pop(self.test_dict, "key4")
        
        # Test popping a nested key from a dictionary.
        self.assertEqual(_.pop(self.test_dict, "key2.key3"), "value3")
        self.assertNotIn("key3", self.test_dict["key2"])
        
        # Test popping a key from a list.
        self.assertEqual(_.pop(self.test_list, "0"), 1)
        self.assertNotIn(1, self.test_list)
        
        # Test popping an index from a list.
        self.assertEqual(_.pop(self.test_list, "1"), 2)
        self.assertNotIn(2, self.test_list)
        
        # Test popping an index that does not exist from a list (should raise an IndexError).
        with self.assertRaises(IndexError):
            _.pop(self.test_list, "3")
        
        # Test popping from an empty dictionary.
        with self.assertRaises(KeyError):
            _.pop({}, "key1")
        
        # Test popping from an empty list.
        with self.assertRaises(IndexError):
            _.pop([], "0")

    def test_get(self):
        # Test getting an existing key from a dictionary.
        self.assertEqual(_.get(self.test_dict, "key1"), "value1")
        
        # Test getting a non-existing key from a dictionary with the default sentinel.
        self.assertEqual(_.get(self.test_dict, "key4", "default_value"), "default_value")
        
        # Test getting a non-existing key from a dictionary without the default sentinel (should raise a KeyError).
        with self.assertRaises(KeyError):
            _.get(self.test_dict, "key4")
        
        # Test getting a nested key from a dictionary.
        self.assertEqual(_.get(self.test_dict, "key2.key3"), "value3")
        
        # Test getting a key from a list.
        self.assertEqual(_.get(self.test_list, "0"), 1)
        
        # Test getting an index from a list.
        self.assertEqual(_.get(self.test_list, "1"), 2)
        
        # Test getting an index that does not exist from a list (should raise an IndexError).
        with self.assertRaises(KeyError):
            _.get(self.test_list, "3")
        
        # Test getting from an empty dictionary.
        with self.assertRaises(KeyError):
            _.get({}, "key1")
        
        # Test getting from an empty list.
        with self.assertRaises(KeyError):
            _.get([], "0")

    def test_set(self):
        # Test setting a new key-value pair in an existing dictionary.
        _.set(self.test_dict, "key4", "value4")
        self.assertIn("key4", self.test_dict)
        self.assertEqual(self.test_dict["key4"], "value4")
        
        # Test setting a new key-value pair in an existing nested dictionary.
        _.set(self.test_dict, "key2.key4", "value4")
        self.assertIn("key4", self.test_dict["key2"])
        self.assertEqual(self.test_dict["key2"]["key4"], "value4")
        
        # Test setting a value for an existing key in a dictionary.
        _.set(self.test_dict, "key1", "new_value")
        self.assertEqual(self.test_dict["key1"], "new_value")
        
        # Test setting a value for an existing nested key in a dictionary.
        _.set(self.test_dict, "key2.key3", "new_value")
        self.assertEqual(self.test_dict["key2"]["key3"], "new_value")
        
        # Test setting a value for a new index in a list.
        _.set(self.test_list, "3", 4)
        self.assertRaises(IndexError)
        
        # Test setting a value for an existing index in a list.
        _.set(self.test_list, "0", 0)
        self.assertEqual(self.test_list[0], 0)
        
        # Test setting a value for an index that does not exist in a list (should raise an IndexError).
        with self.assertRaises(IndexError):
            _.set(self.test_list, "10", 10)
        
        # Test setting a value for an empty dictionary.
        _.set({}, "key1", "value1")
        self.assertEqual({}, {"key1": "value1"})
        
        # Test setting a value for an empty list.
        _.set([], "0", 0)
        self.assertEqual([], [0])

if __name__ == "__main__":
    unittest.main()
