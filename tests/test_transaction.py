import os
import unittest
from typing import Any, Dict, List

from hypergo.transaction import Transaction

class TestTransaction(unittest.TestCase):
    def setUp(self) -> None:
        self._transaction: Transaction = Transaction()

    def test_init_with_parent(self) -> None:
        parent_data: Dict[str, Any] = {"name": "John", "age": 30}
        parent_transaction: Transaction = Transaction(data=parent_data)

        child_data: Dict[str, Any] = {"age": 31, "city": "New York"}
        child_transaction: Transaction = Transaction(parent_transaction, child_data)

        self.assertEqual(child_transaction._parent, parent_transaction)
        self.assertEqual(child_transaction._data, child_data)

    def test_init_without_parent(self) -> None:
        data: Dict[str, Any] = {"name": "John", "age": 30}
        transaction: Transaction = Transaction(data=data)

        self.assertIsNone(transaction._parent)
        self.assertEqual(transaction._data, data)

    def test_push(self) -> None:
        result: Transaction = self._transaction.push()
        self.assertEqual(result._parent, self._transaction)

    def test_pop(self) -> None:
        child_transaction: Transaction = Transaction()
        child_transaction._parent = self._transaction

        result: Transaction = child_transaction.pop()
        self.assertEqual(result, self._transaction)

    def test_set(self) -> None:
        pass

    def test_set(self) -> None:
        data: Dict[str, Any] = {"name": "John", "age": 30}
        transaction: Transaction = Transaction(data=data)

        # Set a new key-value pair
        transaction.set("city", "New York")
        self.assertEqual(transaction.get("city"), "New York")

        # Update an existing key's value
        transaction.set("age", 31)
        self.assertEqual(transaction.get("age"), 31)

        # Set a nested key-value pair
        transaction.set("address.street", "123 Main St")
        self.assertEqual(transaction.get("address.street"), "123 Main St")

    def test_get(self) -> None:
        data: Dict[str, Any] = {"name": "John", "age": 30}
        transaction: Transaction = Transaction(data=data)

        # Get an existing key's value
        self.assertEqual(transaction.get("name"), "John")
        self.assertEqual(transaction.get("age"), 30)

        # Get a nested key's value
        transaction.set("address.street", "123 Main St")
        self.assertEqual(transaction.get("address.street"), "123 Main St")

        # Get a non-existent key's value
        self.assertIsNone(transaction.get("city"))

        # Get a non-existent nested key's value
        self.assertIsNone(transaction.get("address.city"))

    def test_lineage_with_parent(self) -> None:
        parent_data: Dict[str, Any] = {"name": "John", "age": 30}
        parent_transaction: Transaction = Transaction(data=parent_data)

        child_transaction: Transaction = parent_transaction.push()

        lineage: List[str] = child_transaction.lineage()
        self.assertEqual(lineage, [child_transaction._id, parent_transaction._id])

    def test_lineage_without_parent(self) -> None:
        data: Dict[str, Any] = {"name": "John", "age": 30}
        transaction: Transaction = Transaction(data=data)

        lineage: List[str] = transaction.lineage()

        self.assertEqual(lineage, [transaction._id])

if __name__ == '__main__':
    unittest.main()
