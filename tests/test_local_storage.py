import os
import unittest
from typing import Any

from hypergo.storage import Storage
from hypergo.local_storage import LocalStorage


class TestLocalStorage(unittest.TestCase):
    def setUp(self) -> None:
        self._storage: Storage = LocalStorage()

    def test_load(self) -> None:
        file_name: str = "test.txt"
        content: str = "Hello, world!"
        with open(file_name, "w", encoding="utf-8") as file:
            file.write(content)
        result: str = self._storage.load(file_name)
        self.assertEqual(result, content)

    def test_save(self) -> None:
        file_name: str = "test.txt"
        content: str = "Hello, world!"
        self._storage.save(file_name, content)
        with open(file_name, "r", encoding="utf-8") as file:
            result: str = file.read()
        self.assertEqual(result, content)

    def tearDown(self) -> None:
        file_name: str = "test.txt"
        if os.path.exists(file_name):
            os.remove(file_name)


if __name__ == '__main__':
    unittest.main()
