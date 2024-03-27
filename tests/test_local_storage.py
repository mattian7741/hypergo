import os
import sys
import unittest


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.storage import Storage
from hypergo.local_storage import LocalStorage


class TestLocalStorage(unittest.TestCase):
    def setUp(self) -> None:
        self.storage: Storage = LocalStorage()
        self.file_name: str = "test.txt"
        self.file_path = os.path.join(os.path.expanduser('~'), '.hypergo_storage', self.file_name)

    def test_load(self) -> None:
        content: str = "Hello, world!"
        with open(self.file_path, "w", encoding="utf-8") as fp:
            fp.write(content)
        result: str = self.storage.load(self.file_name)
        self.assertEqual(result, content)

    def test_save(self) -> None:
        content: str = "Hello, world!"
        self.storage.save(self.file_name, content)
        with open(self.file_path, "r", encoding="utf-8") as fp:
            result: str = fp.read()
        self.assertEqual(result, content)

    def tearDown(self) -> None:
        if os.path.exists(self.file_path):
            os.remove(self.file_path)


if __name__ == '__main__':
    unittest.main()
