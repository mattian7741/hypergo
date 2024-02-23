import os
import tempfile
from typing import Any, Callable, Dict
import unittest
from unittest import mock

import hypergo.hyperdash as _
from hypergo.hypertest import (
    ENCRYPTIONKEY,
    encryption,
    exceptions,
    execute,
    fetchbyreference,
    handle_substitution,
    passbyreference,
    replace_wildcard_from_routingkey,
    save_transaction,
    storebyreference,
    transactions,
)
from hypergo.local_storage import LocalStorage
from hypergo.storage import Storage
from hypergo.transaction import Transaction

def test_func(data):
    message_body_in_the_function = data["message"]["body"]

    data["message"]["body"] = f"modified {message_body_in_the_function}"

    yield data


the_config: Dict[str, Any] = {
    "lib_func": "hypergo.hypertest.the_function",
    "input_keys": ["x.y.z", "v.u.w"],
    "output_keys": ["m.n.?"],
    "input_bindings": ["{config.custom_properties.?}", "{message.body.a.x}", "{message.body.a.y}", "{transaction}"],
    "input_bindings": ["{message.body}"],
    "output_bindings": [{"message": {"body": {"p": {"q": {"r": "{output}"}}}}}],
    "custom_properties": {"x": "{message.body.a.fn}", "w": "{message.body.a.c}"},
}

the_storage: Storage = LocalStorage()
the_context: Dict[str, Any] = {"function": test_func, "storage": the_storage, "config": the_config}


class TestBasicCase(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    @mock.patch("hypergo.hypertest.the_function", test_func)
    def test_basic_case(self, mock_test_func):
        message = {
            "routingkey": "x.y.z",
            "body": {"a": {"fn": lambda m, n: (m + n) / (m * n), "x": 23, "y": 41}}
        }
        serialized_message = _.serialize(message, "body")
        compressed_message = _.compress(serialized_message, "body")
        encrypted_message = _.encrypt(compressed_message, "body", ENCRYPTIONKEY)
        stored_message = storebyreference(encrypted_message, "body", the_storage.use_sub_path("passbyreference"))
        import json  # pylint: disable=import-outside-toplevel

        for i in execute(stored_message):
            loaded_message = fetchbyreference(i, "body", the_storage.use_sub_path("passbyreference"))
            unencrypted = _.decrypt(loaded_message, "body", ENCRYPTIONKEY)
            decompressed = _.decompress(unencrypted, "body")
            print(json.dumps(decompressed))



if __name__ == "__main__":
    unittest.main()