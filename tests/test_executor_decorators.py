import base64
import inspect
import os
import tempfile
import unittest
from unittest import mock
import hypergo.hyperdash as _

import hypergo.hyperdash as _
from hypergo.hypertest import (
    ENCRYPTIONKEY,
    bind_func,
    compression,
    encryption,
    exceptions,
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

test_storage: Storage = LocalStorage()


class TestReplaceWildcardFromRoutingkey(unittest.TestCase):
    def test_no_wildcard_present(self):
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["{message.body.abc.bcd}"],
                "output_bindings": ["message.body.payload"],
            },
            "message": {
                "routingkey": "xxx.yyy.zzz",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }

        input_string = "message.body.abc.bcd"
        expected_output = "message.body.abc.bcd"

        self.assertEqual(
            replace_wildcard_from_routingkey(context, input_string), expected_output
        )

    def test_single_wildcard_replacement(self):
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["{config.custom_properties.?}"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {"111": {"222": "333"}, "444": "555"},
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.111.222",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }
        input_string = "config.custom_properties.?"
        expected_output = "config.custom_properties.111"

        self.assertEqual(
            replace_wildcard_from_routingkey(context, input_string), expected_output
        )

    def test_wildcard_with_no_match(self):
        # What should happen here?
        pass


class TestHandleSubstitutions(unittest.TestCase):
    def test_message_body(self):
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["{message.body.abc.bcd}"],
                "output_bindings": ["message.body.payload"],
            },
            "message": {
                "routingkey": "xxx.yyy.zzz",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }
        expected_output = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": [23],
                "output_bindings": ["message.body.payload"],
            },
            "message": {
                "routingkey": "xxx.yyy.zzz",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }

        self.assertDictEqual(handle_substitution(context, context), expected_output)

    def test_custom_properties(self):
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": [
                    "string constant",
                    "{config.custom_properties.the\\.correct\\.field}",
                ],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "the.incorrect.field": "the incorrect value",
                    "the.correct.field": "the correct value",
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }
        expected_output = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["string constant", "the correct value"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "the.incorrect.field": "the incorrect value",
                    "the.correct.field": "the correct value",
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }

        self.assertDictEqual(handle_substitution(context, context), expected_output)

    def test_custom_properties_with_wildcard(self):
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": [
                    "string constant",
                    "{config.custom_properties.?.nested}",
                ],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "incorrect": {"nested": "the incorrect value"},
                    "correct": {"nested": "the correct value"},
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.correct",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }
        expected_output = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["string constant", "the correct value"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "incorrect": {"nested": "the incorrect value"},
                    "correct": {"nested": "the correct value"},
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.correct",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }

        self.assertDictEqual(handle_substitution(context, context), expected_output)

    def test_multiple_substitutions(self):
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": [
                    "{message.body.abc.bcd}",
                    "{config.custom_properties.?.nested}",
                ],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "incorrect": {"nested": "the incorrect value"},
                    "correct": {"nested": "the correct value"},
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.correct",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }
        expected_output = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": [23, "the correct value"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "incorrect": {"nested": "the incorrect value"},
                    "correct": {"nested": "the correct value"},
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.correct",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }

        self.assertDictEqual(handle_substitution(context, context), expected_output)

    def test_unmatched_substitution(self):  # is this the behavior we want?
        context = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["{message.body.xxxxxxx}"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "incorrect": {"nested": "the incorrect value"},
                    "correct": {"nested": "the correct value"},
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.correct",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }
        expected_output = {
            "lib_func": "repo.some.function",
            "storage": test_storage,
            "config": {
                "lib_func": "hypergo.hypertest.the_function",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["{message.body.xxxxxxx}"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {
                    "incorrect": {"nested": "the incorrect value"},
                    "correct": {"nested": "the correct value"},
                },
            },
            "message": {
                "routingkey": "xxx.yyy.zzz.correct",
                "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
            },
        }

        self.assertDictEqual(handle_substitution(context, context), expected_output)


def test_function(value: int):
    if value < 0:
        raise ValueError("value must be non-negative")
    for i in range(0, 2):
        yield value + i


class TestExceptions(unittest.TestCase):
    @mock.patch("builtins.print")
    def test_no_exceptions(self, mocked_print):
        gen = exceptions(test_function)(1)
        result_list = list(gen)

        self.assertEqual(result_list, [1, 2])
        mocked_print.assert_not_called()

    @mock.patch("builtins.print")
    def test_with_exception(self, mocked_print):
        gen = exceptions(test_function)(-1)
        result_list = list(gen)

        self.assertTrue(mocked_print.called)

        printed_exception = mocked_print.call_args[0][0]
        self.assertIsInstance(printed_exception, ValueError)
        self.assertEqual(str(printed_exception), "value must be non-negative")


class TestPassByReference(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    @mock.patch("hypergo.hyperdash.unique_identifier", return_value="unique_storage_key")
    def test_passbyreference(self, mock_unique_identifier):
        storage = LocalStorage()

        @passbyreference
        def test_func(data):
            message_body_in_the_function = data["message"]["body"]

            yield {"message": {"body": f"modified {message_body_in_the_function}"}}

        data = {"message": {"body": "data"}, "storage": storage}

        # It's awkward to use this function in a passbyreference test. Need to rethink.
        storebyreference(
            data, "message.body", storage.use_sub_path("passbyreference/")
        )

        result_generator = test_func(data)
        result_data = next(result_generator)

        self.assertEqual(result_data["message"]["body"], "unique_storage_key")

        expected_file_path = os.path.join(
            ".hypergo_storage", "passbyreference", "unique_storage_key"
        )

        self.assertTrue(
            os.path.exists(expected_file_path), "File was not created as expected"
        )
        with open(expected_file_path, "r", encoding="utf-8") as file:
            self.assertEqual(
                file.read(),
                '"modified data"',
                "File content does not match expected",
            )


class TestEncryptDecrypt(unittest.TestCase):
    # TODO: rewrite this once @encryption responds to configs appropriately
    def test_encrypt_decrypt(self):
        @encryption
        def test_func(data):
            message_body_in_the_function = data["message"]["body"]

            yield {"message": {"body": f"modified {message_body_in_the_function}"}}

        data = {"message": {"body": "data"}}

        _.encrypt(data, "message.body", ENCRYPTIONKEY)

        result_generator = test_func(data)
        result_data = next(result_generator)

        self.assertEqual(
            _.decrypt(result_data, "message.body", ENCRYPTIONKEY),
            {"message": {"body": "modified data"}},
        )


class TestTransactions(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    @mock.patch("hypergo.utility.Utility.unique_identifier", return_value="unique_storage_key")
    def test_transaction_dne(self, mock_unique_identifier):
        storage = LocalStorage()

        @transactions
        def test_func(data):
            message_body_in_the_function = data["message"]["body"]

            data["message"]["body"] = f"modified {message_body_in_the_function}"

            yield data

        data = {
            "message": {
                "body": "data",
            },
            "storage": storage,
        }

        result_generator = test_func(data)
        result_data = next(result_generator)

        self.assertEqual(result_data["message"]["body"], "modified data")

        expected_file_path = os.path.join(
            ".hypergo_storage", "transactions", "transactionkey_unique_storage_key"
        )

        self.assertTrue(
            os.path.exists(expected_file_path), "File was not created as expected"
        )
        with open(expected_file_path, "r", encoding="utf-8") as file:
            # I think this is the temporary result, right?
            self.assertEqual(
                file.read(),
                '{"txid": "unique_storage_key", "data": {}}',
                "File content does not match expected",
            )

    @mock.patch("hypergo.utility.Utility.unique_identifier", return_value="txid")
    def test_transaction_exists(self, mock_unique_identifier):
        storage = LocalStorage()

        @transactions
        def test_func(data):
            message_body_in_the_function = data["message"]["body"]

            data["message"]["body"] = f"modified {message_body_in_the_function}"

            yield data

        existing_transaction = Transaction("txid", {"some": "data"})
        serialized_transaction = existing_transaction.serialize()
        storage.save(f"transactionkey_{existing_transaction.txid}", serialized_transaction)

        data = {
            "message": {
                "body": "data",
            },
            "storage": storage,
            "transaction": existing_transaction
        }

        result_generator = test_func(data)
        result_data = next(result_generator)

        self.assertEqual(result_data["message"]["body"], "modified data")

        expected_file_path = os.path.join(
            ".hypergo_storage", "transactions", "transactionkey_txid"
        )

        self.assertTrue(
            os.path.exists(expected_file_path), "File was not created as expected"
        )
        with open(expected_file_path, "r", encoding="utf-8") as file:
            # I think this is a temporary result, right? why is data empty?
            self.assertEqual(
                file.read(),
                '{"txid": "txid", "data": {}}',
                "File content does not match expected",
            )


class TestBindArguments(unittest.TestCase):
    def test_encrypt_decrypt(self):
        pass


class TestChunking(unittest.TestCase):
    def test_passbyreference(self):
        pass


class TestStreaming(unittest.TestCase):
    def test_passbyreference(self):
        pass

class TestCompression(unittest.TestCase):
    # TODO: rewrite this once @compression responds to configs appropriately
    def test_compression(self):
        storage = LocalStorage()

        data = {
            "message": {
                "body": "data",
            },
            "storage": storage,
        }

        @compression
        def test_func(data):
            message_body_in_the_function = data["message"]["body"]

            data["message"]["body"] = f"modified {message_body_in_the_function}"

            yield data
        
        compressed_data = _.compress(data, "message.body")

        # Execute the decorated generator with compressed input data
        generator = test_func(compressed_data)
        result_data = next(generator)

        decompressed_result_data = _.decompress(result_data, "message.body")

        self.assertDictEqual(decompressed_result_data, {
            "message": {
                "body": "modified data",
            },
            "storage": storage,
        })

class TestBindFunc(unittest.TestCase):
    @mock.patch("importlib.import_module")
    def test_bind_func(self, mock_import_module):
        @bind_func
        def test_func(data):
            message_body_in_the_function = data["message"]["body"]
            data["message"]["body"] = f"modified {message_body_in_the_function}"

            yield data

        def func_to_bind():
            yield True

        mock_module = mock.Mock()
        setattr(mock_module, 'func_to_bind', func_to_bind)

        mock_import_module.return_value = mock_module

        context = {
            "config": {
                "lib_func": "testiboi.func_to_bind",
                "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                "output_keys": ["mmm.nnn.ooo"],
                "input_bindings": ["{config.custom_properties.?}"],
                "output_bindings": ["message.body.payload"],
                "custom_properties": {"111": {"222": "333"}, "444": "555"},
            },
            "message": {
                "body": "data",
            },
        }

        result_generator = test_func(context)
        result_data = next(result_generator)

        # we can't directly test the functionality of func_to_bind because when it is set as an attribute on a mock object, it becomes a mock. But we can assert that there is something there.
        assert "func_spec" in result_data["config"]

        result_data["config"].pop("func_spec")

        self.assertDictEqual(
            result_data,
            {
                "config": {
                    "lib_func": "testiboi.func_to_bind",
                    "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
                    "output_keys": ["mmm.nnn.ooo"],
                    "input_bindings": ["{config.custom_properties.?}"],
                    "output_bindings": ["message.body.payload"],
                    "custom_properties": {"111": {"222": "333"}, "444": "555"},
                },
                "message": {
                    "body": "modified data",
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
