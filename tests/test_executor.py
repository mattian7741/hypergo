import os
import tempfile
import unittest
from typing import Any, Dict
from unittest import mock

import hypergo.hyperdash as _
from hypergo.hypertest import (
    exceptions,
    handle_substitution,
    passbyreference,
    replace_wildcard_from_routingkey,
    storebyreference,
)
from hypergo.local_storage import LocalStorage
from hypergo.storage import Storage

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

    def test_passbyreference(self):
        input_body = "data"
        storage_key = f"unique_storage_key"

        with mock.patch(
            "hypergo.hyperdash.unique_identifier", return_value=storage_key
        ):
            storage = LocalStorage()

            @passbyreference
            def test_func(data):
                message_body_in_the_function = data["message"]["body"]

                yield {
                    "message": {"body": f"modified {message_body_in_the_function}"},
                    "storage": storage,
                }

            data = {"message": {"body": input_body}, "storage": storage}

            # It's awkward to use this function in a passbyreference test. Need to rethink.
            storebyreference(
                data, "message.body", storage.use_sub_path("passbyreference/")
            )

            result_generator = test_func(data)
            result_data = next(result_generator)

            self.assertEqual(result_data["message"]["body"], storage_key)

            # Construct the file path where the content is expected to be saved
            expected_file_path = os.path.join(
                ".hypergo_storage", "passbyreference", storage_key
            )

            # Check that the file exists and contains the expected content
            self.assertTrue(
                os.path.exists(expected_file_path), "File was not created as expected"
            )
            with open(expected_file_path, "r", encoding="utf-8") as file:
                self.assertEqual(
                    file.read(),
                    '"modified data"',
                    "File content does not match expected",
                )


# class TestExecute(unittest.TestCase):
#     def test_uncompress_no_key(self):
#         # message = {
#         #     "transaction": "transactionkey_202308161628349844546ab7b199",
#         #     "routingkey": "x.y.z",
#         #     "body": {
#         #         "a": {
#         #             "fn": lambda m, n: (m + n) / (m * n),
#         #             "x": 23,
#         #             "y": 41
#         #         }
#         #     }
#         # }

#         message = {
#             "routingkey": "xyz.y.z",
#             "body": {
#                 "a": {
#                     "fn": lambda m, n: (m + n) / (m * n),
#                     "xyz": 23,
#                     "y": 41
#                 }
#             }
#             # ,{
#             #     "a": {
#             #         "fn": lambda m, n: (m + n) / (m - n),
#             #         "x": 31,
#             #         "y": 15
#             #     }
#             # }]
#         }
#         serialized_message = _.serialize(message, "body")
#         compressed_message = _.compress(serialized_message, "body")
#         encrypted_message = _.encrypt(compressed_message, "body", ENCRYPTIONKEY)
#         stored_message = storebyreference(encrypted_message, "body", test_storage.use_sub_path("passbyreference"))
#         import json # pylint: disable=import-outside-toplevel
#         for i in execute(stored_message):
#             loaded_message = fetchbyreference(i, "body", test_storage.use_sub_path("passbyreference"))
#             unencrypted = _.decrypt(loaded_message, "body", ENCRYPTIONKEY)
#             uncompressed = _.uncompress(unencrypted, "body")
#             print(json.dumps(uncompressed))

if __name__ == "__main__":
    unittest.main()
