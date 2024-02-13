import unittest
from typing import Any, Callable, Dict

import hypergo.hyperdash as _
from hypergo import transaction
from hypergo.hypertest import handle_substitution, replace_wildcard_from_routingkey
from hypergo.local_storage import LocalStorage
from hypergo.storage import Storage


def the_function(
    farfunc: Callable[[float, float], float],
    float1: float,
    float2: float,
    trans: transaction,
) -> float:
    count = trans.get("count", 1)
    trans.set("count", count + 1)
    result: float = farfunc(float1, float2) / count
    return result


# def the_function(arr: List[Any]) -> float:
#     print("arr: ", arr)
#     return 4321

the_config: Dict[str, Any] = {
    "lib_func": "hypergo.hypertest.the_function",
    "input_keys": ["xxx.yyy.zzz", "vvv.uuu.www"],
    "output_keys": ["mmm.nnn.?"],
    "input_bindings": [
        "{config.custom_properties.?}",
        "{message.body.abc.bcd}",
        "{message.body.dfg}",
        "{transaction}",
    ],
    # "input_bindings": ["{message.body}"],
    "output_bindings": [{"message": {"body": {"p": {"q": {"r": "{output}"}}}}}],
    "custom_properties": {"111": "{message.body.a.fn}", "222": "{message.body.a.c}"},
}

# the_generator: Callable[..., Generator[Any, None, None]] = generatorize(the_function)
the_storage: Storage = LocalStorage()
the_context: Dict[str, Any] = {
    "function": the_function,
    "storage": the_storage,
    "config": the_config,
    "message": {
        "routingkey": "xxx.yyy.zzz",
        "body": {"abc": {"bcd": 23, "cdf": 41}, "dfg": "scalar"},
    },
}


class TestReplaceWildcardFromRoutingkey(unittest.TestCase):
    def test_no_wildcard_present(self):
        context = {
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
            "function": the_function,
            "storage": the_storage,
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
#         stored_message = storebyreference(encrypted_message, "body", the_storage.use_sub_path("passbyreference"))
#         import json # pylint: disable=import-outside-toplevel
#         for i in execute(stored_message):
#             loaded_message = fetchbyreference(i, "body", the_storage.use_sub_path("passbyreference"))
#             unencrypted = _.decrypt(loaded_message, "body", ENCRYPTIONKEY)
#             uncompressed = _.uncompress(unencrypted, "body")
#             print(json.dumps(uncompressed))

if __name__ == "__main__":
    unittest.main()
