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

def the_function(farfunc: Callable[[float, float], float], float1: float, float2: float, trans: Transaction) -> float:
    count = trans.get("count", 1)
    trans.set("count", count + 1)
    result: float = farfunc(float1, float2) / count
    return result


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
the_context: Dict[str, Any] = {"function": the_function, "storage": the_storage, "config": the_config}


class TestBasicCase(unittest.TestCase):
    def test_no_wildcard_present(self):
        def the_function(farfunc: Callable[[float, float], float], float1: float, float2: float, trans: Transaction) -> float:
            count = trans.get("count", 1)
            trans.set("count", count + 1)
            result: float = farfunc(float1, float2) / count
            return result


        the_config: Dict[str, Any] = {
            "lib_func": "hypergo.hypertest.the_function",
            "input_keys": ["abc.def", "ghi.jkl"],
            "output_keys": ["mno.pqr"],
            "input_bindings": ["constant string"],
            "output_bindings": [{"message": {"body": {"p": {"q": {"r": "{output}"}}}}}],
            "custom_properties": {"x": "{message.body.a.fn}", "w": "{message.body.a.c}"},
        }

        the_storage: Storage = LocalStorage()
        the_context: Dict[str, Any] = {"function": the_function, "storage": the_storage, "config": the_config}