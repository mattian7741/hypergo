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

    def test_basic_case(self):
        config = {
            "version": "2.0.0",
            "namespace": "datalink",
            "name": "syncdbtapitriggerconnector",
            "package": "ldp-pipeline-orchestrator",
            "lib_func": "pipeline_orchestrator.__main__.pass_message",
            "input_keys": ["json.record.sync_dbt.api_log_datum"],
            "output_keys": ["api_trigger.request.sync_dbt.api_log_datum"],
            "input_bindings": ["{message.body.json_datum}"],
            "output_bindings": ["message.body"],
            "input_operations": {},
            "output_operations": {}
        }

        message = {
            "routingkey": "api_log_datum.json.record.sync_dbt",
            "body": {
                "json_datum": {
                    "DBT_MODEL": "PAYMENT",
                    "BATCHID": "20220718085603627",
                    "MASTERURL": "https://login.microsoftonline.com/d6130c51-e1f1-43fa-b6cb-9634098b05a6/oauth2/v2.0/token",
                    "CLIENTID": "d2b5cbdc-d5b7-44d9-925a-97187476bdce",
                    "CLIENTSECRET": "_WO8Q~iNgH38U7kyqtEcuE~aD51wKsEYrVN3IaoH",
                    "SUBSCRIPTIONKEY": "49e9c5c615c444019c9c3c7fac8fe469",
                    "SCOPE": "api://8ab3e011-1a1a-4ed2-91a5-646d7030e0f6/.default",
                    "POSTURL": "https://dev-api.linklogistics.com/change-data-capture/v1/trigger/ws-customer"
                }
            },
            "transaction": "transactionkey_20230918002901587329f648735b"
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