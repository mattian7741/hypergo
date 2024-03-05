import os
import tempfile
import unittest
from unittest import mock
from unittest.mock import Mock
from hypergo.hypertest import Executor

import hypergo.hyperdash as _
from hypergo.local_storage import LocalStorage


class TestExecute(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.original_cwd = os.getcwd()
        os.chdir(self.temp_dir.name)

    def tearDown(self):
        os.chdir(self.original_cwd)
        self.temp_dir.cleanup()

    @mock.patch("hypergo.utility.Utility.unique_identifier", return_value="unique_storage_key")
    def test_basic_case(self, mock_unique_identifier):
        config = {
            "version": "2.0.0",
            "namespace": "datalink",
            "name": "testcomponent",
            "package": "ldp-pipeline-orchestrator",
            "lib_func": "pipeline_orchestrator.__main__.pass_message",
            "input_keys": ["some.test.keys"],
            "output_keys": ["result.output.keys"],
            "input_bindings": ["some data"],
            "output_bindings": ["message.body"],
            "input_operations": {},
            "output_operations": {}
        }

        message = {
            "routingkey": "some.test.keys",
            "body": {}
        }
        
        executor = Executor(config, LocalStorage())
        result_generator = executor.execute(message)
        result = next(result_generator)

        print(f"end result: {result}")

        self.assertDictEqual({"routingkey": "result.output.keys", "message": {"body": "some data"}, "transaction": "transactionkey_unique_storage_key"}, result)

    @mock.patch("hypergo.utility.Utility.unique_identifier", return_value="unique_storage_key")
    def test_simple_substitution(self, mock_unique_identifier):
        config = {
            "version": "2.0.0",
            "namespace": "datalink",
            "name": "testcomponent",
            "package": "ldp-pipeline-orchestrator",
            "lib_func": "pipeline_orchestrator.__main__.pass_message",
            "input_keys": ["some.test.keys"],
            "output_keys": ["result.output.keys"],
            "input_bindings": ["{message.body.some}"],
            "output_bindings": ["message.body"],
            "input_operations": {},
            "output_operations": {}
        }

        message = {
            "routingkey": "some.test.keys",
            "body": {
                "some": "data"
            }
        }

        executor = Executor(config, LocalStorage())
        result_generator = executor.execute(message)
        result = next(result_generator)

        self.assertDictEqual({"routingkey": "result.output.keys", "message": {"body": "data"}, "transaction": "transactionkey_unique_storage_key"}, result)


if __name__ == "__main__":
    unittest.main()