import os
import sys
import unittest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.config import ConfigType
from hypergo.executor import Executor


class TestGetOutputRoutingKeys(unittest.TestCase):    
    def test_token_consumption(self):
        cfg: ConfigType = {
                            "namespace": "datalink",
                            "name": "snowflakedbexecutor",
                            "package": "ldp-db-executor",
                            "lib_func": "pipeline_orchestrator.__main__.pass_message",
                            "input_keys": ["a.b.c"],
                            "output_keys": ["?.e.f"],
                            "input_bindings": ["config.custom_properties.?"],
                            "output_bindings": ["message.body.snowflake.db.record"],
                            "custom_properties":{}
                        }
        executor = Executor(cfg)

        routingkey = "a.b.c.d"
        expected_result_set = {"d", "e", "f"}

        result = executor.get_output_routing_keys(input_message_routing_key=routingkey)

        self.assertEqual(set(result[0].split(".")), expected_result_set)

    def test_multiple_output_keys(self):
        cfg: ConfigType = {
                            "namespace": "datalink",
                            "name": "snowflakedbexecutor",
                            "package": "ldp-db-executor",
                            "lib_func": "pipeline_orchestrator.__main__.pass_message",
                            "input_keys": ["a.b.c"],
                            "output_keys": ["?.e.f", "?.g.h"],
                            "input_bindings": ["config.custom_properties.?"],
                            "output_bindings": ["message.body.snowflake.db.record"],
                            "custom_properties":{}
                        }
        executor = Executor(cfg)

        routingkey = "a.b.c.d"
        expected_result_sets = [{"d", "e", "f"}, {"d", "g", "h"}]

        result = executor.get_output_routing_keys(input_message_routing_key=routingkey)

        for expected, actual in zip(expected_result_sets, result):
            self.assertEqual(set(actual.split(".")), expected)

if __name__ == '__main__':
    unittest.main()