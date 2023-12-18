import os
import sys
import unittest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.message import MessageType
from hypergo.config import ConfigType
from hypergo.executor import Executor


class TestOutputRoutingKey(unittest.TestCase):    
    def test_get_output_routing_key(self):
        cfg: ConfigType = {
                            "namespace": "datalink",
                            "name": "snowflakedbexecutor",
                            "package": "ldp-dbexecutor",
                            "lib_func": "dbexecutor.snowflake_db_executor.__main__.execute",
                            "input_keys": ["a.b.c", "a.b.d", "a.b", "a"],
                            "output_keys": ["y.h.?.?"],
                            "input_bindings": ["message.body"],
                            "output_bindings": ["message.body.field"]
                        }
        
        message: MessageType = {
            "body": [{"name": "Chris", "company": "LinkLogistics"}],
            "routingkey": "a.b.c.x",
            "transaction": "I'm a transaction"
        }

        executor = Executor(cfg)
        input_routingkey = "a.b.c.x"
        output_key = ".h.x.y"
        output_binding = "message.body.field"
        value = 999

        expected_output_message: MessageType = {
            "body": {"field": 999},
            "routingkey": output_key,
            "transaction": "I'm a transaction"
        }

        actual_result = executor.get_output_message(context=message, cfg=cfg, input_routingkey=input_routingkey, output_key=output_key, output_binding=output_binding, value=value)
        
        self.assertDictEqual(actual_result["body"], expected_output_message["body"])
        self.assertEqual(actual_result["routingkey"], expected_output_message["routingkey"])
        self.assertEqual(actual_result["transaction"], expected_output_message["transaction"])

if __name__ == '__main__':
    unittest.main()