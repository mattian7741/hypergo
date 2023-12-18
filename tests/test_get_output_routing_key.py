import os
import sys
import unittest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.message import MessageType
from hypergo.config import ConfigType
from hypergo.executor import Executor


class TestOutputRoutingKey(unittest.TestCase):
    def setUp(self) -> None:
        self.message: MessageType = {
        "body": [{"name": "Chris", "company": "LinkLogistics"}],
        "routingkey": "a.b.c.x"
        }
        return super().setUp()
    
    def test_get_output_routing_key(self):
        cfg: ConfigType = {
                            "namespace": "datalink",
                            "name": "snowflakedbexecutor",
                            "package": "ldp-dbexecutor",
                            "lib_func": "dbexecutor.snowflake_db_executor.__main__.execute",
                            "input_keys": ["a.b.c", "a.b.d", "a.b", "a"],
                            "output_keys": ["y.h.?.?"],
                            "input_bindings": ["message.body"],
                            "output_bindings": ["message.body"]
                        }
        executor = Executor(cfg)
        routingkey = "a.b.c.x"
        output_key = "y.h.?.?"
        expected_output_routing_key = ".b.c.h.x.y"
        self.assertEqual(executor.get_output_routing_key(routingkey, output_key), expected_output_routing_key)

if __name__ == '__main__':
    unittest.main()