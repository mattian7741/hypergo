import os
import sys
import unittest

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.message import MessageType
from hypergo.config import ConfigType
from hypergo.executor import Executor

class TestDynamicRoutingKey(unittest.TestCase):
    def setUp(self) -> None:
        self.message: MessageType = {
            "body": [{"name": "Chris", "company": "LinkLogistics"}],
            "routingkey": "a.b.c.x"
        }
        return super().setUp()

    def test_dynamic_routing_key(self):
        cfg: ConfigType = {
                            "version": "2.0.0",
                            "namespace": "datalink",
                            "name": "batchstreamer",
                            "package": "ldp-batch-to-stream-producer",
                            "lib_func": "batch_to_stream_producer.__main__.batch_to_stream",
                            "input_keys": ["a.b.c", "a.b.d", "a.b", "a"],
                            "output_keys": ["y.h.?.?"],
                            "input_bindings": ["{message.body}", None],
                            "output_bindings": ["message.body"]
                        }

        executor = Executor(cfg)
        output_key: str = "b.c.h.x.y"
        for sdk_message in executor.execute(self.message):
            self.assertEqual(output_key, sdk_message["routingkey"])


if __name__ == '__main__':
    unittest.main()
