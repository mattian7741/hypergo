import json
import unittest
from unittest.mock import MagicMock

import azure.functions as func
from azure.servicebus import ServiceBusMessage

from hypergo.message import Message


class TestMessage(unittest.TestCase):
    def setUp(self):
        self.sample_message = {
            "body": {"key": "value"},
            "routingkey": "route",
            "transaction": ["txn1", "txn2"],
            "storagekey": "storage",
        }

    def test_from_azure_functions_service_bus_message(self):
        service_bus_message = MagicMock(spec=func.ServiceBusMessage)
        service_bus_message.get_body.return_value = json.dumps(self.sample_message["body"]).encode("utf-8")
        service_bus_message.user_properties = {
            "routingkey": self.sample_message["routingkey"],
            "transaction": self.sample_message["transaction"],
            "storagekey": self.sample_message["storagekey"],
        }

        result = Message.from_azure_functions_service_bus_message(service_bus_message)

        expected_result = {
            "body": self.sample_message["body"],
            "routingkey": self.sample_message["routingkey"],
            "transaction": self.sample_message["transaction"],
            "storagekey": self.sample_message["storagekey"],
        }
        self.assertEqual(result, expected_result)

    def test_to_azure_service_bus_service_bus_message(self):
        result = Message.to_azure_service_bus_service_bus_message(self.sample_message)

        expected_result = ServiceBusMessage(
            body=json.dumps(self.sample_message["body"]),
            application_properties={
                "routingkey": self.sample_message["routingkey"],
                "transaction": self.sample_message["transaction"],
                "storagekey": self.sample_message["storagekey"],
            },
        )
        self.assertEqual(list(result.body), list(expected_result.body))
        self.assertEqual(result.application_properties, expected_result.application_properties)


if __name__ == "__main__":
    unittest.main()
