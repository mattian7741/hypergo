from typing import Any

import azure.functions as func
from azure.servicebus import (ServiceBusClient, ServiceBusMessage,
                              ServiceBusSender)

from hypergo.message import Message, MessageType
from hypergo.service_bus_connection import ServiceBusConnection


class AzureServiceBusConnection(ServiceBusConnection):
    def __init__(self, conn_str: str) -> None:
        self._service_bus_client: ServiceBusClient = ServiceBusClient.from_connection_string(conn_str)

    def send(self, message: MessageType, namespace: str) -> None:
        azure_message: ServiceBusMessage = Message.to_azure_service_bus_service_bus_message(message)
        with self._service_bus_client:
            sender: ServiceBusSender = self._service_bus_client.get_topic_sender(namespace)
            sender.send_messages(azure_message)

    def consume(self, azure_message: func.ServiceBusMessage, **kwargs: Any) -> None:
        message: MessageType = Message.from_azure_functions_service_bus_message(azure_message)
        self.general_consume(message, **kwargs)
