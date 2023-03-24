from typing import Any, Dict

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusSender

from hypergo.message import Message

# from hypergo.configuration import Configuration


class AzureServiceBusMessage(Message):
    def get_data(self) -> Dict[str, Any]:
        return self._message.get_body().decode('utf-8')

    def get_meta(self) -> Dict[str, Any]:
        return self._message.user_properties

    def get_rk(self) -> str:
        return self._message.user_properties["routingkey"]

    def send(self, message: Message) -> None:
        # Configuration.get("ldpevents_servicebus_connection_string")
        servicebus_connection_string: str = "some_string"
        servicebus_client: ServiceBusClient = ServiceBusClient.from_connection_string(servicebus_connection_string)
        with servicebus_client:
            sender: ServiceBusSender = servicebus_client.get_topic_sender("datalink")
            msg: ServiceBusMessage = ServiceBusMessage(body=message.get_data(), application_properties=message.get_meta())
            sender.send_messages(msg)
