import json

import azure.functions as func
from azure.servicebus import ServiceBusMessage

from hypergo.custom_types import JsonDict, TypedDictType


class MessageType(TypedDictType):
    body: JsonDict
    routingkey: str


class Message:
    @staticmethod
    def from_azure_functions_service_bus_message(message: func.ServiceBusMessage) -> MessageType:
        return {"body": json.loads(message.get_body().decode("utf-8")), "routingkey": message.user_properties["routingkey"]}

    @staticmethod
    def to_azure_service_bus_service_bus_message(message: MessageType) -> ServiceBusMessage:
        ret: ServiceBusMessage = ServiceBusMessage(body=json.dumps(message["body"]), application_properties={"routingkey": message["routingkey"]})
        return ret

    # def __init__(self, message: MessageType) -> None:
    #     self._body: JsonDict = message["body"]
    #     self._routingkey: str = message["routingkey"]
    #     self._config: Config = Config(message["config"])

    # def to_dict(self) -> MessageType:
    # return {"body": self._body, "routingkey": self._routingkey, "config":
    # self._config.to_dict()}

    # @property
    # def body(self) -> JsonDict:
    #     return self._body

    # @property
    # def routingkey(self) -> str:
    #     return self._routingkey

    # @property
    # def config(self) -> Config:
    #     return self._config
