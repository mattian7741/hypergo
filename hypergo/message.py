import json
import sys
from typing import cast
from urllib.parse import urlparse

import azure.functions as func
from azure.servicebus import ServiceBusMessage

from hypergo.custom_types import JsonDict, TypedDictType

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


class MessageType(TypedDictType):
    body: JsonDict
    routingkey: str
    storagekey: NotRequired[str]


class Message:
    @staticmethod
    def from_azure_functions_service_bus_message(message: func.ServiceBusMessage) -> MessageType:
        return {
            "body": json.loads(message.get_body().decode("utf-8")),
            "routingkey": message.user_properties["routingkey"],
            "storagekey": cast(str, message.user_properties.get("storagekey")),
        }

    @staticmethod
    def from_http_request(request: func.HttpRequest) -> MessageType:
        return {"body": request.get_json(), "routingkey": "http_request" + urlparse(request.url).path.replace("/", ".")}

    @staticmethod
    def to_azure_service_bus_service_bus_message(message: MessageType) -> ServiceBusMessage:
        ret: ServiceBusMessage = ServiceBusMessage(
            body=json.dumps(message["body"]),
            application_properties={
                "routingkey": message["routingkey"],
                "storagekey": cast(str, message.get("storagekey")),
            },
        )

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
