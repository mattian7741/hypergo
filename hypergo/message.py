import json
import sys
from typing import Union, cast
from urllib.parse import urlparse

import azure.functions as func
from azure.servicebus import ServiceBusMessage

from hypergo.custom_types import JsonDict, TypedDictType
from hypergo.transaction import Transaction

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


class MessageType(TypedDictType):
    body: JsonDict
    routingkey: str
    storagekey: NotRequired[str]
    transaction: NotRequired[Union[str, Transaction]]
    __txid__: NotRequired[str]


class Message:
    @staticmethod
    def from_azure_functions_service_bus_message(
        message: func.ServiceBusMessage,
    ) -> MessageType:
        return {
            "body": json.loads(message.get_body().decode("utf-8")),
            "routingkey": message.user_properties["routingkey"],
            "storagekey": cast(str, message.user_properties.get("storagekey")),
            "transaction": cast(str, message.user_properties.get("transaction")),
        }

    @staticmethod
    def from_http_request(request: func.HttpRequest) -> MessageType:
        return {
            "body": request.get_json(),
            "routingkey": "http.azurefunction" + urlparse(request.url).path.replace("/", "."),
        }

    @staticmethod
    def from_stdio_message(stdin: str) -> MessageType:
        return cast(MessageType, json.loads(stdin))

    @staticmethod
    def to_azure_service_bus_service_bus_message(
        message: MessageType,
    ) -> ServiceBusMessage:
        ret: ServiceBusMessage = ServiceBusMessage(
            body=json.dumps(message.get("body")),
            application_properties={
                "routingkey": message["routingkey"],
                "storagekey": cast(str, message.get("storagekey")),
                "transaction": cast(str, message.get("transaction")),
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
