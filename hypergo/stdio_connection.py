import json
from typing import List, Union

from hypergo.config import ConfigType
from hypergo.connection import Connection
from hypergo.message import Message, MessageType
from hypergo.storage import Storage
from hypergo.utility import Utility


class RoutingKeyMismatchError(Exception):
    def __init__(self, routing_key: str, input_keys: List[str]):
        self.routing_key = routing_key
        self.input_keys = input_keys
        message = f"Message cannot be routed to this component because the routing key '{routing_key}' does not match any of the input keys {input_keys}"
        super().__init__(message)


class StdioConnection(Connection):
    def __init__(self) -> None:
        pass

    def send(self, message: MessageType, namespace: str) -> None:
        print(json.dumps(message))

    def consume(self, stdio_message: str, config: ConfigType, storage: Union[Storage, None]) -> None:
        message: MessageType = Message.from_stdio_message(stdio_message)
        routingkey = Utility.deep_get(message, "routingkey")
        input_keys = Utility.deep_get(config, "input_keys")

        for key in input_keys:
            if set(key.split(".")).issubset(set(routingkey.split("."))):
                return self.general_consume(message, config, storage)

        raise RoutingKeyMismatchError(routingkey, input_keys)
