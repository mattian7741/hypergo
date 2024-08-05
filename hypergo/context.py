import sys
from typing import Optional

from hypergo.config import ConfigType
from hypergo.custom_types import TypedDictType
from hypergo.message import MessageType
from hypergo.storage import Storage
from hypergo.transaction import Transaction

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


class ContextType(TypedDictType):
    message: MessageType
    config: ConfigType
    storage: NotRequired[Optional[Storage]]
    transaction: Transaction
