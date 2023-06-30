import json
import sys
from typing import List

import yaml

from hypergo.custom_types import JsonDict, TypedDictType

if sys.version_info >= (3, 11):
    from typing import NotRequired
else:
    from typing_extensions import NotRequired


# from hypergo.utility import Utility


class ConfigType(TypedDictType):
    namespace: str
    name: str
    package: str
    lib_func: str
    input_keys: List[str]
    output_keys: List[str]
    input_bindings: List[str]
    output_bindings: List[str]
    input_operations: NotRequired[List[str]]
    output_operations: NotRequired[List[str]]
    input_json_schema: NotRequired[JsonDict]
    output_json_schema: NotRequired[JsonDict]


class Config:  # pylint: disable=too-many-instance-attributes
    # @staticmethod
    # def from_yaml(file_name: str) -> "Config":
    #     return Config.from_dict(Utility.yaml_read(file_name))

    # @staticmethod
    # def from_json(file_name: str) -> "Config":
    #     return Config.from_dict(Utility.json_read(file_name))

    singleton_instance: "Config"

    # @staticmethod
    # def instance() -> "Config":
    #     Config.singleton_instance = Config.singleton_instance or Config.from_json("config.json")
    #     return Config.singleton_instance

    @staticmethod
    def from_yaml(file_name: str) -> ConfigType:
        with open(file_name, "r", encoding="utf-8") as file_handle:
            cfg_dict: ConfigType = yaml.safe_load(file_handle)
            return cfg_dict

    @staticmethod
    def from_json(file_name: str) -> ConfigType:
        with open(file_name, "r", encoding="utf-8") as file_handle:
            cfg_dict: ConfigType = json.load(file_handle)
            return cfg_dict

    # @staticmethod
    # def from_dict(cfg_dict: ConfigType) -> "Config":
    #     return Config(cfg_dict)

    # def to_dict(self) -> ConfigType:
    # return {"namespace": self.namespace, "name": self.name, "package":
    # self.package, "lib_func": self.lib_func, "input_keys": self.input_keys,
    # "output_keys": self.output_keys, "input_bindings": self.input_bindings,
    # "output_bindings": self.output_bindings}

    # def __init__(self, cfg_dict: ConfigType) -> None:
    #     self._namespace: str = cast(str, cfg_dict.get("namespace"))
    #     self._name: str = cast(str, cfg_dict.get("name"))
    #     self._package: str = cast(str, cfg_dict.get("package"))
    #     self._lib_func: str = cast(str, cfg_dict.get("lib_func"))
    #     self._input_keys: List[str] = cast(List[str], cfg_dict.get("input_keys"))
    #     self._output_keys: List[str] = cast(List[str], cfg_dict.get("output_keys"))
    #     self._input_bindings: List[str] = cast(List[str], cfg_dict.get("input_bindings"))
    #     self._output_bindings: List[str] = cast(List[str], cfg_dict.get("output_bindings"))

    # @property
    # def namespace(self) -> str:
    #     return self._namespace

    # @property
    # def name(self) -> str:
    #     return self._name

    # @property
    # def package(self) -> str:
    #     return self._package

    # @property
    # def lib_func(self) -> str:
    #     return self._lib_func

    # @property
    # def input_keys(self) -> List[str]:
    #     return self._input_keys

    # @property
    # def output_keys(self) -> List[str]:
    #     return self._output_keys

    # @property
    # def input_bindings(self) -> List[str]:
    #     return self._input_bindings

    # @property
    # def output_bindings(self) -> List[str]:
    #     return self._output_bindings
