import json
import re
from typing import Any, Dict, List, Union, cast

import yaml
from typing_extensions import NotRequired

from hypergo.custom_types import JsonDict, TypedDictType
from hypergo.logger import logger
from hypergo.mapping import Mapping
from hypergo.utility import Utility


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
    custom_properties: NotRequired[Dict[str, Any]]


class Config:
    @staticmethod
    def from_yaml(file_name: str) -> ConfigType:
        with open(file_name, "r", encoding="utf-8") as file_handle:
            cfg_dict: JsonDict = yaml.safe_load(file_handle)
            cfg: ConfigType = Config.convert(cfg_dict)
            if cfg != cfg_dict:
                logger.warning(f"Mapping version deprecated; use:\n{cfg}")
            return cfg

    @staticmethod
    def from_json(file_name: str) -> ConfigType:
        with open(file_name, "r", encoding="utf-8") as file_handle:
            cfg_dict: JsonDict = json.load(file_handle)
            cfg: ConfigType = Config.convert(cfg_dict)
            if cfg != cfg_dict:
                logger.warning(f"Mapping version deprecated; use:\n{cfg}")
            return cfg

    @staticmethod
    def convert(cfg_dict: JsonDict) -> ConfigType:
        mapping_dict: Dict[str, Mapping] = {
            "0.X.X": Mapping(
                {
                    "version": "1.0.0",
                    "name": lambda source: source("name"),
                    "namespace": lambda source: source("namespace"),
                    "package": lambda source: source("package"),
                    "lib_func": lambda source: source("lib_func"),
                    "input_keys": lambda source: source("input_keys"),
                    "output_keys": lambda source: source("output_keys"),
                    "input_bindings": lambda source: [
                        (
                            field
                            if not isinstance(field, str)
                            else re.sub(
                                r"^'(.+)'$",
                                "\\1",
                                re.sub(r"^([^'].+[^'])$", "{\\1}", field),
                            )
                        )
                        for field in source("input_bindings")
                    ],
                    "output_bindings": lambda source: source("output_bindings"),
                    "input_operations": lambda source: source("input_operations", []),
                    "output_operations": lambda source: source("output_operations", []),
                    "custom_properties": lambda source: source("custom_properties", {}),
                }
            ),
            "1.X.X": Mapping(
                {
                    "version": "2.0.0",
                    "name": lambda source: source("name"),
                    "namespace": lambda source: source("namespace"),
                    "package": lambda source: source("package"),
                    "lib_func": lambda source: source("lib_func"),
                    "input_keys": lambda source: source("input_keys"),
                    "output_keys": lambda source: source("output_keys"),
                    "input_bindings": lambda source: source("input_bindings"),
                    "output_bindings": lambda source: source("output_bindings"),
                    "input_operations": lambda source: {op: ["body"] for op in source("input_operations")},
                    "output_operations": lambda source: {op: ["body"] for op in source("output_operations")},
                    "custom_properties": lambda source: source("custom_properties", {}),
                }
            ),
        }

        mapping: Union[Mapping, None] = mapping_dict.get(
            re.sub(
                r"(\d)\.\d\.\d",
                "\\1.X.X",
                Utility.deep_get(cfg_dict, "version", "0.0.0"),
            )
        )

        if not mapping:
            return cast(ConfigType, cfg_dict)
        return Config.convert(mapping.map(cfg_dict))
