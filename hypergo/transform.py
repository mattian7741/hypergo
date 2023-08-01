from typing import Any, Callable, Generator, List, TypeVar, cast

from hypergo.custom_types import JsonDict, JsonType
from hypergo.storage import Storage
from hypergo.utility import Utility

T = TypeVar("T")


class Transform:
    @staticmethod
    def operations(func):
        inmap = {
            "compression": Utility.uncompress,
            "serialization": Utility.deserialize
        }
        outmap = {
            "serialization": Utility.serialize,
            "compression": Utility.compress
        }
        def sort_a_like_b(A, B):
            B_index = {b: i for i, b in enumerate(B)}
            return sorted(A, key=lambda x: B_index.get(x[0], len(B)))

        def wrapper(self, data):

            inops = sort_a_like_b(Utility.deep_get(self._config, "input_operations"), list(inmap.keys()))
            outops = sort_a_like_b(Utility.deep_get(self._config, "output_operations"), list(outmap.keys()))
            mod_data = data
            for op in inops:
                mod_data = inmap[op[0]](mod_data, *op[1:])
            for result in func(self, mod_data):
                mod_data = result             
                for op in outops:
                    mod_data = outmap[op[0]](mod_data, *op[1:])

                yield mod_data
        return wrapper