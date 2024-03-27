from collections import namedtuple
from typing import List

__all__ = ["custom_metrics_metadata"]

CustomMetrics = namedtuple("CustomMetrics", ["package", "module_name", "class_name"])

custom_metrics_metadata: List[CustomMetrics] = [
    CustomMetrics(package="hypergo.metrics", module_name="custom_metrics", class_name="CustomMetrics")
]
