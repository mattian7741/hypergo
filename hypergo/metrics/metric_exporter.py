from abc import abstractmethod
from hypergo.metrics.base_metrics import MetricResult
from typing import Any, Sequence


class MetricExporter:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs

    @abstractmethod
    def export(self, meter: str, metric_name: str, description: str,
               metric_result: MetricResult | Sequence[MetricResult]) -> None:
        raise NotImplementedError("Derived class must implement export method")
