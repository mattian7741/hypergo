
import inspect
from abc import abstractmethod
from typing import Callable, List, Set, Sequence, Union
from hypergo.utility import DynamicImports
from hypergo.metrics.metric_exporter import MetricExporter, ConsoleMetricExporter
from hypergo.metrics.base_metrics import MetricResult


class HypergoMetrics:
    _current_metric_exporters: Set[MetricExporter] = set([ConsoleMetricExporter()])

    @staticmethod
    def get_metrics_callback(
        package: str, module_name: str, class_name: str
    ) -> List[Callable[[MetricResult], MetricResult]]:
        callbacks: List[Callable[[MetricResult], MetricResult]] = []
        imported_class = DynamicImports.dynamic_imp_class(
            package=package, module_name=module_name, class_name=class_name
        )
        for _, member in inspect.getmembers(imported_class, predicate=inspect.isfunction):
            callbacks.append(member)
        return callbacks

    @staticmethod
    def set_metric_exporter(metric_exporter: MetricExporter) -> None:
        HypergoMetrics._current_metric_exporters.add(metric_exporter)

    @staticmethod
    def get_metric_exporters() -> Set[MetricExporter]:
        return HypergoMetrics._current_metric_exporters

    @abstractmethod
    def send(meter: str, metric_name: str, description: str,
             metric_result: Union[MetricResult, Sequence[MetricResult]]):
        for exporter in HypergoMetrics.get_metric_exporters():
            exporter.export(meter=meter, metric_name=metric_name, description=description, metric_result=metric_result)
