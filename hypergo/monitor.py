from functools import wraps
from typing import Any, Callable, Dict, Type, Union, cast

from hypergo.executor import Executor
from hypergo.metrics import custom_metrics_metadata
from hypergo.metrics.base_metrics import MetricResult
from hypergo.metrics.hypergo_metrics import HypergoMetric, Meter

__all__ = ["collect_metrics"]


def find_class_instance(class_type: Type[Any], *args: Any, **kwargs: Any) -> Union[Any, None]:
    for arg in args:
        if isinstance(arg, class_type):
            return arg

    for value in kwargs.values():
        if isinstance(value, class_type):
            return value
    return None


def collect_metrics(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        executor: Executor = cast(Executor, find_class_instance(Executor, *args, **kwargs))
        function_name: str = executor.callback.__name__
        metric_callbacks: Dict[Callable[[Union[MetricResult, None]], MetricResult], MetricResult] = {}
        for custom_metrics in custom_metrics_metadata:
            for metric_callback in HypergoMetric.get_metrics_callback(
                package=custom_metrics.package,
                module_name=custom_metrics.module_name,
                class_name=custom_metrics.class_name,
            ):
                metric_callbacks.setdefault(
                    cast(Callable[[Union[MetricResult, None]], MetricResult], metric_callback),
                    metric_callback(cast(MetricResult, None)),
                )

        result: Any = func(*args, **kwargs)
        meter: Meter = HypergoMetric.get_meter(name=function_name)
        for metric_callback, value in metric_callbacks.items():
            HypergoMetric.send(
                meter=meter,
                metric_name=metric_callback.__name__,
                metric_result=metric_callback(value),
                description=metric_callback.__doc__,
            )
        HypergoMetric.collect()
        return result

    return wrapper
