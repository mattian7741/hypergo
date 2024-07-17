from threading import Lock
from typing import Any, Dict, Set, Sequence, Union, Optional, Iterable, Callable, Mapping, cast
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricReader,
    MetricExporter,
    ConsoleMetricExporter,
    AggregationTemporality,
)
from opentelemetry.sdk.metrics import MeterProvider, Meter, ObservableGauge
from opentelemetry.metrics import CallbackOptions, Observation
from hypergo.metrics.metric_exporter import MetricExporter as HypergoMetricExporter
from hypergo.metrics.base_metrics import MetricResult
from hypergo.utility import get_random_string


deltaTemporality = {
    ObservableGauge: AggregationTemporality.DELTA,
}


class OtelMetricsExporter(HypergoMetricExporter):

    _current_meter_provider: Union[MeterProvider, None] = None
    _current_metric_reader: MetricReader = InMemoryMetricReader(preferred_temporality=deltaTemporality)
    _otel_metric_lock: Lock = Lock()

    def __init__(self, metric_exporter: MetricExporter = ConsoleMetricExporter(preferred_temporality=deltaTemporality)):
        self._metric_exporter = metric_exporter

    @staticmethod
    def __set_meter_provider() -> None:
        if not OtelMetricsExporter._current_meter_provider:
            OtelMetricsExporter._current_meter_provider = MeterProvider(metric_readers=cast(Sequence[Any],
                                                                        [OtelMetricsExporter._current_metric_reader]))

    @staticmethod
    def __get_meter(name: str) -> Meter:
        with OtelMetricsExporter._otel_metric_lock:
            OtelMetricsExporter.__set_meter_provider()
        return OtelMetricsExporter._current_meter_provider.get_meter(name=name)

    @staticmethod
    def __create_observable_gauge(
        meter_name: str,
        metric_name: str,
        metric_result: Union[MetricResult, Sequence[MetricResult]],
        description: Optional[str] = None,
    ) -> None:
        def create_callback(
            value: Union[float, int], attributes: Dict[str, Union[str, None]]
        ) -> Callable[[CallbackOptions], Iterable[Observation]]:
            def func(options: CallbackOptions) -> Iterable[Observation]:
                yield Observation(value, attributes=cast(Mapping[str, str], attributes))

            return func

        _metric_values: Sequence[MetricResult] = ()
        _callbacks: Set[Callable[[CallbackOptions], Iterable[Observation]]] = set()
        metric_unit: Union[str, None] = None

        _metric_values = metric_result if isinstance(metric_result, Sequence) else tuple([metric_result])
        for _metric_result in _metric_values:
            name, unit, value, timestamp = (
                _metric_result.name,
                _metric_result.unit,
                _metric_result.value,
                _metric_result.timestamp,
            )
            if not metric_unit:
                metric_unit = unit
            elif metric_unit != unit:
                raise ValueError(f"All MetricResult(s) for {metric_name} should have the same unit value")
            _callbacks.add(
                create_callback(
                    value=value,
                    attributes={
                        "unit": unit,
                        "name": name,
                        "timestamp": timestamp,
                        "function_name": meter_name,
                        "metric_name": metric_name
                    },
                )
            )

        meter: Meter = OtelMetricsExporter.__get_meter(name=meter_name)

        meter.create_observable_gauge(
            name=metric_name+"-"+get_random_string(20),
            callbacks=cast(Sequence[Callable[[CallbackOptions], Iterable[Observation]]], _callbacks),
            unit=cast(str, metric_unit),
            description=cast(str, description),
        )

    def export(self, meter: str, metric_name: str, description: str,
               metric_result: Union[MetricResult, Sequence[MetricResult]]) -> None:
        OtelMetricsExporter.__create_observable_gauge(meter_name=meter, metric_name=metric_name,
                                                      description=description, metric_result=metric_result)
        self._metric_exporter.export(metrics_data=cast(InMemoryMetricReader,
                                                       OtelMetricsExporter._current_metric_reader.get_metrics_data()),
                                     timeout_millis=60000)
