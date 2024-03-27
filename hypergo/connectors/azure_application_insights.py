from typing import cast
from azure.monitor.opentelemetry.exporter import (
    AzureMonitorLogExporter,
    AzureMonitorTraceExporter,
    AzureMonitorMetricExporter,
)

from hypergo.loggers.hypergo_logger import LogExporter, TraceExporter
from hypergo.metrics.hypergo_metrics import MetricExporter
from hypergo.secrets import Secrets


class AzureApplicationInsights:
    def __init__(self, secrets: Secrets):
        self._log_exporter: LogExporter = AzureMonitorLogExporter.from_connection_string(
            conn_str=secrets.get("APPLICATIONINSIGHTS-CONNECTION-STRING"), disable_offline_storage=True
        )
        self._trace_exporter: TraceExporter = cast(
            TraceExporter,
            AzureMonitorTraceExporter.from_connection_string(
                conn_str=secrets.get("APPLICATIONINSIGHTS-CONNECTION-STRING"), disable_offline_storage=True
            ),
        )
        self._metric_exporter: MetricExporter = AzureMonitorMetricExporter.from_connection_string(
            conn_str=secrets.get("APPLICATIONINSIGHTS-CONNECTION-STRING"), disable_offline_storage=True
        )

    def __del__(self) -> None:
        self.log_exporter.shutdown()
        self.trace_exporter.shutdown()
        self.metric_exporter.shutdown()

    @property
    def log_exporter(self) -> LogExporter:
        return self._log_exporter

    @property
    def trace_exporter(self) -> TraceExporter:
        return self._trace_exporter

    @property
    def metric_exporter(self) -> MetricExporter:
        return self._metric_exporter
