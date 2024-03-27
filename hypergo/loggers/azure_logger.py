import logging
from typing import cast, Optional, Union
from hypergo.loggers.base_logger import BaseLogger
from hypergo.loggers.hypergo_logger import HypergoLogger, HypergoTracer
from hypergo.metrics.hypergo_metrics import HypergoMetric
from hypergo.connectors.azure_application_insights import AzureApplicationInsights
from hypergo.secrets import Secrets


class AzureLogger(BaseLogger, AzureApplicationInsights):

    def __init__(
        self,
        secrets: Secrets,
        name: Optional[str] = None,
        log_level: int = logging.DEBUG,
        log_format: Optional[Union[str, logging.Formatter]] = None,
    ):
        BaseLogger.__init__(self, name=name, log_level=log_level, log_format=log_format)
        AzureApplicationInsights.__init__(self, secrets=secrets)
        HypergoLogger.set_log_exporter(log_exporter=self.log_exporter)
        HypergoTracer.set_trace_exporter(trace_exporter=self.trace_exporter)
        HypergoMetric.set_metric_exporter(self.metric_exporter)

    def get_handler(self) -> logging.Handler:
        return HypergoLogger.get_handler(self.log_level)

    def log(self, message: str, level: Optional[int] = None) -> None:
        if level is None:
            level = self.log_level
        self.logger.setLevel(level)
        # Get a tracer for the current module.
        with HypergoTracer.get_tracer(__name__).start_as_current_span(
            cast(str, self.name), attributes={"trace-type": "user"}
        ):
            # Log the message with the specified level
            self.logger.log(level, message)
