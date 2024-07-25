import os
import sys
import unittest
from uuid import uuid4
from unittest.mock import patch

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


from hypergo.message import MessageType
from hypergo.config import ConfigType
from hypergo.executor import Executor
from hypergo.loggers.azure_logger import AzureLogger
from hypergo.monitor import collect_metrics
from hypergo.metrics.hypergo_metrics import HypergoMetrics
from hypergo.metrics.otel_metrics_exporter import OtelMetricsExporter


class TestAzureMonitor(unittest.TestCase):
    def setUp(self) -> None:
        self.message: MessageType = {
            "routingkey": "workday.scheduled"
        }
        return super().setUp()

    @collect_metrics
    def __mock_send_message(self, executor: Executor, message: MessageType, config: ConfigType):
        try:
            for _ in executor.execute(message):
                _
        except Exception:
            pass

    @patch.dict(os.environ, {"APPLICATIONINSIGHTS-CONNECTION-STRING": f"InstrumentationKey={uuid4()};IngestionEndpoint=https://eastus2-3.in.applicationinsights.azure.com/;LiveEndpoint=https://eastus2.livediagnostics.monitor.azure.com/"})
    @patch("hypergo.metrics.otel_metrics_exporter.OtelMetricsExporter.export")
    @patch("hypergo.metrics.otel_metrics_exporter.OtelMetricsExporter.send")
    @patch("hypergo.secrets.LocalSecrets")
    def test_azure_monitor(self, mock_secrets, mock_send, mock_export):
        cfg: ConfigType = {
                                "version": "2.0.0",
                                "namespace": "datalink",
                                "name": "workdayapiconnector",
                                "package": "ldp-hcm-workday",
                                "lib_func": "hcm_workday.__main__.fetch_data",
                                "input_keys": ["workday.scheduled"],
                                "output_keys": ["workday.batch.complete.json"],
                                "input_bindings": [],
                                "output_bindings": ["message.body.json_data"],
                                "output_operations": {"pass_by_reference": ["message.body.json_data"]},
                                "trigger": "service-bus-topic"
                            }
        mock_secrets.get.return_value = os.environ["APPLICATIONINSIGHTS-CONNECTION-STRING"]
        # Create an instance of AzureLogger
        logger = AzureLogger(secrets=mock_secrets)
        HypergoMetrics.set_metric_exporter(OtelMetricsExporter(metric_exporter=logger.metric_exporter))
        executor = Executor(cfg, logger=logger)
        self.__mock_send_message(executor=executor, message=self.message, config=cfg)
        assert mock_send.call_count == 5
        assert mock_export.call_count == 5


if __name__ == '__main__':
    unittest.main()
