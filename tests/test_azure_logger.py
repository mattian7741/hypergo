import os
import sys
import logging
from uuid import uuid4
import unittest
from unittest.mock import MagicMock, patch

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from hypergo.loggers.azure_logger import AzureLogger


class TestAzureLogger(unittest.TestCase):

    @patch.dict(os.environ, {"APPLICATIONINSIGHTS-CONNECTION-STRING": f"InstrumentationKey={uuid4()};IngestionEndpoint=https://eastus2-3.in.applicationinsights.azure.com/;LiveEndpoint=https://eastus2.livediagnostics.monitor.azure.com/"})
    @patch("hypergo.loggers.hypergo_logger.HypergoTracer.get_tracer")
    @patch("hypergo.secrets.LocalSecrets")
    def test_azure_logger(self, mock_secrets, mock_get_tracer):
        mock_secrets.get.return_value = os.environ["APPLICATIONINSIGHTS-CONNECTION-STRING"]
        mock_get_tracer.return_value = MagicMock()

        # Create an instance of AzureLogger
        logger = AzureLogger(secrets=mock_secrets)

        logger.log("Test message", level=logging.INFO)
        mock_get_tracer.assert_called_with('hypergo.loggers.azure_logger')


if __name__ == '__main__':
    unittest.main()
