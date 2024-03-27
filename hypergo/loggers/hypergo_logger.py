from logging import Handler
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import LogExporter, BatchLogRecordProcessor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SpanExporter as TraceExporter, BatchSpanProcessor
from opentelemetry.trace import set_tracer_provider, get_tracer, Tracer


class HypergoLogger:
    is_logger_provider_set: bool = False
    logger_provider: LoggerProvider = LoggerProvider()

    @staticmethod
    def set_log_exporter(log_exporter: LogExporter) -> None:
        if not HypergoLogger.is_logger_provider_set:
            set_logger_provider(HypergoLogger.logger_provider)
            HypergoLogger.is_logger_provider_set = True
        HypergoLogger.logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))

    @staticmethod
    def get_handler(log_level: int) -> Handler:
        return LoggingHandler(level=log_level, logger_provider=HypergoLogger.logger_provider)


class HypergoTracer:
    is_tracer_provider_set: bool = False
    tracer_provider: TracerProvider = TracerProvider()

    @staticmethod
    def set_trace_exporter(trace_exporter: TraceExporter) -> None:
        if not HypergoTracer.is_tracer_provider_set:
            set_tracer_provider(HypergoTracer.tracer_provider)
            HypergoTracer.is_tracer_provider_set = True
        HypergoTracer.tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter=trace_exporter))

    @staticmethod
    def get_tracer(name: str) -> Tracer:
        return get_tracer(name)
