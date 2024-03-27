import time
from typing import Tuple, Union
import psutil
from hypergo.metrics.base_metrics import ExecutionTimeMetrics, ResourceUsageMetrics, MetricResult


class CustomMetrics(ExecutionTimeMetrics, ResourceUsageMetrics):
    @staticmethod
    def function_total_execution_time(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        """Record total execution time by function"""
        if not popped_result:
            return MetricResult(unit="second", value=time.time())
        return MetricResult(unit="second", name="total_execution_time", value=time.time() - popped_result.value)

    @staticmethod
    def cpu_usage(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        """Record CPU usage by function"""
        process = psutil.Process()
        if not popped_result:
            return MetricResult(unit="percent", value=process.cpu_percent(interval=0.1))
        return MetricResult(
            unit="percent", name="cpu_usage", value=abs(process.cpu_percent(interval=0.1) - popped_result.value)
        )

    @staticmethod
    def memory_usage(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        """Record memory usage by function"""
        process = psutil.Process()
        if not popped_result:
            return MetricResult(unit="bytes", value=process.memory_info().rss)
        return MetricResult(unit="bytes", name="memory_usage", value=process.memory_info().rss - popped_result.value)

    @staticmethod
    def disk_io_bytes(
        popped_result: Union[Tuple[MetricResult, MetricResult], None] = None
    ) -> Tuple[MetricResult, MetricResult]:
        """Record disk I/O usage in bytes by function"""
        if not popped_result:
            disk_io = psutil.disk_io_counters()
            return (
                MetricResult(unit="bytes", name="read_bytes", value=disk_io.read_bytes),
                MetricResult(unit="bytes", name="write_bytes", value=disk_io.write_bytes),
            )

        disk_io = psutil.disk_io_counters()

        read_bytes, write_bytes = popped_result

        return (
            MetricResult(unit="bytes", name="read_bytes", value=disk_io.read_bytes - read_bytes.value),
            MetricResult(unit="bytes", name="write_bytes", value=disk_io.write_bytes - write_bytes.value),
        )

    @staticmethod
    def disk_io_time(
        popped_result: Union[Tuple[MetricResult, MetricResult], None] = None
    ) -> Tuple[MetricResult, MetricResult]:
        """Record disk I/O usage in seconds by function"""
        if not popped_result:
            disk_io = psutil.disk_io_counters()
            return (
                MetricResult(unit="ms", name="read_time", value=disk_io.read_time),
                MetricResult(unit="ms", name="write_time", value=disk_io.write_time),
            )

        disk_io = psutil.disk_io_counters()

        read_time, write_time = popped_result

        return (
            MetricResult(unit="seconds", name="read_time", value=(disk_io.read_time - read_time.value) * 0.001),
            MetricResult(unit="seconds", name="write_time", value=(disk_io.write_time - write_time.value) * 0.001),
        )
