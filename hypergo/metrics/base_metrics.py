import platform
from importlib.metadata import version
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple, Union
from datetime import datetime, timezone
from dataclasses import dataclass, asdict


@dataclass(frozen=True, slots=True)
class MetricResult:
    unit: str
    value: Union[float, int]
    name: Optional[str] = None
    timestamp: Optional[datetime] = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(
            self,
            dict_factory=lambda fields: {
                key: value if not isinstance(value, datetime) else str(value) for key, value in fields
            },
        )


@dataclass(frozen=True, slots=True)
class Meter:
    meter_name: str
    metric_group_name: str
    result: MetricResult
    description: str
    system: str = platform.uname().system
    client: str = platform.uname().node
    sdk_version: str = version("hypergo")

    def to_dict(self) -> Dict[str, Any]:
        _result: Dict[str, Any] = {
            "meter_name": self.meter_name,
            "meter_group_name": self.metric_group_name,
            "description": self.description,
            "system": self.system,
            "client": self.client,
            "sdk_version": self.sdk_version,
        }
        _result.update(self.result.to_dict())
        return _result


class ExecutionTimeMetrics(ABC):
    @staticmethod
    @abstractmethod
    def function_total_execution_time(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class ResourceUsageMetrics(ABC):
    @staticmethod
    @abstractmethod
    def cpu_usage(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def memory_usage(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def disk_io_bytes(
        popped_result: Union[Tuple[MetricResult, MetricResult], None] = None
    ) -> Tuple[MetricResult, MetricResult]:
        pass

    @staticmethod
    @abstractmethod
    def disk_io_time(
        popped_result: Union[Tuple[MetricResult, MetricResult], None] = None
    ) -> Tuple[MetricResult, MetricResult]:
        pass


class ThroughputMetrics(ABC):
    @staticmethod
    @abstractmethod
    def requests_per_second(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def concurrency(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def queue_length(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class ErrorMetrics(ABC):
    @staticmethod
    @abstractmethod
    def error_rate(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def error_types(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def error_messages(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class LatencyMetrics(ABC):
    @staticmethod
    @abstractmethod
    def function_start_latency(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def end_to_end_latency(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class DependencyMetrics(ABC):
    @staticmethod
    @abstractmethod
    def external_service_calls(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def dependency_health(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class ResourceUtilizationMetrics(ABC):
    @staticmethod
    @abstractmethod
    def database_queries(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def cache_hits_and_misses(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class ConcurrencyMetrics(ABC):
    @staticmethod
    @abstractmethod
    def concurrent_executions(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def queue_wait_time(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class CostMetrics(ABC):
    @staticmethod
    @abstractmethod
    def resource_costs(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def resource_utilization_vs_cost(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass


class SecurityMetrics(ABC):
    @staticmethod
    @abstractmethod
    def authentication_attempts(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass

    @staticmethod
    @abstractmethod
    def security_alerts(popped_result: Union[MetricResult, None] = None) -> MetricResult:
        pass
