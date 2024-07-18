from abc import abstractmethod
from hypergo.metrics.base_metrics import MetricResult
from typing import Any, Sequence


class MetricExporter:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.args = args
        self.kwargs = kwargs
        self.__hash = self.__get_hash()

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "__hash":
            raise AttributeError('''Can't set attribute "{0}"'''.format(__name))
        elif __name == f"_{self.__class__.__name__}__hash":
            if hasattr(self, __name):
                raise AttributeError('''Can't set attribute "{0}"'''.format(__name))
        object.__setattr__(self, __name, __value)

    def __get_hash(self) -> int:
        _hash: int = hash(None)
        for arg in self.args:
            _hash += hash(arg)
        for _, v in sorted(self.kwargs.items(), key=lambda x: x[0]):
            _hash += hash(v)
        return _hash

    @abstractmethod
    def export(self, meter: str, metric_name: str, description: str,
               metric_result: MetricResult | Sequence[MetricResult]) -> None:
        raise NotImplementedError("Derived class must implement export method")

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, MetricExporter) and (self.kwargs == __value.kwargs) and (self.args == __value.args)

    def __hash__(self) -> int:
        return self.__hash
