import json
from abc import abstractmethod
from collections.abc import Iterable
from sys import stdout
from hypergo.metrics.base_metrics import MetricResult
from typing import List, Dict, Any, IO, Sequence


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


class ConsoleMetricExporter(MetricExporter):
    def __init__(self, out: IO = stdout, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.out = out

    def export(self, meter: str, metric_name: str, description: str,
               metric_result: MetricResult | Sequence[MetricResult]) -> None:
        if isinstance(metric_result, Iterable):
            result: List[Dict[str, Any]] = []
            for record in metric_result:
                _result: Dict[str, Any] = record.to_dict()
                _result.update({"meter": meter, "metric_name": metric_name, "description": description})
                result.append(_result)
        else:
            result: Dict[str, Any] = metric_result.to_dict()
            result.update({"meter": meter, "metric_name": metric_name, "description": description})

        self.out.write(json.dumps(result, indent=4))
        self.out.flush()
