import json
from abc import abstractmethod
from collections.abc import Iterable
from sys import stdout
from typing import cast, List, Dict, Any, TextIO, Sequence
from hypergo.metrics.base_metrics import Meter, MetricResult


class MetricExporter:
    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            self.__setattr__(k, v)
        self.__hash = self.__get_hash()
        self.__result_set: List[Meter] = []

    def __del__(self) -> None:
        self.shutdown()

    def shutdown(self) -> None:
        while self.__result_set and len(self.__result_set) > 0:
            self.flush()

    def __setattr__(self, __name: str, __value: Any) -> None:
        if __name == "__hash":
            raise AttributeError(f"Can't set attribute {__name}")
        if __name == f"_{self.__class__.__name__}__hash":
            if hasattr(self, __name):
                raise AttributeError(f"Can't set attribute {__name}")
        object.__setattr__(self, __name, __value)

    def __get_hash(self) -> int:
        _hash: int = hash(None)
        for _, v in sorted(vars(self).items(), key=lambda x: x[0]):
            _hash += hash(v)
        return _hash

    def get_current_metrics(self) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for meter in self.__result_set:
            result.append(meter.to_dict())
        return result

    def flush(self) -> None:
        if self.export():
            self.__result_set.clear()

    def send(
        self, meter: str, metric_name: str, description: str, metric_result: MetricResult | Sequence[MetricResult]
    ) -> None:
        if isinstance(metric_result, Iterable):
            for result in metric_result:
                self.__result_set.append(
                    Meter(meter_name=meter, metric_group_name=metric_name, result=result, description=description)
                )
        else:
            self.__result_set.append(
                Meter(meter_name=meter, metric_group_name=metric_name, result=metric_result, description=description)
            )

    @abstractmethod
    def export(self) -> bool:
        raise NotImplementedError("export function must be implemented by the subclass")

    def __eq__(self, __value: object) -> bool:
        return isinstance(__value, MetricExporter) and (vars(self) == vars(__value))

    def __hash__(self) -> int:
        return self.__hash


class ConsoleMetricExporter(MetricExporter):
    def __init__(self, out: TextIO = stdout) -> None:
        super().__init__(out=out)

    def export(self) -> bool:
        out: TextIO = cast(TextIO, getattr(self, "out"))
        out.write(json.dumps(self.get_current_metrics(), indent=4))
        out.flush()
        return True
