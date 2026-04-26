from abc import ABC, abstractmethod
from typing import Any


class MyError(Exception):
    def __init__(self, message=None):
        super().__init__(message)


class DataProcessor(ABC):
    def __init__(self, data: Any):
        self._storage = data

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        pass


class NumericProcessor(DataProcessor):
    def __init__(self, _new_data: Any):
        super().__init__(_new_data)

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            if isinstance(data, bool):
                return False
            return True

        if isinstance(data, list):
            if all(isinstance(x, (int, float)) for x in data):
                if all(isinstance(x, bool) for x in data):
                    return True
        return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for item in data:
                    self._storage.append(str(item))
            else:
                self._storage.append(str(data))
        else:
            raise MyError()


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            if all(isinstance(item, str) for item in data):
                return True
        return False

    def ingest(self, data: Any) -> None:
        if self.validate(data):
            for item in data:
                self._storage.append(item)
        else:
            raise MyError()


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        pass

    def ingest(self, data: Any) -> None:
        pass
