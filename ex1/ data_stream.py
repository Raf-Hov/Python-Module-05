from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._storage: list = []
        self._rank = 0
        self.total_atribut = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if self._storage:
            return self._storage.pop(0)
        raise IndexError("Storage is empty")


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True

        if isinstance(data, list):
            if all(isinstance(x, (int, float)) and not isinstance(x, bool)
                   for x in data):
                return True
        return False

    def ingest(self, data: int | float | list[int | float]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for item in data:
                    self._storage.append((self._rank, str(item)))
                    self._rank += 1
            else:
                self._storage.append((self._rank, str(data)))
                self._rank += 1
        else:
            raise TypeError("Improper numeric data")


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list):
            if all(isinstance(item, str) for item in data):
                return True
        return False

    def ingest(self, data: str | list[str]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for i in data:
                    self._storage.append((self._rank, i))
                    self._rank += 1
            else:
                self._storage.append((self._rank, data))
                self._rank += 1
        else:
            raise TypeError("Improper text data")


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, dict):
            if all(isinstance(i, str) for i in data.keys()):
                if all(isinstance(i, str) for i in data.values()):
                    return True

        if isinstance(data, list):
            self.__k = 0
            if all(isinstance(i, dict) for i in data):
                for j in data:
                    if all(isinstance(i, str) for i in j.keys()):
                        if all(isinstance(i, str) for i in j.values()):
                            self.__k += 1
                            if self.__k == len(data):
                                return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        if self.validate(data):
            if isinstance(data, list):
                for item in data:
                    form = ": ".join(item.values())
                    self._storage.append((self._rank, form))
                    self._rank += 1
            else:
                for item in data.items():
                    form: str = ""
                    form = ": ".join(item)
                    self._storage.append((self._rank, form))
                    self._rank += 1
        else:
            raise TypeError("Improper log data")


class DataStream:
    def __init__(self):
        self.proc_list: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.proc_list.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            booled = False
            for i in self.proc_list:
                if i.validate(item):
                    i.ingest(item)
                    booled = True
                    break
            if not booled:
                print("DataStream error - Can't", end=" ")
                print(f"process element in stream: {stream}")


if __name__ == "__main__":
    log1 = NumericProcessor()
    log2 = TextProcessor()
    log3 = LogProcessor()
    datast = DataStream()
    datast.process_stream(log1)
    datast.process_stream(log2)
    datast.process_stream(log3)
    stream = [
            'Hello world',
            [3.14, -1, 2.71],
            [
                {'log_level': 'WARNING',
                 'log_message': 'Telnet access! Use ssh instead'},
                {'log_level': 'INFO',
                 'log_message': 'User wil isconnected'}
            ],
            42,
            ['Hi', 'five']
            ]
    datast.register_processor(stream)
