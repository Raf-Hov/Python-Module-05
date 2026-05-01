from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):
    def __init__(self):
        self._storage: list = []
        self._rank = 0
        self.counter = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if self._storage:
            self.counter -= 1
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
                    self.counter += 1
            else:
                self._storage.append((self._rank, str(data)))
                self.counter += 1
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
                    self.counter += 1
            else:
                self._storage.append((self._rank, data))
                self.counter += 1
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
                    self.counter += 1
            else:
                for item in data.items():
                    form: str = ""
                    form = ": ".join(item)
                    self._storage.append((self._rank, form))
                    self._rank += 1
                    self.counter += 1
        else:
            raise TypeError("Improper log data")


class DataStream:
    def __init__(self):
        self._proc_list: list[DataProcessor] = []
        self._first = 1
        self._first1 = True

    def set_name(self, name: DataProcessor):  # petqa haskanal
        self.name = name.__class__.__name__
        self.name = self.name.replace("Processor", " Processor").strip()

    def register_processor(self, proc: DataProcessor) -> None:
        self._proc_list.append(proc)
        if self._first == 1:
            self.set_name(proc)
            print(f"\nRegistering {self.name}\n")
            self._first = 0
        elif self._first == 0:
            print("\nRegistering other data processors")
            print("Send the same/other batch again")
            self._first = -1
        elif self._first == -1:
            pass

    def process_stream(self, stream: list[Any]) -> None:
        if self._first1:
            print(f"Send first batch of data on stream: {stream}")
        else:
            pass
        for item in stream:
            for i in self._proc_list:
                if i.validate(item):
                    i.ingest(item)
                    break
                elif self._first1:
                    print("DataStream error - Can't", end=" ")
                    print(f"process element in stream: {item}")
                    self._first1 = False
        return

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if all(not i for i in self._proc_list):
            print("No processor found, no data")
            return
        else:
            for i in self._proc_list:
                if i._rank:
                    self.set_name(i)
                    print(f"{self.name}: total {i._rank} items", end=" ")
                    print(f"processed, remaining {i.counter}", end=" ")
                    print("on processor")


if __name__ == "__main__":
    print("Initialize Data Stream...")
    datast = DataStream()
    datast.print_processors_stats()
    log1 = NumericProcessor()
    log2 = TextProcessor()
    log3 = LogProcessor()
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
    datast.register_processor(log1)
    datast.process_stream(stream)
    datast.print_processors_stats()
    datast.register_processor(log2)
    datast.register_processor(log3)
    datast.process_stream(stream)
    datast.print_processors_stats()
    print("\nConsume some elements from the data processors:", end="")
    print(" Numeric 3, Text 2, Log 1")
    for _ in range(3):
        log1.output()
    for _ in range(2):
        log2.output()
    for _ in range(1):
        log3.output()
    datast.print_processors_stats()
