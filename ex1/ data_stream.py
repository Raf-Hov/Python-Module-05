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
        self.my_list: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        self.my_list.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            processed = False
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    processed = True
                    break
            if not processed:
                print("DataStream error - Can't", end=" ")
                print(f"process element in stream: {stream}")

    def print_processors_stats(self) -> None:
        if not self.my_list:
            print("No processor found, no data")
        for proc in self.my_list:
            po = proc.__class__.__name__
            name = po.replace("Processor", " Processor")
            total = proc._rank
            remaining = len(proc._storage)
            print(f"{name}: total {total} items processed, ", end="")
            print(f"remaining {remaining} on processor")


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===\n")
    log1 = NumericProcessor()
    log2 = TextProcessor()
    log3 = LogProcessor()
    k = "Hello"

    print("Tumeric Processor...")
    print(f" Trying to validate input '42': {log1.validate(42)}")
    print(" Trying to validate input ", end="")
    print(f" '{k}': {log1.validate(k)}")
    try:
        print(" Test invalid ingestion of string", end=" ")
        print("'foo' without prior validation:")
        log1.ingest("foo")
    except TypeError as e:
        print(f" Got exception: {e}")
    my_list = [1, 2, 3, 4, 5]
    print(f" Processing data: {my_list}")
    log1.ingest(my_list)
    k = log1
    print(" Extracting 3 values...")
    for i in range(3):
        rank, value = log1.output()
        print(f" Numeric value {rank}: {value}")

    print("\nTesting Text Processor...")
    print(f" Trying to validate input '42': {log2.validate(42)}")
    my_list = ['Hello', 'Nexus', 'World']
    print(f" Processing data: {my_list}")
    print(" Extracting 1 value...")
    try:
        log2.ingest(my_list)
    except TypeError as e:
        print(f" Got exception: {e}")
    for i in range(1):
        rank, value = log2.output()
        print(f" Text value {rank}: {value}")

    print("\nTesting Log Processor...")
    print(f" Trying to validate input 'Hello': {log3.validate(k)}")
    my_dict = [{'log_level': 'NOTICE', 'log_message': 'Connection to server'},
               {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}]
    print(f" Processing data: {my_dict}")
    print(" Extracting 2 values...")
    try:
        log3.ingest(my_dict)
    except TypeError as e:
        print(f" Got exception: {e}")
    for i in range(2):
        rank, value = log3.output()
        print(f" Log entry {rank}: {value}")

    print("\nInitialize Data Stream...")
