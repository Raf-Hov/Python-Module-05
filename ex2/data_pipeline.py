from abc import ABC, abstractmethod
from typing import Any, Protocol


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
        print("Initialize Data Stream...")
        self._first1 = True
        self._my_list: list[tuple[int, str]] = []

    def set_name(self, name: DataProcessor):  # petqa haskanal
        self.name = name.__class__.__name__
        self.name = self.name.replace("Processor", " Processor").strip()

    def register_processor(self, proc: DataProcessor) -> None:
        self._proc_list.append(proc)
        self.set_name(proc)
        print(f"Registering {self.name}")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for i in self._proc_list:
            for _ in range(nb):
                self._my_list.append(i.output())
            plugin.process_output(self._my_list)
            self._my_list = []

    def process_stream(self, stream: list[Any]) -> None:
        if self._first1:
            self._first1 = False
            print(f"\nSend first batch of data on stream: {stream}")
        else:
            print(f"\nSend another batch of data: {stream}")
        for item in stream:
            for i in self._proc_list:
                if i.validate(item):
                    i.ingest(item)
                    break
                else:
                    print("DataStream error - Can't", end=" ")
                    print(f"process element in stream: {item}")
        return

    def print_processors_stats(self) -> None:
        print("\n== DataStream statistics ==")
        if all(not i for i in self._proc_list):
            print("No processor found, no data\n")
            return
        else:
            for i in self._proc_list:
                if i._rank:
                    self.set_name(i)
                    print(f"{self.name}: total {i._rank} items", end=" ")
                    print(f"processed, remaining {i.counter}", end=" ")
                    print("on processor")


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None:
        pass


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("CSV Output:")
        my_list: list = []
        my_str: str = ""
        for i in data:
            if i is not None:
                _, s = i
                my_list.append(s)
        my_str = my_str + ",".join(my_list)
        print(my_str)


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        print("JSON Output:")
        my_dict = {}
        for i in data:
            if i is not None:
                rank, s = i
                name = "item_" + str(rank)
                my_dict[name] = s
        print(my_dict)


if __name__ == "__main__":
    print("=== Code Nexus - Data Pipeline ===\n")
    datast = DataStream()
    csv = CSVPlugin()
    json = JSONPlugin()
    log1 = NumericProcessor()
    log2 = TextProcessor()
    log3 = LogProcessor()
    batch = [
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
    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [
            {'log_level': 'ERROR',
             'log_message': '500 server crash'},
            {'log_level': 'NOTICE',
             'log_message': 'Certificateexpires in 10 days'}
        ],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    datast.print_processors_stats()
    datast.register_processor(log1)
    datast.register_processor(log2)
    datast.register_processor(log3)
    datast.process_stream(batch)
    print()
    datast.print_processors_stats()
    print("\nSend 3 processed data from each processor to a CSV plugin:")
    datast.output_pipeline(3, csv)
    datast.print_processors_stats()
    datast.process_stream(batch2)
    datast.print_processors_stats()
    print("\nSend 5 processed data from each processor to a JSON plugin:")
    datast.output_pipeline(5, json)
    datast.print_processors_stats()
