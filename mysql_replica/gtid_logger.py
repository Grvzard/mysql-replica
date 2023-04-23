from abc import ABC, abstractmethod
from typing import final
from pathlib import Path

from pymysqlreplication.gtid import Gtid


class GtidLogger(ABC):
    def __init__(self):
        ...

    def close(self):
        ...

    @abstractmethod
    def read(self) -> str:
        ...

    @abstractmethod
    def write(self, gtid_executed: str) -> None:
        ...

    @final
    def get_executed(self) -> str:
        self._executed = Gtid(self.read())
        return str(self._executed)

    @final
    def set_next(self, gtid: str) -> bool:
        self._next = Gtid(gtid)

        if self._next in self._executed:
            return False

        return True

    @final
    def set_executed(self) -> None:
        self._executed += self._next
        self.write(str(self._executed))


class GtidFile(GtidLogger):
    def __init__(self, filename: str):
        self.name = filename
        Path(self.name).touch(exist_ok=True)
        self.fp = open(self.name, "r+")

    def __enter__(self):
        return self

    def __exit__(self, **exec_info):
        self.fp.close()

    def read(self) -> str:
        self.fp.seek(0)
        return self.fp.readline()

    def write(self, gtid_executed: str) -> None:
        self.fp.seek(0)
        self.fp.write(gtid_executed)
        self.fp.truncate()
        self.fp.flush()
