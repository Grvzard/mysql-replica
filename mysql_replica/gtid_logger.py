from abc import ABC, abstractmethod
from typing import final, Tuple
from pathlib import Path

from pymysqlreplication.gtid import Gtid


class GtidLogger(ABC):
    def __init__(self):
        ...

    def close(self):
        ...

    @abstractmethod
    def read(self) -> Tuple[str, str]:
        ...

    @abstractmethod
    def write(self, gtid_executed: str, gtid_next: str) -> None:
        ...

    @final
    def get_next(self) -> str:
        self.gtid_executed, self.gtid_next = map(Gtid, self.read())
        return str(self.gtid_executed + self.gtid_next)

    @final
    def set_next(self, gtid: str) -> bool:
        self.gtid_executed += self.gtid_next

        gtid_next = Gtid(gtid)
        if gtid_next in self.gtid_executed:
            return False

        self.gtid_next = gtid_next
        self.write(str(self.gtid_executed), str(self.gtid_next))
        return True


class GtidFile(GtidLogger):
    def __init__(self, filename: str):
        self.name = filename
        Path(self.name).touch(exist_ok=True)
        self.fp = open(self.name, "r+")

    def __enter__(self):
        return self

    def __exit__(self, **exec_info):
        self.fp.close()

    def read(self):
        self.fp.seek(0)
        return self.fp.readline().split(' ')

    def write(self, gtid_executed: str, gtid_next: str):
        self.fp.seek(0)
        self.fp.write(' '.join((gtid_executed, gtid_next)))
        self.fp.truncate()
        self.fp.flush()
