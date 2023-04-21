from abc import ABC, abstractmethod

from pymysqlreplication.row_event import RowsEvent


class TargetDb(ABC):
    @abstractmethod
    async def put(self, e: RowsEvent):
        ...

    async def close(self):
        ...
