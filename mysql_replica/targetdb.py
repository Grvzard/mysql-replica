from abc import ABC, abstractmethod


class TargetDb(ABC):
    @abstractmethod
    async def put(self, event):
        ...

    async def open(self):
        ...

    async def close(self):
        ...

    async def flush(self):
        ...
