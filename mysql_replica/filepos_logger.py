from pathlib import Path
from typing import Tuple


class FileposLogger:
    def __init__(self, filename: str):
        self._filename = filename
        Path(self._filename).touch(exist_ok=True)
        self._fp = open(self._filename, "r+")
        self.filepos = ("", 0)

    def read(self) -> Tuple[str, str]:
        self._fp.seek(0)
        try:
            file, pos = self._fp.readline().split(" ")
            if not file or not pos:
                raise Exception("filepos is empty")
        except Exception as exc:
            raise exc
        return (file, pos)

    def write(self, filepos: Tuple[str, int]) -> None:
        self._fp.seek(0)
        self._fp.write(" ".join(str(_) for _ in filepos))
        self._fp.truncate()

    def get_next(self) -> Tuple[str, int]:
        file, pos = self.read()
        return file, int(pos)

    def set_next(self, file: str, pos: int):
        self.filepos = (file, int(pos))
        self.write(self.filepos)

    def close(self) -> None:
        self._fp.close()
