from pathlib import Path
import time
import logging
import asyncio
from random import randint
from typing import List, Dict

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent
from tenacity import Retrying, stop_after_delay

from .filepos_logger import FileposLogger
from .targetdb import TargetDb


logger = logging.getLogger(__name__)


class Replica:
    def __init__(
        self,
        connection_settings: Dict,
        only_schemas: List[str],
        filepos_fpath: str,
        targetdbs: List[TargetDb],
    ):
        self.connection_settings = connection_settings
        self.only_schemas = only_schemas
        self.filepos_fpath = Path(filepos_fpath)
        self.working = True
        self._targetdbs = targetdbs

    async def run(self) -> None:
        self.filepos_logger = FileposLogger(self.filepos_fpath)

        try:
            file, pos = self.filepos_logger.get_next()
        except Exception as e:
            logger.error(e)
            return

        stream = BinLogStreamReader(
            connection_settings=self.connection_settings,
            server_id=randint(100, 100000000),
            only_schemas=self.only_schemas,
            only_events=[WriteRowsEvent],
            log_file=file,
            log_pos=pos,
            resume_stream=True,
            blocking=False,
            slave_heartbeat=60,
        )

        [await db.open() for db in self._targetdbs]
        try:
            while self.working:
                await self._read_stream(stream)
                _tasks = [db.flush() for db in self._targetdbs]
                _tasks.append(asyncio.sleep(4))
                await asyncio.gather(*_tasks)

        finally:
            self.filepos_logger.close()
            logger.info("closing targetdb...")
            [await db.close() for db in self._targetdbs]
            logger.info("replica stoped")

    def stop(self):
        self.working = False

    async def _read_stream(self, stream):
        while True:
            if not self.working:
                break
            try:
                event = Retrying(stop=stop_after_delay(10)).wraps(stream.fetchone)()
            except Exception as exc:
                logger.error(f"fetchone failed: {exc}")
                stream.close()
                break

            event_time = time.asctime(time.localtime(event.timestamp))
            logger.info(f"{event.schema} > {event_time}")
            [await db.put(event) for db in self._targetdbs]

            try:
                logger.debug(f"filepos: {stream.log_file}, {stream.log_pos}")
                self.filepos_logger.set_next(stream.log_file, stream.log_pos)
            except Exception as exc:
                logger.error(
                    "filepos_logger set failed at ",
                    f"{stream.log_file}, {stream.log_pos}: ",
                    f"{exc}",
                )
                self.stop()
                break
