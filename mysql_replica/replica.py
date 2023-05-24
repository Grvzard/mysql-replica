import time
import logging
import asyncio
from random import randint
from typing import List, Dict

from asyncmy import connect
from asyncmy.replication import BinLogStream
from asyncmy.replication.row_events import WriteRowsEvent

from .filepos_logger import FileposLogger
from .targetdb import TargetDb


logger = logging.getLogger(__name__)


class Replica:
    def __init__(
        self,
        connection_settings: Dict,
        only_schemas: List[str],
        filepos_logger: FileposLogger,
        targetdbs: List[TargetDb],
    ):
        self.connection_settings = connection_settings
        self.only_schemas = only_schemas
        self.filepos_logger = filepos_logger
        self.working = True
        self._targetdbs = targetdbs

    async def run(self) -> None:
        try:
            file, pos = self.filepos_logger.get_next()
        except Exception as e:
            logger.error(e)
            return

        conn = await connect(**self.connection_settings)
        ctl_conn = await connect(**self.connection_settings)
        stream = BinLogStream(
            connection=conn,
            ctl_connection=ctl_conn,
            server_id=randint(100, 100000000),
            only_schemas=self.only_schemas,
            only_events=[WriteRowsEvent],
            master_log_file=file,
            master_log_position=pos,
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
            logger.info("closing mysql connections...")
            await conn.ensure_closed()
            await ctl_conn.ensure_closed()
            logger.info("closing targetdb...")
            [await db.close() for db in self._targetdbs]
            logger.info("replica stoped")

    def stop(self):
        self.working = False

    async def _read_stream(self, stream):
        async for event in stream:
            if not self.working:
                break

            event_time = time.asctime(time.localtime(event.timestamp))
            logger.info(f"{event.schema} > {event_time}")
            [await db.put(event) for db in self._targetdbs]

            try:
                logger.debug(f"filepos: {stream._master_log_file}, {stream._master_log_position}")
                self.filepos_logger.set_next(stream._master_log_file, stream._master_log_position)
            except Exception as exc:
                logger.error(
                    "filepos_logger set failed at ",
                    f"{stream._master_log_file}, {stream._master_log_position}: ",
                    f"{exc}",
                )
                self.stop()
                break
