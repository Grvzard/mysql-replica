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
        targetdb: TargetDb,
    ):
        self.connection_settings = connection_settings
        self.only_schemas = only_schemas
        self.filepos_logger = filepos_logger
        self.working = True
        self.db = targetdb

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

        await self.db.open()
        try:
            while self.working:
                await self._read_stream(stream)
                _tasks = [asyncio.sleep(4), self.db.flush()]
                await asyncio.gather(*_tasks)

        finally:
            logger.info("closing mysql connections...")
            await conn.ensure_closed()
            await ctl_conn.ensure_closed()
            logger.info("closing targetdb...")
            await self.db.close()
            logger.info("replica stoped")

    def stop(self):
        self.working = False

    async def _read_stream(self, stream):
        async for event in stream:
            if not self.working:
                break

            # TODO: support multiple targetdb for different schemas
            # if event.schema in self.only_schemas:
            #     targetdbs = self._targetdbs[event.schema]
            #     for targetdb in targetdbs:
            #         await targetdb.put(event)

            event_time = time.asctime(time.localtime(event.timestamp))
            logger.info(f"{event.schema} > {event_time}")
            await self.db.put(event)

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
