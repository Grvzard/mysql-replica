import time
import logging
import asyncio
from random import randint
from typing import List, Dict

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import WriteRowsEvent

from .gtid_logger import GtidLogger
from .targetdb import TargetDb


logger = logging.getLogger(__name__)


class Replica:
    def __init__(
        self,
        connection_settings: Dict,
        only_schemas: List[str],
        gtid_logger: GtidLogger,
        targetdb: TargetDb,
    ):
        self.connection_settings = connection_settings
        self.only_schemas = only_schemas
        self.gtid_logger = gtid_logger
        self.working = 1
        self.db = targetdb

    async def run(self) -> None:
        try:
            _ = self.gtid_logger.get_next()
        except Exception as e:
            logger.error(e)
            return

        stream = BinLogStreamReader(
            connection_settings=self.connection_settings,
            only_schemas=self.only_schemas,
            only_events=[WriteRowsEvent, GtidEvent],
            # auto_position = gtid,
            server_id=randint(100, 100000000),
            blocking=False,
            slave_heartbeat=60,
        )

        await self.db.open()

        while self.working:
            stream.auto_position = self.gtid_logger.get_next()
            await self._read_stream(stream)

            await asyncio.sleep(4)

        logger.info("stoping...")
        await self.db.close()
        logger.info("db closed safely")

    async def _read_stream(self, stream):
        do_next = False
        while event := stream.fetchone():
            if isinstance(event, GtidEvent):
                if not self.working:
                    break
                logger.info(event)
                do_next = self.gtid_logger.set_next(event.gtid)
            elif not do_next:
                continue
            else:
                do_next = False
                if event.__class__ in (WriteRowsEvent,):
                    event_time = time.asctime(time.localtime(event.timestamp))
                    logger.debug(f"{event.schema} > {event_time}")
                    await self.db.put(event)
