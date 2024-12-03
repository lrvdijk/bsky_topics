import asyncio
from collections import deque
from datetime import datetime, timedelta
from itertools import islice
import logging
import sys
from typing import Optional, Sequence, AsyncIterator

from rich.console import Console
from rich.status import Status
import orjson
import websockets
from websockets.asyncio.client import connect

from sqlalchemy import select
from sqlalchemy.exc import StatementError
from sqlalchemy.dialects.postgresql import insert

from bsky_topics.db import async_session
from bsky_topics.db.schema import Post

logger = logging.getLogger(__name__)


def exp_average(history: Sequence[int | float], alpha: float = 0.5):
    """Calculate the exponential average of a list of values.

    Gives priority to values at the front of the list.
    """
    exp_average = sum(
        (alpha if i < len(history)-1 else 1) * ((1-alpha)**i) * value
        for i, value in enumerate(history)
    )

    return exp_average


class CollectorMetrics:
    """
    Keeps track of collector events and calculates a running average
    """

    def __init__(self, max_history=5):
        self.max_history = 5

        self.num_received: int = 0
        self.num_inserted: int = 0

        self.history_received = deque([])
        self.history_inserted = deque([])

    def tick(self):
        self.history_received.appendleft(self.num_received)
        self.history_inserted.appendleft(self.num_inserted)

        while len(self.history_received) > self.max_history:
            self.history_received.pop()

        while len(self.history_inserted) > self.max_history:
            self.history_inserted.pop()

        self.num_received = 0
        self.num_inserted = 0

    def get_avg_received_per_tick(self):
        return exp_average(self.history_received)

    def get_avg_inserted_per_tick(self):
        return exp_average(self.history_inserted)


class JetstreamCollector:
    def __init__(self, console: Console, ws_hostname: str, batch_size: Optional[int] = 64):
        self.console: Console = console
        self.console_status: Status | None = None

        self.ws_url: str = f"wss://{ws_hostname}/subscribe?wantedCollections=app.bsky.feed.post"
        self.batch_size: int = batch_size
        self.batch = deque([])

        self.postgres_update_task: asyncio.Task | None = None
        self.stats_task: asyncio.Task | None = None

        self.metrics = CollectorMetrics()

    async def get_last_processed_message(self) -> int | None:
        """
        Obtain the timestamp of the processed post.

        Timestamp represents the number of microseconds since Unix epoch, which can be used
        in the Jetstream websocket connection URL.
        """

        async with async_session() as session:
            stmt = (select(Post.indexed_at)
                    .order_by(Post.indexed_at.desc())
                    .limit(1))
            results = await session.scalars(stmt)

            result = next(iter(results), None)
            if result:
                unix_epoch = datetime(1970, 1, 1)
                time_us = (result - unix_epoch) / timedelta(microseconds=1)

                return time_us

    async def gen_jetstream_url(self):
        """
        Generate the websocket URL.

        It queries the database to obtain the date and time of last processed post, and determines a cursor
        to be included in the URL when approriate.
        """

        ws_url = self.ws_url
        last_indexed = await self.get_last_processed_message()
        if last_indexed:
            # Substract two seconds
            last_indexed -= 2000
            ws_url += f"&cursor={last_indexed:.0f}"

        return ws_url

    async def read(self, ws: websockets.asyncio.connection.Connection) -> AsyncIterator[bytearray]:
        """
        Read messages from Websocket.

        To prevent unnecessary UTF-8 decoding, we specify `decode=False`. UTF-8 decoding is handled
        when parsing JSON with the `orjson` package.
        """

        try:
            while True:
                yield await ws.recv(decode=False)
        except websockets.exceptions.ConnectionClosedOK:
            return

    async def listen(self):
        # Start metrics tracking task
        self.stats_task = asyncio.create_task(self.update_stats())

        ws_url = await self.gen_jetstream_url()
        print("WS URL:", ws_url)

        # Factory keeps retrying to connect on failure/disconnects with exponential backoff mechanism
        ws_factory = connect(ws_url)
        async for ws in ws_factory:
            logger.info("Connected to jetstream.")

            try:
                async for msg in self.read(ws):
                    try:
                        data = orjson.loads(msg)
                    except orjson.JSONDecodeError as e:
                        logger.error("Could not parse JSON:")
                        logger.exception(e)
                        continue

                    if data.get('kind') != "commit":
                        continue

                    commit = data.get('commit', {})
                    if not commit:
                        continue

                    if commit.get('operation') != "create":
                        continue

                    if commit.get('collection') != "app.bsky.feed.post":
                        continue

                    record = commit.get('record', {})
                    if not record:
                        continue

                    if record.get('$type', "") != "app.bsky.feed.post":
                        continue

                    if not data.get('did'):
                        continue

                    if not commit.get('rkey'):
                        continue

                    if not commit.get('cid'):
                        continue

                    post_text = record.get('text').strip().replace('\x00', '')
                    if not post_text:
                        continue

                    self.batch.append({
                        'did': data['did'],
                        'rkey': commit['rkey'],
                        'cid': commit['cid'],
                        'post_text': post_text,
                        'language': record.get('langs', [])
                    })
                    self.metrics.num_received += 1

                    if len(self.batch) >= self.batch_size:
                        # If previous batch not inserted yet, wait until done
                        if self.postgres_update_task and not self.postgres_update_task.done():
                            await self.postgres_update_task

                        self.postgres_update_task = asyncio.create_task(
                            self.process_batch()
                        )
            except websockets.exceptions.ConnectionClosedError:
                # On disconnect, ensure we process remaining posts in the batch
                if self.postgres_update_task and not self.postgres_update_task.done():
                    await self.postgres_update_task

                await self.process_batch()

                # Update cursor in WS URL
                ws_factory.uri = await self.gen_jetstream_url()

                logger.warning("Connection closed. Reconnecting...")

    async def process_batch(self):
        if not self.batch:
            return

        async with async_session() as session:
            stmt = (insert(Post)
                    .on_conflict_do_nothing()
                    .returning(Post.id))

            batch_size = len(self.batch)

            try:
                result = await session.execute(
                    stmt,
                    self.batch
                )
                await session.commit()
            except StatementError as e:
                logger.exception(e)
                self.console.print(list(islice(self.batch, self.batch_size)))
                result = []

            # Remove processed entries
            for _ in range(batch_size):
                self.batch.popleft()

            inserted = list(result)
            self.metrics.num_inserted += len(inserted)

    async def update_stats(self):
        await asyncio.sleep(1)
        self.metrics.tick()

        if sys.stdout.isatty():
            recv_per_s = self.metrics.get_avg_received_per_tick()
            ins_per_s = self.metrics.get_avg_inserted_per_tick()

            print(f"Receiving {recv_per_s:.0f} msg/s, inserting {ins_per_s:.0f} msg/s", end='\r',
                  file=sys.stderr)

        self.stats_task = asyncio.create_task(self.update_stats())
