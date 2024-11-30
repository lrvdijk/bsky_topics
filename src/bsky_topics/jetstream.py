import asyncio
from collections import deque
from datetime import datetime, timedelta
import logging
import sys
from typing import Optional

import orjson
from websockets.asyncio.client import connect
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from bsky_topics.db import async_session
from bsky_topics.db.schema import Post

logger = logging.getLogger(__name__)


class JetstreamCollector:
    def __init__(self, ws_hostname: str, batch_size: Optional[int] = 64):
        self.ws_url: str = f"wss://{ws_hostname}/subscribe?wantedCollections=app.bsky.feed.post"
        self.batch_size: int = batch_size
        self.postgres_update_task: asyncio.Task | None = None
        self.stats_task: asyncio.Task | None = None

        self.num_received_s = 0
        self.num_inserted_s = 0

        self.history_num_received = deque([])
        self.history_num_inserted = deque([])

    async def get_last_processed_message(self) -> int | None:
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

    async def listen(self):
        ws_url = self.ws_url
        last_indexed = await self.get_last_processed_message()
        if last_indexed:
            # Substract two seconds
            last_indexed -= 2000

            ws_url += f"&time_us={last_indexed}"

        self.stats_task = asyncio.create_task(self.update_stats())

        async with connect(self.ws_url) as ws:
            batch = []
            async for msg in ws:
                data = orjson.loads(msg)

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

                batch.append({
                    'did': data['did'],
                    'rkey': commit['rkey'],
                    'cid': commit['cid'],
                    'post_text': record['text'],
                    'language': record.get('langs', [])
                })
                self.num_received_s += 1

                if len(batch) >= self.batch_size:
                    # If previous batch not inserted yet, wait until done
                    if self.postgres_update_task and not self.postgres_update_task.done():
                        await self.postgres_update_task

                    to_process = batch
                    batch = []
                    self.postgres_update_task = asyncio.create_task(
                        self.process_batch(to_process)
                    )

        # On disconnect, ensure we process remaining posts in the batch
        if self.postgres_update_task and not self.postgres_update_task.done():
            await self.postgres_update_task

        await self.process_batch(batch)

    async def process_batch(self, batch: list[dict]):
        async with async_session() as session:
            stmt = (insert(Post)
                    .on_conflict_do_nothing()
                    .returning(Post.c.id))

            result = await session.execute(
                stmt,
                batch
            )
            await session.commit()

            self.num_inserted_s += len(result.rowcount)

    async def update_stats(self):
        await asyncio.sleep(1)

        self.history_num_received.appendleft(self.num_received_s)
        self.history_num_inserted.appendleft(self.num_inserted_s)

        while len(self.history_num_received) >= 5:
            self.history_num_received.pop()

        while len(self.history_num_inserted) >= 5:
            self.history_num_inserted.pop()

        recv_per_s = exp_average(self.history_num_received)
        ins_per_s = exp_average(self.history_num_inserted)

        if sys.stdout.isatty():
            print("Receiving {:.0f} msg/s, inserting {:.0f} msg/s".format(recv_per_s, ins_per_s), end='\r')

        self.num_received_s = 0
        self.num_inserted_s = 0

        self.stats_task = asyncio.create_task(self.update_stats())


def exp_average(history, alpha=0.5):
    exp_average = sum(
        (alpha if i < len(history)-1 else 1) * ((1-alpha)**i) * value
        for i, value in enumerate(history)
    )

    return exp_average
