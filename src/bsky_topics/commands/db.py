import asyncio

import click
from alembic.config import Config as AlembicConfig
from alembic import command

from bsky_topics.commands import cli_main
from bsky_topics.db.schema import Base


@cli_main.group()
def db():
    pass


@db.command()
@click.option('-c', '--alembic-ini', default="alembic.ini", metavar='PATH',
              help="Path to alembic configuration. Defaults to alembic.ini.")
@click.pass_context
def init(ctx, alembic_ini="alembic.ini"):
    engine = ctx.obj['db_engine']

    asyncio.run(init_db(engine))

    cfg = AlembicConfig(alembic_ini)
    command.stamp(cfg, "head")


async def init_db(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
