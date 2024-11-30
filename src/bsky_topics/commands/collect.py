import asyncio

import click

from bsky_topics.commands import cli_main
from bsky_topics.jetstream import JetstreamCollector


@cli_main.command()
@click.pass_context
def collect(ctx):
    config = ctx.obj['config']

    collector = JetstreamCollector(config.ws_hostname)
    asyncio.run(collector.listen())
