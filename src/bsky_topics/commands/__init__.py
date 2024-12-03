import logging

import click

import rich.console
import rich.logging

from bsky_topics.config import Config
from bsky_topics.db import configure_db


DEFAULT_CONFIG_FILE = "env.toml"

root_pkg = __name__[:__name__.find('.')]
logger = logging.getLogger(root_pkg)
logger.setLevel(logging.INFO)


@click.group()
@click.option('-c', '--config', default=DEFAULT_CONFIG_FILE, help="Load configuration from the specified file.")
def cli_main(config=None):
    """
    bsky-topics CLI entry point

    Loads config from the TOML file and connects to the PostgreSQL database.

    See also
    --------
    collect
    """

    # Load app config
    ctx = click.get_current_context()
    config_fname = ctx.params.get('config', DEFAULT_CONFIG_FILE)
    config = Config.load(config_fname)

    # Store config in context for subcommands
    ctx.ensure_object(dict)
    ctx.obj['config'] = config

    # Connect to PostgreSQL database
    engine = configure_db(config.db_url)
    ctx.obj['db_engine'] = engine

    # Set up console output
    ctx.obj['console'] = rich.console.Console()

    # Setup logging handler
    handler = rich.logging.RichHandler(console=ctx.obj['console'])
    logging.basicConfig(format=r"[%(name)s] %(message)s", handlers=[handler])
