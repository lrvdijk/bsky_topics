import tomllib
from pathlib import Path

from sqlalchemy import URL

DEFAULT_WS_URL = "jetstream1.us-east.bsky.network"


class Config:
    """
    Load bsky-topics configuration from a TOML file
    """

    def __init__(self, loaded_config=None):
        if not loaded_config:
            loaded_config = {}

        self.ws_hostname = loaded_config.get('ws_hostname', DEFAULT_WS_URL)
        self.db_url = URL.create(
            "postgresql+asyncpg",
            username=loaded_config.get('postgres', {}).get('username'),
            password=loaded_config.get('postgres', {}).get('password'),
            host=loaded_config.get('postgres', {}).get('hostname', 'localhost'),
            port=loaded_config.get('postgres', {}).get('port', None),
            database=loaded_config.get('postgres', {}).get('database'),
        )

    def load(fname: str | Path):
        if Path(fname).is_file():
            with open(fname, 'rb') as ifile:
                config = tomllib.load(ifile)
        else:
            config = {}

        return Config(config)
