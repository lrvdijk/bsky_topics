from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine


async_session: async_sessionmaker[AsyncSession] = async_sessionmaker(expire_on_commit=False)


def configure_db(db_url: str, *args, **kwargs):
    engine = create_async_engine(db_url, *args, **kwargs)

    async_session.configure(bind=engine)

    return engine
