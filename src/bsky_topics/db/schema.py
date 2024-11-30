from datetime import datetime
from typing import Optional

from sqlalchemy import String
from sqlalchemy import func
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.dialects.postgresql import ARRAY


class Base(AsyncAttrs, DeclarativeBase):
    pass


class Post(Base):
    __tablename__ = 'posts'

    id: Mapped[int] = mapped_column(primary_key=True)
    did: Mapped[str] = mapped_column(String(255))
    rkey: Mapped[str] = mapped_column(String(100))
    cid: Mapped[str] = mapped_column(String(255))
    indexed_at: Mapped[datetime] = mapped_column(insert_default=func.now())
    post_text: Mapped[str]
    language: Mapped[Optional[list[str]]] = mapped_column(ARRAY(String(8)))

    __table_args__ = (
        UniqueConstraint('did', 'rkey', name='did_record_key'),
    )
