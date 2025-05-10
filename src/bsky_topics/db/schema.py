from datetime import datetime
from typing import Optional

from sqlalchemy import String, Boolean, ForeignKey, Index, func
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.dialects.postgresql import ARRAY

from pgvector.sqlalchemy import Vector


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
    exclude_for_embedding: Mapped[Optional[bool]] = mapped_column(Boolean(), default=False)

    __table_args__ = (
        UniqueConstraint('did', 'rkey', name='did_record_key'),
    )


class PostEmbedding(Base):
    __tablename__ = 'post_embeddings'

    id: Mapped[int] = mapped_column(primary_key=True)
    post_id: Mapped[int] = mapped_column(ForeignKey('posts.id'))
    embedding: Mapped[list] = mapped_column(Vector(384))

    post: Mapped[Post] = relationship()


embedding_index = Index(
    'post_embedding_idx',
    PostEmbedding.embedding,
    postgresql_using='hnsw',
    postgresql_with={'m': 16, 'ef_construction': 64},
    postgresql_ops={'embedding': 'vector_cosine_ops'}
)
