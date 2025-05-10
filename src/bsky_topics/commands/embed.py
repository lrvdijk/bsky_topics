import asyncio
import logging

import click
from sqlalchemy import select, insert, update

from bsky_topics.commands import cli_main
from bsky_topics.db import async_session
from bsky_topics.db.schema import Post, PostEmbedding
from bsky_topics.embeddings import PostEmbedder

logger = logging.getLogger(__name__)


@cli_main.command()
@click.option('-b', '--batch-size', type=int, default=256, help="Number of posts to process in batch.")
@click.option('-d', '--device', default="mps", help="(GPU) device use for computing embeddings.")
@click.pass_context
def embed(ctx, batch_size: int, device: str = 'mps'):
    embed_service = PostEmbedService(batch_size, device)
    asyncio.run(embed_service.compute_embeddings())


class PostEmbedService:
    """
    Continuously check for posts without an embedding, and compute if needed.
    """

    def __init__(self, batch_size: int = 256, device: str | None = None):
        self.embedder = PostEmbedder(device)
        self.batch_size = batch_size
        self.backoff_counter = 0

    async def compute_embeddings(self):
        async with async_session() as session:
            while True:
                # Select a batch of posts without an existing embedding
                subquery = (select(PostEmbedding.id)
                            .filter(PostEmbedding.post_id == Post.id))

                stmt = (select(Post.id, Post.post_text)
                        .filter(
                            ~subquery.exists(),
                            ~Post.exclude_for_embedding,
                        )
                        .order_by(Post.id)
                        .limit(self.batch_size))

                batch = await session.execute(stmt)
                batch = list(batch)

                if not batch:
                    logger.info("Nothing to process, sleeping...")
                    await asyncio.sleep(2**self.backoff_counter)
                    self.backoff_counter += 1

                    if self.backoff_counter > 10:
                        # Quit if no new posts found after 10 retries
                        return
                    else:
                        continue

                # Found a batch of posts to process
                self.backoff_counter = 0

                post_ids = [p[0] for p in batch]
                post_texts = [p[1] for p in batch]

                try:
                    embeddings = self.embedder.embed(post_texts)
                except AssertionError:
                    await self.exclude_errornous_posts(post_ids, post_texts)
                    continue

                new_embeddings = [
                    {'post_id': post_ids[i], 'embedding': embeddings[i]}
                    for i in range(len(embeddings))
                ]

                await session.execute(insert(PostEmbedding), new_embeddings)
                await session.commit()

    async def exclude_errornous_posts(self, post_ids: list[int], post_texts: list[str]):
        async with async_session() as session:
            exclude = []

            # Try one by one to test which post in a batch caused an error
            for i, post_text in enumerate(post_texts):
                try:
                    self.embedder.embed([post_text])
                except AssertionError:
                    logger.error("Could not compute embedding for post ID: %d, text: %s", post_ids[i], post_text)
                    logger.error("Skipping from processing in the future.")

                    exclude.append(post_ids[i])


            stmt = update(Post).where(Post.id.in_(exclude)).values(exclude_for_embedding=True)

            await session.execute(stmt)
            await session.commit()

