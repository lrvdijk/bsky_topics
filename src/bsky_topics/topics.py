"""
`bsky_topics.topics` - Compute topics from post embeddings
"""

from __future__ import annotations
from datetime import datetime

import numpy
from sqlalchemy import select

from torch.utils.data import Dataset, DataLoader
from sklearn.cluster import MiniBatchKMeans

from bertopic import BERTopic
from bertopic.vectorizers import ClassTfidfTransformer
from bertopic.representation import MaximalMarginalRelevance

from bsky_topics.db import async_session
from bsky_topics.db.schema import Post, PostEmbedding


class PostsDataset(Dataset):
    """
    Loads post embeddings in memory to be used as a PyTorch dataset

    TODO: load batches from database on demand for less memory usage.
    """

    def __init__(self, post_ids: numpy.ndarray, post_embeddings: numpy.ndarray):
        self.return_embedding = True

        assert len(post_ids) == len(post_embeddings)

        self.post_ids = post_ids
        self.post_embeddings = post_embeddings

    def return_post_texts(self):
        self.return_embedding = False

    def return_embeddings(self):
        self.return_embedding = True

    def __len__(self) -> int:
        return len(self.post_embeddings)

    def __getitem__(self, item: int) -> str | numpy.ndarray:
        if self.return_embedding:
            return self.post_embeddings[item]
        else:
            return self.post_ids[item]

    @classmethod
    async def load_for_date_range(cls, date_start: datetime, date_end: datetime) -> PostsDataset:
        async with async_session() as session:
            stmt = (select(Post.id, PostEmbedding.embedding)
                    .join(PostEmbedding)
                    .filter(Post.indexed_at >= date_start, Post.indexed_at < date_end))

            post_ids = []
            post_embeddings = []
            for partition in await session.execute(stmt).yield_per(1024).partitions():
                for post_id, post_embedding in partition:
                    post_ids.append(post_ids)
                    post_embeddings.append(post_embedding)

            post_ids = numpy.array(post_ids)
            post_embeddings = numpy.vstack(post_embeddings)

            return cls(post_ids, post_embeddings)


class PostClusters:
    """
    Use scikit-learn's MiniBatchKMeans to compute cluster centroids of posts.

    We use MiniBatchKMeans for a memory-efficient approach to compute post clusters. We were
    unable to run HDBSCAN on embeddings of more than 1M posts.

    TODO: Implement minibatch k-means with PyTorch for GPU acceleration.
    """

    def __init__(self, dataset: PostsDataset, n_clusters=10_000, batch_size=1024):
        self.batch_size = batch_size
        self.mkb = MiniBatchKMeans(
            init="k-means++",
            n_clusters=n_clusters,
            batch_size=batch_size
        )

    def fit(self, data: PostsDataset):
        data_loader = DataLoader(data, batch_size=self.batch_size, shuffle=True, num_workers=1)

        for batch in data_loader:
            self.mkb.partial_fit(batch)

    def predict(self, X: numpy.ndarray) -> numpy.ndarray:
        return self.mkb.predict(X)

    @property
    def labels_(self) -> numpy.ndarray:
        return self.mkb.labels_


class ClusterTFIDF:
    """
    Computes per-cluster term frequency and inverse document frequencies.
    """

    def __init__(self, posts: PostsDataset, clusters: PostClusters):
        self.posts = posts
        self.clusters = clusters

    async def compute_ctfidf_matrix(self):
        all_clusters = self.clusters.labels_.unique()

        ix = numpy.arange(len(self.posts))
        for cluster_id in all_clusters:
            cluster_post_ids = self.posts.post_ids[self.clusters.labels_ == cluster_id]


def compute_topics(posts: list[str], embeddings: list[numpy.array]):
    # Ensure to reduce frequent words, such as common stop words
    ctfidf_model = ClassTfidfTransformer(reduce_frequent_words=True)

    # Create your representation model
    representation_model = MaximalMarginalRelevance(diversity=0.5)

    topic_model = BERTopic(
        ctfidf_model=ctfidf_model,
        representation_model=representation_model
    )

    topic_model.fit_transform(posts, embeddings)

    return topic_model


def get_indexed_posts_for_date_range(start: datetime | None = None, end: datetime | None = None):
    if not start and not end:
        raise ValueError("Need at least one of `start` or `end`!")

    stmt = select(Post)

    if start:
        stmt = stmt.filter(Post.indexed_at > start)

    if end:
        stmt = stmt.filter(Post.indexed_at < end)

    return stmt
