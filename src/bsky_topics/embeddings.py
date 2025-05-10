"""
Tools to calculate BlueSky post embeddings
"""

from __future__ import annotations
import logging

import torch
import numpy
from wtpsplit import SaT
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)


class PostEmbedder:
    """
    Manages the logic to compute embeddings for a batch of posts.

    To compute embeddings, we first split the post into seperate sentences using the
    Segment any Text model. Next, we compute embeddings for each sentence. The average of each
    sentence embedding will be returned as the post embedding.
    """

    def __init__(self, device=None):
        self.sat = SaT("sat-3l-sm")

        if device:
            self.sat.half().to(device)

        self.transformer = SentenceTransformer("all-MiniLM-L6-v2", device=device)

    def embed(self, posts: list[str]) -> torch.tensor:
        per_post_sentences = list(self.split_into_sentences(posts))

        # Combine all sentences into a single batch
        flattened_sentences = [s for p in per_post_sentences for s in p]

        # Ensure we can map batch sentences indices back to posts
        curr_ix = 0
        post_sentence_slices = []
        for sentences in per_post_sentences:
            post_sentence_slices.append(
                slice(curr_ix, curr_ix + len(sentences))
            )

            curr_ix += len(sentences)

        sentence_embeddings = self.transformer.encode(flattened_sentences)

        # For each post, average embeddings of sentences to compute the final
        # post embedding
        post_embeddings = numpy.zeros((len(posts), sentence_embeddings.shape[1]))
        for i, post_slice in enumerate(post_sentence_slices):
            post_embeddings[i] = sentence_embeddings[post_slice].mean(axis=0)

        return post_embeddings

    def split_into_sentences(self, posts: list[str]) -> list[list[str]]:
        """Runs 'Segment any Text' model to split a post into multiple sentences."""
        return self.sat.split(posts)

