# BlueSky topic modeling

## Introduction

Social media platforms enable people to connect with others who share common interests
or experiences worldwide. However, large platforms such as Facebook, Instagram, and
Twitter provide the user with an opaque, engagement-based, algorithmic feed, frequently
resulting in toxic discussion, viral spread of misinformation, and suppression of links to
websites outside their platforms. BlueSky is a new decentralized social media platform
where users can own and host their own data, choose their own (third-party) feed
algorithms, and subscribe to community-maintained moderation lists. It is built on open
protocol with an open API for developers, who can develop their custom feed generators,
moderation tools, and more.

In this project, I developed a foundation for a topic-based feed generator, recommending
posts to users interested in pre-specified topics. I developed a
tool to ingest posts from BlueSky, compute post embeddings, infer global topics discussed
on the platform, assign topics to posts, and query posts related to a topic. This project
paves the way for building personalized, topic-based, custom feeds on BlueSky.

*NB:* This was a class project for the Harvard Extension School Deep Learning class and
hacked together in the span of three weeks. It is not a full-fledged feed generator
and this needs a lot of work to be able to run at scale.

## Software Requirements

* Python
* PostgreSQL with pgvector extension
    * On macOS with homebrew: `brew install postgresql@17 pgvector`
* uv (a new modern Python package manager)
    * On macOS: `brew install uv`


## Installation

1. Setup PostgreSQL
    a. Run psql in a terminal, run the following queries to create a separate user
       and database
    b. `CREATE ROLE bsky_topics WITH LOGIN SUPERUSER PASSWORD ‘yourpassword’;`
    c. `CREATE DATABASE bsky_topics OWNER bsky_topics;`
2. Download the source code
3. Copy `env.toml.example` to `env.toml`
4. Modify env.toml and enter your database name, username, and password.
5. In the folder with the Python source code, run `uv sync` to install all dependencies.
6. Run `uv run bsky-topics db init` to initialize the database with our tables.


## Running the tool

* Collect BlueSky posts: `uv run bsky-topics collect`
* Compute embeddings for posts: `uv run bsky-topics embed`
* Run jupyter notebooks: `uv run jupyter lab`, open notebooks in the notebooks
  folder.


## Architecture

The tool comprises several components: 1) a service that listens to the BlueSky Jetstream,
a live event stream of the BlueSky platform, ingesting every new post submitted; 2) a
service that computes embeddings for batches of new posts; and 3) demo notebooks
showing how to use the computed embeddings to infer topics discussed on BlueSky. We
will detail each component below.

### Listening for and storing new posts

BlueSky is built on the open AT protocol, which defines listening for new events,
authentication, and sending your own events (e.g., submitting a new like or post). In our use
case, we do not need the full (authenticated) feature set available on the AT protocol. Our
tool simply needs to process and index new posts. For such simpler user cases, BlueSky
offers the “JetStream,” a simplified JSON version of events happening on the platform.

Our tool includes a service that connects to the JetStream (using websockets), parses
incoming JSON events, filters events other than new posts, and stores new posts into our
PostgreSQL database. The service can be run using `uv run bsky-topics collect`. Relevant
source code: `src/bsky_topics/jetstream.py`.

### Computing embeddings for posts

We use several pre-trained BERT-like neural networks to compute post embeddings and
topics. First, we infer individual sentences of a post, even those without proper punctuation
using the [“Segment Any Text” (SaT) model](https://github.com/segment-any-text/wtpsplit). Next,
we use a “sentence transformer” to compute semantic embeddings for each sentence. We
compute the final post embedding by taking the average of all sentence embeddings.

We chose to use the SaT model for two reasons: 1) to ensure that (longer) posts could be
split into multiple smaller components, which easily fit into the sentence transformer
model (more details below), and 2) punctuation on social media is likely to be irregular or
absent.

We compute an embedding for each sentence using a sentence transformer, specifically
the [“All-MiniLM-L6-V2" model](https://sbert.net/docs/sentence_transformer/pretrained_models.html).
Sentence transformers enable efficient computation of embeddings for whole sentences that
perform well for semantic search. The MiniLM-L6-V2 model is a multilingual model with great
performance among a number of benchmarks,
while also having one of the fastest inference speeds. Fast inference is important when
hundreds of posts are submitted each second.

The final post embedding is computed by taking the average of all sentence embeddings,
and stored in a PostgreSQL database. Vectors can be stored in the database using the
‘pgvector’ extension. This extension additionally provides index data structures to
efficiently query posts similar to another embedding.

### Inferring global topics

Post embeddings can be used to find other posts that are semantically similar. By
clustering similar posts, we can identify high-level topics being discussed on BlueSky.

We used the Python library [“BERTopic”](https://maartengr.github.io/BERTopic/index.html)
to infer these topics. BERTopic provides an off-the-shelf pipeline that performs
dimensionality reduction, post clustering, and topic inference.
It first computes lower-dimensional representation of posts using UMAP, clusters posts
using HDBSCAN, and identifies words describing the cluster (i.e., the topic) using the
“cluster-term frequency, inverse document frequency” weighting scheme.

## Discussion

The option to develop custom feeds is an exciting feature of the BlueSky platform. In this
project, I experimented with a neural-network-based algorithm that could serve posts
relevant to a specific topic. I was able to compute post embeddings and infer global topics
being discussed.

This prototype, however, is still far away from a real-world application. Hundreds of posts
are being submitted to BlueSky each second, and a powerful machine would be required to
compute embeddings for all these posts in realtime. Furthermore, while the PostgreSQL
vector pgvector provides an index structure that enables efficient querying of approximate
nearest neighbors, constructing this index takes up a lot of resources and did not finish in
24 hours for a dataset of 21M rows.

Another avenue of future improvement could be to first infer more informative topic
representations based on a more curated dataset such as Wikipedia. Then we can use post
embeddings to find relevant topics. Topic modeling on all posts is inefficient and contains a
lot of noise.
