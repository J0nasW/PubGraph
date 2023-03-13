from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from umap import UMAP
import pandas as pd

# from sklearn.feature_extraction.text import CountVectorizer
# vectorizer_model = CountVectorizer(stop_words="english")

# from bertopic.representation import MaximalMarginalRelevance
# representation_model = MaximalMarginalRelevance(diversity=0.2)

# GPU Accelleration
# from cuml.cluster import HDBSCAN
# from cuml.manifold import UMAP
# Create instances of GPU-accelerated UMAP and HDBSCAN
# umap_model = UMAP(n_components=5, n_neighbors=15, min_dist=0.2, metric='cosine')
# hdbscan_model = HDBSCAN(min_samples=10, gen_min_span_tree=True, prediction_data=True)

import plotly.offline as pyo

from helpers.cli_loader import load_bar

FILTER_CAT = "cs"

# Prepare embeddings
with load_bar("Loading local file..."):
    # df = pd.read_json("arxiv-metadata-snapshot-02-23-cs-AI.json", lines=True) # 58k
    df = pd.read_json("arxiv-metadata-oai-snapshot.json", lines=True) # 2.2M
    print("Loaded " + str(len(df)) + " entries.")
    df = df[df["categories"].str.contains(FILTER_CAT)]
    print("Filtered by category " + FILTER_CAT + " to " + str(len(df)) + " entries.")
    docs = df["abstract"].tolist()
    titles = df["title"].tolist()

with load_bar("Generating Topics..."):
    sentence_model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = sentence_model.encode(docs, show_progress_bar=False)

    # Train BERTopic
    topic_model = BERTopic(nr_topics="auto").fit(docs, embeddings)

    # Further reduce topics
    # topic_model.reduce_topics(docs, nr_topics=30) # When enabled, disable nr_topics="auto" in topic_model itself!

    # Reduce dimensionality of embeddings, this step is optional but much faster to perform iteratively:
    reduced_embeddings = UMAP(n_neighbors=20, n_components=2, min_dist=0.3, metric='cosine').fit_transform(embeddings)

    topic_model.update_topics(docs, n_gram_range=(1, 3))

    topic_labels = topic_model.generate_topic_labels(nr_words=3,
                                                 topic_prefix=False,
                                                 word_length=10,
                                                 separator=", ")


    topic_model.set_topic_labels(topic_labels)

with load_bar("Generating Visualization..."):

    #topic_model.visualize_documents(docs, reduced_embeddings=reduced_embeddings).show()
    fig = topic_model.visualize_documents(titles, reduced_embeddings=reduced_embeddings, width=1800, height=1200, hide_annotations=True, custom_labels=True)
    # fig.show()
    # topic_model.visualize_heatmap(titles).show()
    # topic_model.visualize_barchart(titles).show()

    # Save plot as HTML file
    pyo.plot(fig, filename='topic_modelling_arXiv.html')
