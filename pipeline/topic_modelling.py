#########################################################################################################
#
# This file contains the TopicModelling class, which is used to generate topics from a given text.
# 
#########################################################################################################

import pandas as pd

# Topic Modelling
from sentence_transformers import SentenceTransformer
from bertopic import BERTopic
from bertopic.representation import MaximalMarginalRelevance

# Traditional CPU
from umap import UMAP
from hdbscan import HDBSCAN

import openai
from bertopic.representation import OpenAI

from sklearn.feature_extraction.text import CountVectorizer
from bertopic.vectorizers import ClassTfidfTransformer
from bertopic.representation import KeyBERTInspired

# from umap import UMAP

# GPU Accelleration - requires cuml and it is hard to set it up...
# from cuml.cluster import HDBSCAN
# from cuml.manifold import UMAP
# Create instances of GPU-accelerated UMAP and HDBSCAN
# umap_model = UMAP(n_components=5, n_neighbors=15, min_dist=0.2, metric='cosine')
# hdbscan_model = HDBSCAN(min_samples=10, gen_min_span_tree=True, prediction_data=True)

# Visualization
import plotly.offline as pyo

from helpers.cli_loader import load_bar

class TopicModelling():

    def __init__(self, docs_df, text_choice):
        self.save_topic_model = False
        # self.sentence_model = SentenceTransformer("pritamdeka/S-Scibert-snli-multinli-stsb")
        # self.sentence_model = SentenceTransformer("all-mpnet-base-v2")
        self.sentence_model = SentenceTransformer("all-distilroberta-v1")
        # self.representation_model = PartOfSpeech("en_core_web_sm")
        self.vectorizer_model = CountVectorizer()
        self.umap_model = UMAP(n_neighbors=5, n_components=5, min_dist=0.4, metric='cosine')
        self.hdbscan_model = HDBSCAN(min_cluster_size = 10, gen_min_span_tree=True, prediction_data=True)
        self.ctfidf_model = ClassTfidfTransformer(reduce_frequent_words=True)
        self.representation_model = MaximalMarginalRelevance(diversity=0.3)
        self.ngram_range = (1, 3)
        self.min_topic_size = 15
        self.top_n_words = 10
        self.docs_df = docs_df
        self.text_choice = text_choice
        self.num_topics = 50
        
    def tm_generator(self):
        
        docs = self.docs_df[self.text_choice].tolist()
        titles = self.docs_df["title"].tolist()
        ids = self.docs_df["id"].tolist()
        
        # Strip all docs from newline characters
        docs = [doc.replace("\n", " ") for doc in docs]
        docs = [doc.replace("  ", " ") for doc in docs]
        
        # Strip all titles from newline characters
        titles = [title.replace("\n", " ") for title in titles]
        titles = [title.replace("  ", " ") for title in titles]
        
        # print(docs)
        # print(titles)
        
        with load_bar("Generating Topics..."):
            embeddings = self.sentence_model.encode(docs, show_progress_bar=False)
            
            # Reduce dimensionality of embeddings, this step is optional but much faster to perform iteratively:
            # reduced_embeddings = self.umap_model.fit_transform(embeddings)
            
            # keyword_list = [t[0] for t in (self.df_keywords.iloc[:, 1].tolist())]
            # print(keyword_list)
            
            # Create flat list from df_keywords dataframe
            # vocabulary = [t[0] for t in [item for sublist in self.df_keywords.iloc[:, 1].tolist() for item in sublist]]
            # print("Got " + str(len(vocabulary)) + " keywords.")
            
            # Drop duplicates
            # vocabulary = list(set(vocabulary))
            # print("Got " + str(len(vocabulary)) + " keywords after dropping duplicates.")
            
            # vectorizer_model= CountVectorizer(vocabulary=vocabulary)

            # Train BERTopic
            topic_model = BERTopic(nr_topics = self.num_topics,
                                   top_n_words = self.top_n_words,
                                   min_topic_size = self.min_topic_size,
                                   embedding_model = self.sentence_model,
                                   umap_model = self.umap_model,
                                   vectorizer_model = self.vectorizer_model,
                                   hdbscan_model = self.hdbscan_model,
                                   ctfidf_model = self.ctfidf_model,
                                   representation_model=self.representation_model,
                                   calculate_probabilities=True)
            topics, probs = topic_model.fit_transform(docs, embeddings)
            
            # new_topics = topic_model.reduce_outliers(docs, topics, strategy="embeddings", threshold=0.5, embeddings=embeddings)
            # new_topics = topic_model.reduce_outliers(docs, topics, probabilities=probs, strategy="probabilities")


            # Further reduce topics
            # topic_model.reduce_topics(docs, nr_topics=30) # When enabled, disable nr_topics="auto" in topic_model itself!
            
            # Fine-tune topic representations after training BERTopic
            vectorizer_model = CountVectorizer(stop_words="english", ngram_range=(1, 3))
            topic_model.update_topics(docs, topics=topics, vectorizer_model=vectorizer_model)

            topic_labels = topic_model.generate_topic_labels(nr_words=3,
                                                        topic_prefix=False,
                                                        word_length=15,
                                                        separator=", ")
            topic_model.set_topic_labels(topic_labels)

        with load_bar("Generating Visualization..."):

            #topic_model.visualize_documents(docs, reduced_embeddings=reduced_embeddings).show()
            # fig = topic_model.visualize_documents(titles, reduced_embeddings=reduced_embeddings, width=1800, height=1200, hide_annotations=True, custom_labels=True)
            fig = topic_model.visualize_documents(titles, embeddings=embeddings, width=1800, height=1000, hide_annotations=True, custom_labels=True)
            # fig.show()
            # topic_model.visualize_heatmap(titles).show()
            # topic_model.visualize_barchart(titles).show()

            # Save plot as HTML file
            plot_name = "output/visualizations/" + self.text_choice + "_topic_model_visualization.html"
            pyo.plot(fig, filename=plot_name)
            
        with load_bar("Saving topic model..."):
            if self.save_topic_model:
                # Save topic model
                tm_name = "output/" + self.text_choice + "_topic_model"
                topic_model.save(tm_name)
                
            try:
                # Generate a set of topics and probabilities
                topics_set = list(set(topics))
                topic_lables = [topic_model.get_topic(topic) for topic in topics_set]
                
                # Create a list of ids that correspond to the topics
                associated_ids = []
                associated_titles = []
                for topic in topics_set:
                    associated_ids.append([ids[i] for i, x in enumerate(topics) if x == topic])
                    associated_titles.append([titles[i] for i, x in enumerate(topics) if x == topic])

                # Create a dataframe with the topics and their associated ids
                tm_df = pd.DataFrame({"topic": topics_set, "topic_label": topic_lables, "id": associated_ids, "title": associated_titles})
                
                # print(tm_df)
                print("Done processing files. Got " + str(len(tm_df)) + " topics.")

            except Exception as e:
                print(e)
                print("Error in writing the topics and probabilities to the DataFrame. Skipping...")
            
            # Save tm_df as JSON file
            tm_df_name = "output/" + self.text_choice + "_topic_model.json"
            tm_df.to_json(tm_df_name, orient="records", lines=True)
            
        return tm_df_name