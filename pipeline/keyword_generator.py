#########################################################################################################
#
# This file contains the KeywordGenerator class, which is used to generate keywords from a given text.
# 
#########################################################################################################

import pandas as pd
import os
from tqdm.contrib.concurrent import process_map
import multiprocessing
from multiprocessing import Pool
multiprocessing.set_start_method('spawn')

# Keyword Generation
from keybert import KeyBERT
from keyphrase_vectorizers import KeyphraseCountVectorizer

# Similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE

# Visualization
import plotly.express as px
import plotly.offline as pyo

from helpers.cli_loader import load_bar

class Keyword_Generator():

    def __init__(self, docs_df, text_choice):
        self.key_model = KeyBERT()
        self.stopwords = "english"
        self.pos_pattern = "<N.*>"
        self.counter = 0
        self.use_multiprocessing = False
        self.docs_df = docs_df
        self.text_choice = text_choice
        
        if self.pos_pattern:
            self.vectorizer = KeyphraseCountVectorizer(pos_pattern=self.pos_pattern)
        else:
            self.vectorizer = KeyphraseCountVectorizer(stop_words=self.stopwords)
            
    def keyword_gen(self, texts):
        return self.key_model.extract_keywords(texts, vectorizer=self.vectorizer)

    def generate_keywords(self):
        
        texts = self.docs_df[self.text_choice].tolist()
        texts_with_keywords = []
        
        print("Processing text")
        with load_bar("Generating Keywords with Vectorizer..."):
            # Use Pool Multiprocessing with tqdm to show progress
            if self.use_multiprocessing:
                usable_cpu_cores = os.cpu_count() - 2
                texts_with_keywords = process_map(self.keyword_gen, texts, max_workers=usable_cpu_cores)
            else:
                texts_with_keywords = self.keyword_gen(texts)
            
        print("Done processing files. Got " + str(len(texts_with_keywords)) + " results.")
    
        # Save the keywords in a new dataframe with the id and the keywords
        column_name = self.text_choice + "_keywords"
        df_keywords = pd.DataFrame(columns=["id", column_name])
        # Save keywords and ids in the dataframe
        for i in range(len(texts_with_keywords)):
            df_keywords.loc[i] = [self.docs_df["id"][i], texts_with_keywords[i]]

        # Save the dataframe as a json file
        file_name = "output/" + column_name + ".json"
        df_keywords.to_json(file_name, orient="records", lines=True)
        
        try:        
            self.generate_keywords_dict(df_keywords, self.text_choice)    
        except Exception as e:
            print("Error generating keywords dict: " + str(e))
            
        return file_name
    
    def generate_keywords_dict(self, keywords_df, text_choice):

        with load_bar("Generating Keywords dict and visual representation..."):

            # Create a dataframe only from df_keywords["keywords"]
            column_name = text_choice + "_keywords"
            df_keywords_only = pd.DataFrame(columns=["keywords"])
            df_keywords_only["keywords"] = keywords_df[column_name]

            # Flatten the list of keywords
            df_keywords_only["keywords"] = df_keywords_only["keywords"].apply(lambda x: [item[0] for item in x])

            # Extract the keywords from the column "keywords" and create new rows for each keyword
            df_keywords_only = df_keywords_only.explode("keywords")

            self.counter = len(df_keywords_only)
            print("Number of keywords: " + str(self.counter))

            # Drop exact duplicates
            df_keywords_only = df_keywords_only.drop_duplicates()

            # Reset the index
            df_keywords_only = df_keywords_only.reset_index(drop=True)

            #print(df_keywords_only)
            print("Number of keywords after dropping exact duplicates: " + str(len(df_keywords_only)) + " (-" + str(self.counter - len(df_keywords_only)) + ")")
            self.counter = len(df_keywords_only)

            # Cluster similar keywords together using the Levenshtein distance
            # Convert strings to vectors using TF-IDF
            vectorizer = CountVectorizer()
            vectors = vectorizer.fit_transform(df_keywords_only["keywords"])

            # Perform k-means clustering https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
            # Calculate the perfect amount of n_clusters
            n_cluster_nr = int(len(df_keywords_only["keywords"].unique()) / 10)
            kmeans = KMeans(n_clusters=n_cluster_nr, init='k-means++', max_iter=500, n_init="auto")
            clusters = kmeans.fit_predict(vectors)

            # Add cluster labels to DataFrame
            df_keywords_only['cluster'] = clusters

            # Print clusters
            # for cluster in df_keywords_only['cluster'].unique():
            #     print(f"Cluster {cluster}:")
            #     for string in df_keywords_only[df_keywords_only['cluster'] == cluster]['keywords']:
            #         print(string)

            # Reduce dimensionality using t-SNE https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html#sklearn.manifold.TSNE
            tsne = TSNE(n_components=3, perplexity=40, n_iter=2000)
            reduced_vectors = tsne.fit_transform(vectors.toarray())

            # Add t-SNE coordinates to DataFrame
            df_keywords_only['tsne_x'] = reduced_vectors[:, 0]
            df_keywords_only['tsne_y'] = reduced_vectors[:, 1]
            df_keywords_only['tsne_z'] = reduced_vectors[:, 2]

            # Define colormap
            colors = {0: 'red', 1: 'blue', 2: 'green'}

            # Plot clusters
            # fig = px.scatter_3d(df_keywords_only, x='tsne_x', y='tsne_y', z="tsne_z", color='cluster', color_discrete_map=colors,
            #                 hover_name='keywords', hover_data={'tsne_x': False, 'tsne_y': False, "tsne_z": False})
            
            # Plot clusters in 3D with legend
            fig = px.scatter_3d(df_keywords_only, x='tsne_x', y='tsne_y', z='tsne_z', color='cluster', color_discrete_map=colors,
                                hover_name='keywords', hover_data={'tsne_x': False, 'tsne_y': False, 'tsne_z': False},
                                opacity=0.7, symbol='cluster', title='Cluster Plot of Keywords', labels={'cluster': 'Cluster'})
            
            fig.update_traces(marker=dict(line=dict(width=0.5,color='DarkSlateGrey')))
            fig.update_layout(legend=dict(yanchor="bottom", y=0.01, xanchor="left", x=0.01))
                        

        # Save plot as HTML file
        pyo.plot(fig, filename="output/visualizations/keywords_cluster_plot.html")

        # fig.show()