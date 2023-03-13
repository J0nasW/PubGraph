import yake
from keybert import KeyBERT
import os
import multiprocessing
from multiprocessing import Pool
# multiprocessing.set_start_method('spawn')

from tqdm import tqdm
from helpers.cli_loader import load_bar
import pandas as pd

import nltk
from nltk.corpus import stopwords

# Similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE

from fuzzywuzzy.process import dedupe, fuzz

from keyphrase_vectorizers import KeyphraseCountVectorizer

# Visualization
import plotly.express as px
import plotly.offline as pyo

# USER INPUT
NR_KEYWORDS = 10 # Number of keywords to be extracted per text
STOPWORDS = "english"
#POS_PATTERN = "<N.*>"
POS_PATTERN = None

key_model = KeyBERT()
if POS_PATTERN:
    vectorizer = KeyphraseCountVectorizer(stop_words=STOPWORDS, pos_pattern=POS_PATTERN)
else:
    vectorizer = KeyphraseCountVectorizer(stop_words=STOPWORDS)

def keyword_func(text):
    return key_model.extract_keywords(text, vectorizer=vectorizer, top_n=NR_KEYWORDS)


def generate_keywords_vec(df, text_choice, output_dir=None):

    texts = df[text_choice].tolist()
    texts_with_keywords = []

    # key_model = KeyBERT()
    # if POS_PATTERN:
    #     vectorizer = KeyphraseCountVectorizer(stop_words=STOPWORDS, pos_pattern=POS_PATTERN)
    # else:
    #     vectorizer = KeyphraseCountVectorizer(stop_words=STOPWORDS)
    
    print("Processing text")
    with load_bar("Generating Keywords with Vectorizer.."):
        with Pool(os.cpu_count()-2):
            texts_with_keywords = list(tqdm(pool.imap(keyword_func, texts), total=len(texts)))
        
        #texts_with_keywords = key_model.extract_keywords(texts, vectorizer=vectorizer, top_n=NR_KEYWORDS)

    print("Done processing files. Got " + str(len(texts_with_keywords)) + " results.")
    
    # Save the keywords in the dataframe
    # keyword_variable = text_choice + "_keywords"
    # df[keyword_variable] = texts_with_keywords

    # Save the dataframe as a json file
    # df.to_json("keywords.json", orient="records", lines=True)

    # Save the keywords in a new dataframe with the id and the keywords
    column_name = text_choice + "_keywords"
    df_keywords = pd.DataFrame(columns=["id", column_name])
    df_keywords["id"] = df["id"]
    df_keywords[column_name] = texts_with_keywords

    # Save the dataframe as a json file
    file_name = column_name + ".json"
    df_keywords.to_json(file_name, orient="records", lines=True)

    return df_keywords

def generate_keywords_dict(keywords_df, text_choice, nr_documents):

    counter = 0

    with load_bar("Generating Keywords Dict and visual representation..."):

        # Create a dataframe only from df_keywords["keywords"]
        column_name = text_choice + "_keywords"
        df_keywords_only = pd.DataFrame(columns=["keywords"])
        df_keywords_only["keywords"] = keywords_df[column_name]

        # Flatten the list of keywords
        df_keywords_only["keywords"] = df_keywords_only["keywords"].apply(lambda x: [item[0] for item in x])

        # Extract the keywords from the column "keywords" and create new rows for each keyword
        df_keywords_only = df_keywords_only.explode("keywords")

        counter = len(df_keywords_only)
        print("Number of keywords: " + str(counter))

        # Drop exact duplicates
        df_keywords_only = df_keywords_only.drop_duplicates()

        # Reset the index
        df_keywords_only = df_keywords_only.reset_index(drop=True)

        #print(df_keywords_only)
        print("Number of keywords after dropping exact duplicates: " + str(len(df_keywords_only)) + " (-" + str(counter - len(df_keywords_only)) + ")")
        counter = len(df_keywords_only)

        # Cluster similar keywords together using the Levenshtein distance
        # Convert strings to vectors using TF-IDF
        vectorizer = CountVectorizer()
        vectors = vectorizer.fit_transform(df_keywords_only["keywords"])

        # Perform k-means clustering https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
        kmeans = KMeans(n_clusters=nr_documents, init='k-means++', max_iter=500, n_init="auto")
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
    pyo.plot(fig, filename='cluster_plot_keywords.html')

    fig.show()


    # # Compute non-negative distance matrix using Euclidean distance
    # distances = pairwise_distances(vectors, metric='euclidean')

    # # Perform clustering using DBSCAN
    # dbscan = DBSCAN(eps=0.5, min_samples=2, metric='precomputed')
    # clusters = dbscan.fit_predict(distances)

    # # Add cluster labels to DataFrame
    # df_keywords_only['cluster'] = clusters

    # # Print clusters
    # for cluster in df_keywords_only['cluster'].unique():
    #     print(f"Cluster {cluster}:")
    #     for string in df_keywords_only[df_keywords_only['cluster'] == cluster]['keywords']:
    #         print(string)


    # print(df_keywords_only)

    # Save the keywords in a JSON File
    df_keywords_only.to_json("keywords_with_clusters.json", orient="records", lines=True)
