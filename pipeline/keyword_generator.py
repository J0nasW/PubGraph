import yake
from keybert import KeyBERT
import os
from multiprocessing import Pool

from tqdm import tqdm

import pandas as pd

import nltk
from nltk.corpus import stopwords

# Similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.cluster import KMeans
from sklearn.manifold import TSNE

from fuzzywuzzy.process import dedupe, fuzz

# Visualization
import plotly.express as px
import plotly.offline as pyo

from helpers.read_keywords import *

# USER INPUT
NR_KEYWORDS = 10 # Number of keywords to be extracted per text
N_GRAM_RANGE = (1, 2) # N-gram range for the model
STOP_WORDS = "english" # Stop words for the model

DEDUPE_THRESHOLD = 75
MANUAL_CANDIDATES = read_n_grams("ai_candidates_n_grams.txt") + read_unigrams("ai_candidates_unigrams.txt") # Can be None; Can be a list of strings
YAKE_CANDIDATES = False

FLAIR_MODEL = "allenai/scibert_scivocab_cased" # Can be None; Can handle multiple Huggingface Models
HF_MODEL = None # Can be None; Can handle Huggingface Models with "Feature Extraction"
SENTENCE_TRANSFORMERS_MODEL = None # Can be None; Can handle Sentence Transformers Models from https://www.sbert.net/docs/pretrained_models.html

USE_YAKE = True
YAKE_NR_KEYWORDS = 100 # Number of keywords to be extracted per text

MODEL_MAX_TOKENS = 512 # Can be None

if HF_MODEL:
    from transformers.pipelines import pipeline
    hf_model = pipeline("feature-extraction", model=HF_MODEL)
    key_model = KeyBERT(model=hf_model)
elif FLAIR_MODEL:
    from flair.embeddings import TransformerDocumentEmbeddings, WordEmbeddings, DocumentPoolEmbeddings
    flair_model = TransformerDocumentEmbeddings(FLAIR_MODEL)
    # repair value for max_subtokens_sequence_length and stride from https://github.com/flairNLP/flair/issues/2305
    flair_model.max_subtokens_sequence_length = 512
    # if allow_long_sentences parameter is True then stride should be 512 // 2 else just set to 0 (maintain the original class behavior)
    flair_model.stride = 512 // 2 #or 0
    key_model = KeyBERT(model=flair_model)
elif SENTENCE_TRANSFORMERS_MODEL:
    from sentence_transformers import SentenceTransformer
    sentence_transformers_model = SentenceTransformer(SENTENCE_TRANSFORMERS_MODEL)
    key_model = KeyBERT(model=sentence_transformers_model)
else:
    print("No model selected. Please select a model.")
    exit()

if USE_YAKE:
    yake_kw_extractor = yake.KeywordExtractor(top=YAKE_NR_KEYWORDS)

def process_keywords(text):
    # print("Processing text")

    # Convert text to lowercase
    text = text.lower()

    # Limit the text to MODEL_MAX_TOKENS words if it is longer than MODEL_MAX_TOKENS
    if MODEL_MAX_TOKENS:
        if len(text.split()) > MODEL_MAX_TOKENS:
            text = " ".join(text.split()[:MODEL_MAX_TOKENS])

    if MANUAL_CANDIDATES:
        candidates = MANUAL_CANDIDATES
        print(candidates)
        key_model.extract_keywords(text, candidates=candidates, keyphrase_ngram_range=N_GRAM_RANGE, top_n=NR_KEYWORDS, stop_words=STOP_WORDS)
    elif YAKE_CANDIDATES:
        # YAKE Keywords:
        candidates = yake_kw_extractor.extract_keywords(text)
        candidates = [candidate[0] for candidate in candidates]    
        key_model.extract_keywords(text, candidates=candidates, keyphrase_ngram_range=N_GRAM_RANGE, top_n=NR_KEYWORDS, stop_words=STOP_WORDS)
    else:
        key_model.extract_keywords(text, keyphrase_ngram_range=N_GRAM_RANGE, top_n=NR_KEYWORDS, stop_words=STOP_WORDS)

    return 


def generate_keywords(df, text_choice, output_dir=None):

    texts = df[text_choice].tolist()
    texts_with_keywords = []

    CPU_COUNT = os.cpu_count() - 2
    # CPU_COUNT = 4
    print("Using " + str(CPU_COUNT) + " cores (" + str(os.cpu_count()) + " are available).")
    with Pool(CPU_COUNT) as pool:
        # texts_with_keywords = pool.map(process_keywords, texts)
        # print("Mapping...")
        texts_with_keywords = list(tqdm(pool.imap(process_keywords, texts), total=len(texts)))
        # print("Running...")
        # pool.close()
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

    # Create a dataframe only from df_keywords["keywords"]
    column_name = text_choice + "_keywords"
    df_keywords_only = pd.DataFrame(columns=["keywords"])
    df_keywords_only["keywords"] = keywords_df[column_name]

    # Flatten the list of keywords if it is not empty
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

    # keywords_only_list = df_keywords_only["keywords"].tolist()

    # # Drop duplicates that are similar
    # deduped_keywords = list(dedupe(keywords_only_list, threshold=DEDUPE_THRESHOLD, scorer=fuzz.ratio))

    # # Create a new dataframe with the deduped keywords
    # df_keywords_only = pd.DataFrame(deduped_keywords, columns=["keywords"])

    # print("Number of keywords after deduping with threshold of " + str(DEDUPE_THRESHOLD) + ": " + str(len(df_keywords_only)) + " (-" + str(counter - len(df_keywords_only)) + ")")
    # counter = len(df_keywords_only)

    # Clean the keywords by using several nltk corpora
    # nltk.download('stopwords', quiet=True)
    # nltk.download('wordnet')
    # nltk.download('words')
    # nltk.download('names')
    # nltk.download('averaged_perceptron_tagger')

    # Strip the df_keywords_only from stopwords
    # stop_words = set(stopwords.words('english'))
    # df_keywords_only["keywords"] = df_keywords_only["keywords"].apply(lambda x: " ".join([word for word in x.split() if word not in stop_words]))

    # print("Number of keywords after NLTK: " + str(len(df_keywords_only)))

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
