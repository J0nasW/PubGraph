import yake
from keybert import KeyBERT

import pandas as pd

# USER INPUT
NR_KEYWORDS = 5 # Number of keywords to be extracted per text

FLAIR_MODEL = "allenai/scibert_scivocab_cased" # Can be None; Can handle multiple Huggingface Models
HF_MODEL = None # Can be None; Can handle Huggingface Models with "Feature Extraction"
SENTENCE_TRANSFORMERS_MODEL = None # Can be None; Can handle Sentence Transformers Models from https://www.sbert.net/docs/pretrained_models.html

MODEL_MAX_TOKENS = 512 # Can be None

def generate_keywords(df, text_choice, output_dir):

    if HF_MODEL:
        from transformers.pipelines import pipeline
        hf_model = pipeline("feature-extraction", model=HF_MODEL)
        key_model = KeyBERT(model=hf_model)
    elif FLAIR_MODEL:
        from flair.embeddings import TransformerDocumentEmbeddings, WordEmbeddings, DocumentPoolEmbeddings
        flair_model = TransformerDocumentEmbeddings(FLAIR_MODEL)
        key_model = KeyBERT(model=flair_model)
    elif SENTENCE_TRANSFORMERS_MODEL:
        from sentence_transformers import SentenceTransformer
        sentence_transformers_model = SentenceTransformer(SENTENCE_TRANSFORMERS_MODEL)
        key_model = KeyBERT(model=sentence_transformers_model)
    else:
        print("No model selected. Please select a model.")
        exit()

    yake_kw_extractor = yake.KeywordExtractor(top=100)

    if text_choice == "abstract":
        abstracts = df["abstract"].tolist()
        abstract_keywords = []

        for text in abstracts:
            # Convert text to lowercase
            text = text.lower()

            # KeyBERT Keywords:
            #keywords_list.append(key_model.extract_keywords(text, keyphrase_ngram_range=(1, 2), stop_words="english", use_maxsum=True, nr_candidates=20, top_n=NR_KEYWORDS))

            # YAKE Keywords:
            candidates = yake_kw_extractor.extract_keywords(text)
            #yake_candidtates_list.append(candidates)

            candidates = [candidate[0] for candidate in candidates]
            abstract_keywords.append(key_model.extract_keywords(text, candidates=candidates, keyphrase_ngram_range=(1, 2), top_n=NR_KEYWORDS, stop_words="english"))
        
        # Save the keywords in the dataframe
        df["abstract_keywords"] = abstract_keywords


    elif text_choice == "fulltext":
        fulltexts = df["text"].tolist()
        fulltext_keywords = []

        for text in fulltexts:
            # Convert text to lowercase
            text = text.lower()

            # Limit the text to MODEL_MAX_TOKENS words if it is longer than MODEL_MAX_TOKENS
            if MODEL_MAX_TOKENS:
                if len(text.split()) > MODEL_MAX_TOKENS:
                    text = " ".join(text.split()[:MODEL_MAX_TOKENS])

            # KeyBERT Keywords:
            candidates = yake_kw_extractor.extract_keywords(text)
            candidates = [candidate[0] for candidate in candidates]
            fulltext_keywords.append(key_model.extract_keywords(text, candidates=candidates, keyphrase_ngram_range=(1, 2), top_n=NR_KEYWORDS, stop_words="english"))
       
        # Save the keywords in the dataframe
        df["fulltext_keywords"] = fulltext_keywords

    else:
        print("Please select a text choice.")
        exit()

    # Save the dataframe as a json file
    df.to_json(output_dir, orient="records", lines=True)