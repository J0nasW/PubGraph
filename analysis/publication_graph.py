#########################################################################################################
#
# The heart of the PubGraph library. This file contains the main class, PubGraph, which is used to generate a graph of publications.
# 
#########################################################################################################

import pandas as pd

class PublicationGraph():
    
    def __init__(self, text_choice, arxiv_metadata, topics, openalex_metadata):
        self.text_choice = text_choice
        self.arxiv_metadata = arxiv_metadata
        self.topics = topics
        self.openalex_metadata = openalex_metadata
        