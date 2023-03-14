#########################################################################################################
#
# This file contians the OpenAlex_ID_Matcher class, which is used to match the OpenAlex IDs to the arXiv titles.
# 
# Author: @j0nasw
# Date: 2023-03-14
# 
#########################################################################################################

import pandas as pd
from pyalex import Works, Authors, Venues, Institutions, Concepts
import pyalex

from helpers.cli_loader import load_bar
from alive_progress import alive_bar


class OpenAlex():

    def __init__(self, docs_df):
        self.docs_df = docs_df
        self.error_counter = 0
        
        self.polite_email = "jonas.wilinski@tuhh.de"
        
        if self.polite_email != "":
            pyalex.config.email = self.polite_email
        
    def oalex_id_matcher(self):
       
        # Create a new dataframe with an id column and an oalex_data column to store the data
        oalex_df = pd.DataFrame(columns=["id", "title", "oalex_ids", "oalex_data"])
        
        with alive_bar(len(self.docs_df), title="Searching for OpenAlex WorkIDs...") as bar:        
            # Iterate over the dataframe and match the IDs
            for index, row in self.docs_df.iterrows():
                # Insert the docs id into the new dataframe
                oalex_df.at[index, "id"] = row["id"]
                
                # Strip the title from \n
                row["title"] = row["title"].replace("\n", "")
                
                # Insert the title into the new dataframe
                oalex_df.at[index, "title"] = row["title"]
                
                # Get the OpenAlex IDs
                works = (Works() \
                        .search_filter(title="*"+row["title"]+"*") \
                        .get())

                # If works is empty, create an empty dataframe
                if len(works) == 0:
                    works_df = pd.DataFrame()
                    self.error_counter += 1
                else:
                    try:
                        # Create a dataframe from the works
                        works_df = pd.DataFrame(works)
                        
                        # Check if works_df has a column "relevant_score"
                        if "relevance_score" not in works_df.columns:
                            # Save the oalex ids as a list in the oalex_df
                            oalex_df.at[index, "oalex_ids"] = works_df["id"].tolist()
                            
                        else:
                            # Drop entries with a relevance_score of NaN or 0
                            works_df = works_df.dropna(subset=["relevance_score"])
                            # Sort df by relevance score
                            works_df = works_df.sort_values(by="relevance_score", ascending=False)
                            
                            # Save the oalex ids as a list in the oalex_df
                            oalex_df.at[index, "oalex_ids"] = works_df["id"].tolist()
                            
                    except Exception:
                        works_df = pd.DataFrame()
                        self.error_counter += 1
                
                # Save the works_df in the oalex_df
                oalex_df.at[index, "oalex_data"] = works_df
                
                bar()
        
        print("Encountered " + str(self.error_counter) + " errors while searching for publications.")
            
        # Saving the dataframe as a json file
        with load_bar("Saving topic model..."):
            oalex_df.to_json("oalex_ids.json", orient="records", lines=True)
            
        return oalex_df