#########################################################################################################
#
# This file contians the OpenAlex_ID_Matcher class, which is used to match the OpenAlex IDs to the arXiv titles.
# 
# Author: @j0nasw
# Date: 2023-03-14
# 
#########################################################################################################

import pandas as pd
import re
from fuzzywuzzy import fuzz
from pyalex import Works, Authors, Venues, Institutions, Concepts
import pyalex

from helpers.cli_loader import load_bar
from alive_progress import alive_bar


class OpenAlex():

    def __init__(self, docs_df, text_choice):
        self.docs_df = docs_df
        self.text_choice = text_choice
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
                
                # Strip all special characters but leave - using re
                row["title"] = re.sub(r"[^a-zA-Z0-9-]", " ", row["title"])
                
                # Insert the title into the new dataframe
                oalex_df.at[index, "title"] = row["title"]
                
                # Insert the authors into the new dataframe
                oalex_df.at[index, "authors"] = row["authors"]
                
                # Get the OpenAlex IDs
                works = (Works() \
                        .search_filter(title="*"+str(row["title"])+"*") \
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
                            # Drop entries with a relevance_score of NaN or below float 100
                            works_df = works_df.dropna(subset=["relevance_score"])
                            works_df = works_df[works_df["relevance_score"] > 100]
                            
                            # Sort df by relevance score
                            works_df = works_df.sort_values(by="relevance_score", ascending=False)
                            
                            # Compare the authors with the works_df[authorships][display_name] and drop the rows where the authors don't match. Use fuzzy compare to match the names.
                            for index2, row2 in works_df.iterrows():
                                authors_list = []
                                # CHeck if row[authors] contain the word "and"
                                if " and " in row["authors"]:
                                    # Split the authors at "and"
                                    arxiv_authors = row["authors"].split(" and ")
                                elif "," in row["authors"]:
                                    arxiv_authors = row["authors"].split(",")
                                else:
                                    arxiv_authors = [row["authors"]]
                                # Get the authors of the current row
                                authorships = row2["authorships"]
                                
                                
                                for authorship in authorships:
                                    author = authorship["author"]
                                    author_name = author["display_name"]
                                    authors_list.append(author_name)
                                        
                                # ratio = SequenceMatcher(None, authors_list, arxiv_authors).ratio()
                                ratio = fuzz.token_set_ratio(authors_list, arxiv_authors)
                                    
                                # Drop the row if the ratio is below 0.5
                                if ratio < 50:
                                    works_df = works_df.drop(index2)
                                    self.error_counter += 1
                                    # print("Authors from OpenAlex: " + str(authors_list))
                                    # print("Authors from arXiv: " + str(arxiv_authors))
                                    # print("⚠︎ Authors don't match. Ratio is: " + str(ratio))
                            
                    except Exception:
                        works_df = pd.DataFrame()
                        self.error_counter += 1
                
                # Save the works_df in the oalex_df
                oalex_df.at[index, "oalex_data"] = works_df
                
                bar()
        
        if self.error_counter > 0:
            print("⚠︎ Encountered " + str(self.error_counter) + " errors while searching for publications.")
            # Print count of processed openalex work ids
            
        # Saving the dataframe as a json file
        with load_bar("Saving the OpenAlex IDs..."):
            oalex_name = "output/" + self.text_choice + "_openalex_ids.json"
            oalex_df.to_json(oalex_name, orient="records", lines=True)
            
        return oalex_name