#########################################################################################################
#
# This file contians the Author_Analysis class, which is used to analyse the authors of the arXiv papers.
# 
# Author: @j0nasw
# Date: 2023-03-14
# 
#########################################################################################################

import pandas as pd

import os

from alive_progress import alive_bar
from helpers.cli_loader import load_bar

# Visualization
import plotly.express as px
import plotly.offline as pyo

class AuthorAnalysis():
    
    def __init__(self, local_oalex_file, local_tm_file, text_choice):
        self.oalex_df = pd.read_json(local_oalex_file, lines=True, orient="records", dtype={"id": "string"})
        self.tm_df = pd.read_json(local_tm_file, lines=True, orient="records", dtype={"id": "string"})
        self.text_choice = text_choice
        
        self.nr_authors = 10
        
        
        
    def top_authors(self):
        
        # Load the oalex_df from file
        # oalex_df = pd.read_csv("data/oalex_df.csv")
        
        # Create a dataframe of all author entries found in the oalex_df
        authors_df = pd.DataFrame(columns=["author_id", "display_name", "orcid", "total_occurances", "position_first_occurances", "position_middle_occurances", "position_last_occurances", "position_only_occurances", "arxiv_ids_total", "arxiv_ids_first", "arxiv_ids_middle", "arxiv_ids_last", "arxiv_ids_only"])
        
        # Convert arxiv columns as string
        authors_df["arxiv_ids_total"] = authors_df["arxiv_ids_total"].astype(str)
        authors_df["arxiv_ids_first"] = authors_df["arxiv_ids_first"].astype(str)
        authors_df["arxiv_ids_middle"] = authors_df["arxiv_ids_middle"].astype(str)
        authors_df["arxiv_ids_last"] = authors_df["arxiv_ids_last"].astype(str)
        authors_df["arxiv_ids_only"] = authors_df["arxiv_ids_only"].astype(str)
        
        with alive_bar(len(self.oalex_df), title="Creating Author Graph...") as bar:        
            
            # Iterate over the oalex_df and get the authors
            for index, row in self.oalex_df.iterrows():
                # Get the authors of the current row
                items = row["oalex_data"]
                
                for item in items:
                    authors = item["authorships"]
                
                    # Iterate over the authors and add them to the authors_df
                    for author in authors:
                        author_element = author["author"]
                        author_id = str(author_element["id"])
                        # row_id = f'{row["id"]:0>7.4f}'
                        row_id = row["id"]
                        # Check if the author is already in the authors_df
                        if author_id in str(authors_df["author_id"]):
                            # print("Author already in authors_df. Name: " + author_element["display_name"] + ", ORCID: " + author_element["id"])
                            # Get the index of the author in the authors_df
                            author_index = authors_df[authors_df["author_id"] == author_id].index[0]
                            
                            # Update the total_occurances
                            authors_df.at[author_index, "total_occurances"] += 1
                            
                            # Update the arxiv_ids_total
                            authors_df.at[author_index, "arxiv_ids_total"] += "," + row_id
                            
                            # Check if the author is the first author
                            if author["author_position"] == "first":
                                # Update the position_first_occurances
                                authors_df.at[author_index, "position_first_occurances"] += 1
                                
                                if authors_df.at[author_index, "position_first_occurances"] == 1:
                                    # Update the arxiv_ids_first
                                    authors_df.at[author_index, "arxiv_ids_first"] = row_id
                                else:
                                    # Update the arxiv_ids_first
                                    authors_df.at[author_index, "arxiv_ids_first"] += "," + row_id
                                
                            # Check if the author is the last author
                            elif author["author_position"] == "last":
                                # Update the position_last_occurances
                                authors_df.at[author_index, "position_last_occurances"] += 1
                                
                                if authors_df.at[author_index, "position_last_occurances"] == 1:
                                    # Update the arxiv_ids_first
                                    authors_df.at[author_index, "arxiv_ids_last"] = row_id
                                else:
                                    # Update the arxiv_ids_last
                                    authors_df.at[author_index, "arxiv_ids_last"] += "," + row_id
                                
                            # Check if the author is the only author
                            elif len(authors) == 1:
                                # Update the position_only_occurances
                                authors_df.at[author_index, "position_only_occurances"] += 1
                                
                                if authors_df.at[author_index, "position_only_occurances"] == 1:
                                    # Update the arxiv_ids_first
                                    authors_df.at[author_index, "arxiv_ids_only"] = row_id
                                else:
                                    # Update the arxiv_ids_only
                                    authors_df.at[author_index, "arxiv_ids_only"] += "," + row_id
                                
                            # Else the author is a middle author
                            else:
                                # Update the position_middle_occurances
                                authors_df.at[author_index, "position_middle_occurances"] += 1
                                
                                if authors_df.at[author_index, "position_middle_occurances"] == 1:
                                    # Update the arxiv_ids_first
                                    authors_df.at[author_index, "arxiv_ids_middle"] = row_id
                                else:
                                    # Update the arxiv_ids_middle
                                    authors_df.at[author_index, "arxiv_ids_middle"] += "," + row_id
                                
                        # Else the author is not in the authors_df
                        else:
                            # Add the author to the authors_df
                            authors_df = authors_df.append({"author_id": author_id, "display_name": author_element["display_name"], "orcid": author_element["orcid"], "total_occurances": 1, "position_first_occurances": 0, "position_middle_occurances": 0, "position_last_occurances": 0, "position_only_occurances": 0, "arxiv_ids_total": row_id, "arxiv_ids_first": "", "arxiv_ids_middle": "", "arxiv_ids_last": "", "arxiv_ids_only": ""}, ignore_index=True)
                            
                            # Check if the author is the first author
                            if author["author_position"] == "first":
                                # Update the position_first_occurances
                                authors_df.at[len(authors_df)-1, "position_first_occurances"] = 1
                                
                                # Update the arxiv_ids_first
                                authors_df.at[len(authors_df)-1, "arxiv_ids_first"] = row_id
                                
                            # Check if the author is the last author
                            elif author["author_position"] == "last":
                                # Update the position_last_occurances
                                authors_df.at[len(authors_df)-1, "position_last_occurances"] += 1
                                
                                # Update the arxiv_ids_last
                                authors_df.at[len(authors_df)-1, "arxiv_ids_last"] = row_id
                                
                            # Check if the author is the only author
                            elif len(authors) == 1:
                                # Update the position_only_occurances
                                authors_df.at[len(authors_df)-1, "position_only_occurances"] = 1
                                
                                # Update the arxiv_ids_only
                                authors_df.at[len(authors_df)-1, "arxiv_ids_only"] = row_id
                                
                            # Else the author is a middle author
                            else:
                                # Update the position_middle_occurances
                                authors_df.at[len(authors_df)-1, "position_middle_occurances"] = 1
                                
                                # Update the arxiv_ids_middle
                                authors_df.at[len(authors_df)-1, "arxiv_ids_middle"] = row_id
                bar()
                
                        
            # Sort the authors_df by the total_occurances
            authors_df = authors_df.sort_values(by="total_occurances", ascending=False, ignore_index=True)
            
            # Store the arxiv_ids_total as a list of strings
            authors_df["arxiv_ids_total"] = authors_df["arxiv_ids_total"].str.split(",")
            authors_df["arxiv_ids_first"] = authors_df["arxiv_ids_first"].str.split(",")
            authors_df["arxiv_ids_middle"] = authors_df["arxiv_ids_middle"].str.split(",")
            authors_df["arxiv_ids_last"] = authors_df["arxiv_ids_last"].str.split(",")
            authors_df["arxiv_ids_only"] = authors_df["arxiv_ids_only"].str.split(",")
                
        with load_bar("Visiualizing Authors and Contributions..."):
            
            # Create an interactive graph using plotly to visualize the authors total occurances
            fig1 = px.bar(authors_df.head(25), x="display_name", y="total_occurances", title="Total Occurances of Authors", labels={"display_name": "Author", "total_occurances": "Total Occurances"})
            fig1.update_layout(xaxis={'categoryorder':'total descending'})
            
            # Save plot as HTML file
            pyo.plot(fig1, filename="output/visualizations/authors_total_occurances.html")
        
        with load_bar("Saving the Auhtor Graph..."):
            
            # Save the authors_df to a json file.
            authors_df_name = "output/" + self.text_choice + "_author_analysis.json"
            authors_df.to_json(authors_df_name, orient="records", lines=True)
        
        # Return the top nr_authors authors
        return authors_df_name
    
    def authors_and_topics(self, local_author_analysis_file):
        
        # Load the DFs
        authors_df = pd.read_json(local_author_analysis_file, orient="records", lines=True)
        topics_df = self.tm_df
        
        # Create a new dataframe for the authors and topics
        authors_and_topics_df = pd.DataFrame(columns=["author_id", "display_name", "topics"])
        
        with alive_bar(len(authors_df), title="Creating Author Graph...") as bar:
        
            # Iterate over the authors_df
            for index, row in authors_df.iterrows():
                for arxiv_id in row["arxiv_ids_total"]:
                    # Get the topics for the arxiv_id
                    topics = topics_df[topics_df["id"] == arxiv_id]["topic"]
                    
                    # Check if the author is already in the authors_and_topics_df
                    if row["author_id"] in authors_and_topics_df["author_id"].values:
                        print("Author Already in DF: " + row["display_name"])
                        # Get the index of the author in the authors_and_topics_df
                        author_index = authors_and_topics_df[authors_and_topics_df["author_id"] == row["author_id"]].index[0]
                        
                        # Update the topics
                        authors_and_topics_df.at[author_index, "topics"] += topics
                        
                    # Else the author is not in the authors_and_topics_df
                    else:
                        # Add the author to the authors_and_topics_df
                        print("New Author: " + row["display_name"])
                        authors_and_topics_df = authors_and_topics_df.append({"author_id": row["author_id"], "display_name": row["display_name"], "topics": topics}, ignore_index=True)
                        
                bar()
                        
                                            
        # print(authors_and_topics_df)
        
        # Visualize the authors and topics using plotly
        fig2 = px.scatter(authors_and_topics_df, x="author_id", y="topics", color="display_name", title="Authors and Topics", labels={"topics": "Topics"})
        
        # Save plot as HTML file
        pyo.plot(fig2, filename="output/visualizations/authors_topic_models.html")
        
        # Save the authors_and_topics_df to a json file.
        author_tm_df_name = "output/" + self.text_choice + "_author_topic_model.json"
        authors_and_topics_df.to_json(author_tm_df_name, orient="records", lines=True)
        
        # Return the filename of the authors_and_topics_df
        return author_tm_df_name