###############################################################################################
# 
# Welcome to PubGraph - A Knowledge Graph Generator for Scientific Publications.
#
###############################################################################################

from termcolor import colored
import argparse
import os
import pandas as pd

from helpers.cli_loader import load_bar
from pipeline.keyword_generator import *

if __name__ == "__main__":
    # Welcome message
    print(colored("Welcome to PubGraph!", "green", attrs=["bold"]))
    print(colored("--------------------", "green", attrs=["bold"]))
    print("")

    # Instantiate the parser
    parser = argparse.ArgumentParser(description="PubGraph: A Knowledge Graph Generator for Scientific Publications. This tool works best with PubCrawl - my other tool for crawling scientific publications.")

    # Add the arguments
    parser.add_argument("-s", "--standalone", action="store_true", help="Flag for standalone use of the application. Example: -s")
    parser.add_argument("-i", "--input_file", type=str, help="Input file to be analyzed. Example: -i arxiv-fulltext.json", required=True)
    parser.add_argument("-c", "--text_choice", type=str, help="Selection of key in JSON file that contains the text to be analyzed. Example: -c abstract", required=True)
    parser.add_argument("-o", "--output_dir", type=str, help="Output directory to save the Knowledge Graph files. Example -o /output")
    parser.add_argument("-l", "--local_file", type=str, help="Local file to be analyzed. Example: -l keywords.json")
    parser.add_argument("-r", "--rows", type=int, help="Number of rows to be analyzed. Example: -r 1000")

    # Execute the parse_args() method
    args = parser.parse_args()

    if args.standalone:
        print(colored("✓ Standalone mode enabled.", "yellow", attrs=["bold"]))
        print("")

        # Print the arguments
        print(colored("✓ You have chosen to process the file: {}".format(args.input_file), "yellow"))
        print("")
        
        print(colored("Loading data...", "yellow"))
        with load_bar(colored("Loading data...", "yellow")):
            data = pd.read_json(args.input_file, lines=True, orient="records")
            if args.rows:
                print(colored("✓ You have chosen to process only {} rows out of {}.".format(args.rows, len(data)), "yellow"))
                data = data.head(args.rows)
        print("")

        print(colored("Data loaded.", "green", attrs=["bold"]))
        # Print count of rows in the dataframe
        if args.rows:
            print(colored("There are {} rows in the dataframe, row limitation is enabled.".format(args.rows), "yellow"))
        else:
            print(colored("There are {} rows in the dataframe.".format(len(data)), "yellow"))
        print("")
        
        if args.local_file:
            print(colored("Loading local keywords file...", "yellow"))
            with load_bar(colored("Loading local keywords file...", "yellow")):
                df_keywords = pd.read_json(args.local_file, lines=True, orient="records")
            print(colored("Done loading local keywords file!", "green", attrs=["bold"]))
            print(colored("{} entries were processed.".format(len(df_keywords)), "yellow"))
            print("")

        else:
            # Generate keywords
            print(colored("Generating Keywords from {}...".format(args.text_choice) , "yellow"))
            df_keywords = generate_keywords(data, args.text_choice, args.output_dir)

            print(colored("Done generating keywords!", "green", attrs=["bold"]))
            print(colored("{} entries were processed.".format(len(df_keywords)), "yellow"))
            print("")

        # Create a keywords dictionary
        print(colored("Cleaning up the keywords and creating a dictionary...", "yellow"))
        keywords_dict = generate_keywords_dict(df_keywords, args.text_choice, 20)

