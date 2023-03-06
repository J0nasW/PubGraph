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
from pipeline.keyword_generator import generate_keywords

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
    parser.add_argument("-c", "--text_choice", type=str, help="Choice of text to be used for the Knowledge Graph. Example: -c abstract", required=True)
    parser.add_argument("-o", "--output_dir", type=str, help="Output directory to save the Knowledge Graph files. Example -o /output")
    parser.add_argument("-r", "--rows", type=int, help="Number of rows to be analyzed. Example: -r 1000")

    # Execute the parse_args() method
    args = parser.parse_args()

    if args.standalone:
        print(colored("Standalone mode enabled.", "yellow", attrs=["bold"]))
        print("")

        print(colored("Loading data...", "yellow"))
        with load_bar(colored("Loading data...", "yellow")):
            data = pd.read_json(args.input_file, lines=True, orient="records")
            if args.rows:
                data = data.head(args.rows)

        if args.text_choice == "abstract":
            with load_bar(colored("Generating Keywords from abstracts...", "yellow")):
                generate_keywords(data, args.text_choice, args.output_dir)
        elif args.text_choice == "fulltext":
            with load_bar(colored("Generating Keywords from fulltexts...", "yellow")):
                generate_keywords(data, args.text_choice, args.output_dir)