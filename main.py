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
from pipeline.topic_modelling import *
from pipeline.openalex_id_matcher import *

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
    parser.add_argument("-r", "--rows", type=int, help="Number of rows to be analyzed. Example: -r 1000")
    parser.add_argument("-k", "--keywords", action="store_true", help="Generate Keywords or use local keywords file. Example: -k")
    parser.add_argument("-t", "--topic_model", action="store_true", help="Generate Topic Model or use local topic model file. Example: -t")
    parser.add_argument("-o", "--open_alex", action="store_true", help="Get the OpenAlex IDs for the publications. Example: -o")

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
        print("")
        
        if args.keywords:

            # Ask the user if they want to use local keywords files
            print(colored("Do you want to use a local keywords file? (Yes / No)", "blue", attrs=["bold"]))
            print("")
            # Get user input
            user_input_local_keywords = input()
            
        if args.topic_model:

            # Ask the user if they want to use local topic model file
            print(colored("Do you want to use a local topic model file? (Yes / No)", "blue", attrs=["bold"]))
            print("")
            # Get user input
            user_input_local_topic_model = input()
            
        if args.open_alex:
            
            # Ask the user if they want to use local OpenAlex IDs
            print(colored("Do you want to use a local OpenAlex IDs file? (Yes / No)", "blue", attrs=["bold"]))
            print("")
            # Get user input
            user_input_local_openalex = input()
            
            
        if args.keywords:

            # Check if the user wants to use a local keywords file
            if user_input_local_keywords.lower() == "yes":
                # Ask the user for the path to the local keywords file
                print(colored("Please enter the path to the local keywords file.", "blue", attrs=["bold"]))
                print("")
                # Get user input
                local_keyword_file = input(colored("Path: ", "blue"))
                
                # Check if path is valid
                if not os.path.isfile(local_keyword_file):
                    print(colored("The provided file does not exist.", "red"))
                    os._exit(1)

                # Load the local keywords file
                print(colored("The provided path is valid. Loading local keywords file...", "yellow"))
                with load_bar(colored("Loading local keywords file...", "yellow")):
                    df_keywords = pd.read_json(local_keyword_file, lines=True, orient="records")
                print(colored("Done loading local keywords file!", "green", attrs=["bold"]))
                print(colored("{} entries were processed.".format(len(df_keywords)), "yellow"))
                print("")
                
            else:
                # Generate keywords
                # Ask the user, how many keywords they want to generate
                # print(colored("How many keywords do you want to generate?", "blue", attrs=["bold"]))
                # print("")
                # Get user input
                # num_keywords = input(colored("Top N Keywords: ", "blue"))
                # Check if the user input is a number
                # if not num_keywords.isdigit():
                #     print(colored("Please enter a valid number.", "red"))
                #     os._exit(1)
                print(colored("Generating Keywords from {}...".format(args.text_choice) , "yellow"))
                keyword_gen = Keyword_Generator(data, args.text_choice)
                df_keywords = keyword_gen.generate_keywords()

                print(colored("Done generating keywords!", "green", attrs=["bold"]))
                print(colored("{} entries were processed.".format(len(df_keywords)), "yellow"))
                print("")
                
        if args.topic_model:

            # Check if the user wants to use a local topic model file
            if user_input_local_topic_model.lower() == "yes":
                # Ask the user for the path to the local keywords file
                print(colored("Please enter the path to the local topic model file.", "blue", attrs=["bold"]))
                print("")
                # Get user input
                local_tm_file = input(colored("Path: ", "blue"))
                
                # Check if path is valid
                if not os.path.isfile(local_tm_file):
                    print(colored("The provided file does not exist.", "red"))
                    os._exit(1)

                # Load the local keywords file
                print(colored("The provided path is valid. Loading local topic model file...", "yellow"))
                with load_bar(colored("Loading local topic model file...", "yellow")):
                    tm_df = pd.read_json(local_tm_file, lines=True, orient="records")
                print(colored("Done loading local topic model file!", "green", attrs=["bold"]))
                print(colored("{} entries were processed.".format(len(tm_df)), "yellow"))
                print("")

            else:
                # Generate topic model
                # Ask the user, how many topics they want to generate
                # print(colored("How many topics do you want to generate?", "blue", attrs=["bold"]))
                # print("")
                # Get user input
                # num_topics = input(colored("N Topics: ", "blue"))
                # Check if the user input is a number
                # if not num_topics.isdigit():
                #     print(colored("Please enter a valid number.", "red"))
                #     os._exit(1)
                print(colored("Generating Topic Model from {}...".format(args.text_choice) , "yellow"))
                # tm_gen = TopicModelling(data, args.text_choice, df_keywords, num_topics)
                tm_gen = TopicModelling(data, args.text_choice)
                tm_df = tm_gen.tm_generator()

                print(colored("Done generating topic model!", "green", attrs=["bold"]))
                # print(colored("The following topics were generated:", "yellow"))
                # print(tm_df)
                print("")
                
        if args.open_alex:

            # Check if the user wants to use a local topic model file
            if user_input_local_openalex.lower() == "yes":
                # Ask the user for the path to the local keywords file
                print(colored("Please enter the path to the local openalex file.", "blue", attrs=["bold"]))
                print("")
                # Get user input
                local_oalex_file = input(colored("Path: ", "blue"))
                
                # Check if path is valid
                if not os.path.isfile(local_tm_file):
                    print(colored("The provided file does not exist.", "red"))
                    os._exit(1)

                # Load the local keywords file
                print(colored("The provided path is valid. Loading local openalex file...", "yellow"))
                with load_bar(colored("Loading local topic model file...", "yellow")):
                    openalex = pd.read_json(local_oalex_file, lines=True, orient="records")
                print(colored("Done loading local openalex file!", "green", attrs=["bold"]))
                print(colored("{} entries were processed.".format(len(tm_df)), "yellow"))
                print("")

            else:
                print(colored("Searching for OpenAlex WorkIDs that match your publications..." , "yellow"))
                # tm_gen = TopicModelling(data, args.text_choice, df_keywords, num_topics)
                oalex_gen = OpenAlex(data)
                openalex = oalex_gen.oalex_id_matcher()

                print(colored("Done searching for OpenAlex Work IDs!", "green", attrs=["bold"]))
                print(colored("{} entries were processed.".format(len(openalex)), "yellow"))
                print("")
            

        # Goodbye message
        print(colored("--------------------", "green", attrs=["bold"]))
        print(colored("Thank you for using PubGraph. Goodbye!", "green", attrs=["bold"]))
        print("")

        # Create a keywords dictionary
        # print(colored("Cleaning up the keywords and creating a dictionary...", "yellow"))
        # keywords_dict = generate_keywords_dict(df_keywords, args.text_choice, 20)

