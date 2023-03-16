###############################################################################################
# 
# Welcome to PubGraph - A Knowledge Graph Generator for Scientific Publications.
#
###############################################################################################

from termcolor import colored
import argparse
import os, sys
import pandas as pd

from helpers.cli_loader import load_bar
from pipeline.keyword_generator import *
from pipeline.topic_modelling import *
from pipeline.openalex_id_matcher import *
from pipeline.author_analysis import *

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
    parser.add_argument("-f", "--force", action="store_true", help="Force rerun of the pipeline. Example: -f")
    parser.add_argument("-m", "--manual_files", action="store_true", help="Use manual file locations for the pipeline. Example: -m")    
    
    # parser.add_argument("-k", "--keywords", action="store_true", help="Generate Keywords or use local keywords file. Example: -k")
    # parser.add_argument("-t", "--topic_model", action="store_true", help="Generate Topic Model or use local topic model file. Example: -t")
    # parser.add_argument("-o", "--open_alex", action="store_true", help="Get the OpenAlex IDs for the publications. Example: -o")
    
    parser.add_argument("-a", "--analyze", action="store_true", help="Analyze the data. Example: -a")

    # Execute the parse_args() method
    args = parser.parse_args()

    if args.standalone:
        print(colored("✓ Standalone mode enabled.", "yellow", attrs=["bold"]))
        print("")

        # Print the arguments
        print(colored("✓ You have chosen to process the file: {}".format(args.input_file), "yellow"))
        print(colored("✓ You have chosen to process the label: {}".format(args.text_choice), "yellow"))
        # Create an output folder for all the JSONs
        if not os.path.exists("output"):
            os.makedirs("output")
        if not os.path.exists("output/visualizations"):
            os.makedirs("output/visualizations")
            
        print(colored("✓ Created output folder.", "yellow"))
        print("")
        
        # Loading the data
        with load_bar(colored("Loading data...", "yellow")):
            data = pd.read_json(args.input_file, lines=True, orient="records")
            if args.rows:
                print(colored("✓ You have chosen to process only {} rows out of {}.".format(args.rows, len(data)), "yellow"))
                data = data.head(args.rows)
            else:
                print(colored("✓ You have chosen to process all {} rows.".format(len(data)), "yellow"))
        print(colored("Data loaded.", "green", attrs=["bold"]))
        print("")
            
        # Check for Keyword File
        if os.path.isfile("output/" + args.text_choice + "_keywords.json") and not args.force:
            print(colored("We found the following file: " + args.text_choice + "_keywords.json", "yellow"))
            
            # Ask the user if they want to re-run OpenAlex ID Matcher
            print(colored("Do you want to re-run the KeywordGenerator and overwrite the existing file? (Yes / No)", "blue", attrs=["bold"]))
            # Get user input
            user_input_rerun_keyword_gen = input()
            
            if not user_input_rerun_keyword_gen.lower() == "yes":
                print(colored("Skipping KeywordGenerator and using the local file instead...", "yellow"))
                local_keyword_file = "output/" + args.text_choice + "_keywords.json"
                print("")
        else:
            if args.manual_files:
                print(colored("We did not find any Keywords file.", "yellow"))
                # Ask the user for the path to the local keywords file
                print(colored("If you have the Keywords JSON file in another place, please provide the path here. Otherwise skip this step with ENTER.", "blue", attrs=["bold"]))
                print("")
                # Get user input
                input_local_keyword_file = input()
                if os.path.isfile(input_local_keyword_file):
                    local_tm_file = input_local_keyword_file
                else:
                    print(colored("The file you provided does not exist. Skipping...", "yellow"))
                    user_input_rerun_keyword_gen = "yes"
            else:
                user_input_rerun_keyword_gen = "yes"
            
        # Check for Topic Modelling File
        if os.path.isfile("output/" + args.text_choice + "_topic_model.json") and not args.force:
            print(colored("We found the following file: " + args.text_choice + "_topic_model.json", "yellow"))
            
            # Ask the user if they want to re-run Topic Modelling
            print(colored("Do you want to re-run the Topic Modelling and overwrite the existing file? (Yes / No)", "blue", attrs=["bold"]))
            
            # Get user input
            user_input_rerun_tm = input()
            
            if not user_input_rerun_tm.lower() == "yes":
                print(colored("Skipping Topic Modelling and using the local file instead...", "yellow"))
                local_tm_file = "output/" + args.text_choice + "_topic_model.json"
                print("")
        else:
            if args.manual_files:
                print(colored("We did not find any Topic Model file.", "yellow"))
                # Ask the user for the path to the local keywords file
                print(colored("If you have the Topic Modelling JSON file in another place, please provide the path here. Otherwise skip this step with ENTER.", "blue", attrs=["bold"]))
                print("")
                # Get user input
                input_local_tm_file = input()
                if os.path.isfile(input_local_tm_file):
                    local_tm_file = input_local_tm_file
                else:
                    print(colored("The file you provided does not exist. Skipping...", "yellow"))
                    user_input_rerun_keyword_gen = "yes"
            else:
                user_input_rerun_tm = "yes"
            
        # Check for OpenAlex File
        if os.path.isfile("output/" + args.text_choice + "_openalex_ids.json") and not args.force:
            print(colored("We found the following file: " + args.text_choice + "_openalex_ids.json", "yellow"))
                            
            # Ask the user if they want to re-run OpenAlex ID Matcher
            print(colored("Do you want to re-run the OpenAlex ID Matcher and overwrite the existing file? (Yes / No)", "blue", attrs=["bold"]))
            
            # Get user input
            user_input_rerun_openalex = input()
            
            if not user_input_rerun_openalex.lower() == "yes":
                print(colored("Skipping OpenAlex ID Matcher and using the local file instead...", "yellow"))
                local_oalex_file = "output/" + args.text_choice + "_openalex_ids.json"
                print("")
        else:
            if args.manual_files:
                print(colored("We did not find any OpenAlex file.", "yellow"))
                # Ask the user for the path to the local keywords file
                print(colored("If you have the OpenAlex ID file in another place, please provide the path here. Otherwise skip this step with ENTER.", "blue", attrs=["bold"]))
                print("")
                # Get user input
                input_local_oalex_file = input()
                if os.path.isfile(input_local_oalex_file):
                    local_oalex_file = input_local_oalex_file
                else:
                    print(colored("The file you provided does not exist. Skipping...", "yellow"))
                    user_input_rerun_keyword_gen = "yes"
            else:
                user_input_rerun_openalex = "yes"
                

        ### Run Generators if necessary
        
        # Keyword Generator
        if user_input_rerun_keyword_gen.lower() == "yes":
            print(colored("Generating Keywords from {}...".format(args.text_choice) , "yellow"))
            keyword_gen = Keyword_Generator(data, args.text_choice)
            local_keyword_file = keyword_gen.generate_keywords()
            print("")
            print(colored("Done generating keywords!", "green", attrs=["bold"]))
            print("")
        
        # Topic Modelling
        if user_input_rerun_tm.lower() == "yes":
            print(colored("Generating Topic Model from {}...".format(args.text_choice) , "yellow"))
            tm_gen = TopicModelling(data, args.text_choice)
            local_tm_file = tm_gen.tm_generator()
            print("")
            print(colored("Done generating topic model!", "green", attrs=["bold"]))
            print("")
        
        # OpenAlex ID Matcher
        if user_input_rerun_openalex.lower() == "yes":
            print(colored("Searching for OpenAlex WorkIDs that match your publications..." , "yellow"))
            oalex_gen = OpenAlex(data, args.text_choice)
            local_oalex_file = oalex_gen.oalex_id_matcher()

            print(colored("Done searching for OpenAlex Work IDs!", "green", attrs=["bold"]))
            print("")
            

        ## Analyze the Files
        
        if args.analyze:
            
            # Check for the Files
            if not os.path.isfile(local_keyword_file):
                print(colored("The local Keywords file does not exist.", "red", attrs=["bold"]))
                print("")
                sys.exit()
            if not os.path.isfile(local_tm_file):
                print(colored("The local Topic Modelling file does not exist.", "red", attrs=["bold"]))
                print("")
                sys.exit()
            if not os.path.isfile(local_oalex_file):
                print(colored("The local OpenAlex file does not exist.", "red", attrs=["bold"]))
                print("")
                sys.exit()
            
            ### Check if parts of the analysis has already been done
            
            # Check for Author Analysis File
            if os.path.isfile("output/" + args.text_choice + "_author_analysis.json") and not args.force:
                print(colored("The author analysis file already exists. Do you want to rerun the author analysis? (Yes / No)", "blue", attrs=["bold"]))
                
                # Get user input
                user_input_rerun_author_analysis = input()
                
                if not user_input_rerun_author_analysis.lower() == "yes":
                    print(colored("Skipping Author Analysis and using the local file instead...", "yellow"))
                    local_author_analysis_file = "output/" + args.text_choice + "_author_analysis.json"
                    print("")
            else:
                if args.manual_files:
                    print(colored("We did not find any Author Analysis file.", "yellow"))
                    # Ask the user for the path to the local author analysis file
                    print(colored("If you have the Author Analysis JSON file in another place, please provide the path here. Otherwise skip this step with ENTER.", "blue", attrs=["bold"]))
                    print("")
                    # Get user input
                    input_local_author_analysis_file = input()
                    if os.path.isfile(input_local_author_analysis_file):
                        local_author_analysis_file = input_local_author_analysis_file
                    else:
                        print(colored("The file you provided does not exist. Skipping...", "yellow"))
                        user_input_rerun_keyword_gen = "yes"
                else:
                    user_input_rerun_author_analysis = "yes"
                    
            # Check for Author Topic Model Visualization File
            if os.path.isfile("output/" + args.text_choice + "_author_topic_model.json") and not args.force:
                print(colored("The author topic model file already exists. Do you want to rerun the author topic model visualization? (Yes / No)", "blue", attrs=["bold"]))
                
                # Get user input
                user_input_rerun_author_tm = input()
                
                if not user_input_rerun_author_tm.lower() == "yes":
                    print(colored("Skipping Author Topic Model Visualization and using the local file instead...", "yellow"))
                    local_author_tm_file = "output/" + args.text_choice + "_author_topic_model.json"
                    print("")
            else:
                if args.manual_files:
                    print(colored("We did not find any Author Topic Model file.", "yellow"))
                    # Ask the user for the path to the local author analysis file
                    print(colored("If you have the Author Topic Model JSON file in another place, please provide the path here. Otherwise skip this step with ENTER.", "blue", attrs=["bold"]))
                    print("")
                    # Get user input
                    input_local_author_tm = input()
                    if os.path.isfile(input_local_author_tm):
                        local_author_tm_file = input_local_author_tm
                    else:
                        print(colored("The file you provided does not exist. Skipping...", "yellow"))
                        user_input_rerun_keyword_gen = "yes"
                else:
                    user_input_rerun_author_tm = "yes"
            
            # Run Analysis modules if necessary
            
            authors_class = AuthorAnalysis(local_oalex_file, local_tm_file, args.text_choice)
            
            # Author Analysis
            if user_input_rerun_author_analysis.lower() == "yes":
                print(colored("Generating Author Analysis..." , "yellow"))
                local_author_analysis_file = authors_class.top_authors()
                print(colored("Done generating author analysis!", "green", attrs=["bold"]))
                print("")
                    
            # Author Topic Model Visualization
            if user_input_rerun_author_tm.lower() == "yes":
                print(colored("Generating Author Topic Model Graph..." , "yellow"))
                local_author_tm_file = authors_class.authors_and_topics(local_author_analysis_file)
                print(colored("Done generating a topic model visualization with the authors!", "green", attrs=["bold"]))
                print("")
            
        # if args.keywords:

        #     # Check if the user wants to use a local keywords file
        #     if user_input_local_keywords.lower() == "yes":
        #         # Ask the user for the path to the local keywords file
        #         print(colored("Please enter the path to the local keywords file.", "blue", attrs=["bold"]))
        #         print("")
        #         # Get user input
        #         local_keyword_file = input(colored("Path: ", "blue"))
                
        #         # Check if path is valid
        #         if not os.path.isfile(local_keyword_file):
        #             print(colored("The provided file does not exist.", "red"))
        #             os._exit(1)

        #         # Load the local keywords file
        #         print(colored("The provided path is valid. Loading local keywords file...", "yellow"))
        #         with load_bar(colored("Loading local keywords file...", "yellow")):
        #             df_keywords = pd.read_json(local_keyword_file, lines=True, orient="records")
        #         print(colored("Done loading local keywords file!", "green", attrs=["bold"]))
        #         print(colored("{} entries were processed.".format(len(df_keywords)), "yellow"))
        #         print("")
                
        #     else:
        #         # Generate keywords
        #         # Ask the user, how many keywords they want to generate
        #         # print(colored("How many keywords do you want to generate?", "blue", attrs=["bold"]))
        #         # print("")
        #         # Get user input
        #         # num_keywords = input(colored("Top N Keywords: ", "blue"))
        #         # Check if the user input is a number
        #         # if not num_keywords.isdigit():
        #         #     print(colored("Please enter a valid number.", "red"))
        #         #     os._exit(1)
        #         print(colored("Generating Keywords from {}...".format(args.text_choice) , "yellow"))
        #         keyword_gen = Keyword_Generator(data, args.text_choice)
        #         df_keywords = keyword_gen.generate_keywords()

        #         print(colored("Done generating keywords!", "green", attrs=["bold"]))
        #         print(colored("{} entries were processed.".format(len(df_keywords)), "yellow"))
        #         print("")
                
        # if args.topic_model:

        #     # Check if the user wants to use a local topic model file
        #     if user_input_local_topic_model.lower() == "yes":
        #         # Ask the user for the path to the local keywords file
        #         print(colored("Please enter the path to the local topic model file.", "blue", attrs=["bold"]))
        #         print("")
        #         # Get user input
        #         local_tm_file = input(colored("Path: ", "blue"))
                
        #         # Check if path is valid
        #         if not os.path.isfile(local_tm_file):
        #             print(colored("The provided file does not exist.", "red"))
        #             os._exit(1)

        #         # Load the local keywords file
        #         print(colored("The provided path is valid. Loading local topic model file...", "yellow"))
        #         with load_bar(colored("Loading local topic model file...", "yellow")):
        #             tm_df = pd.read_json(local_tm_file, lines=True, orient="records")
        #         print(colored("Done loading local topic model file!", "green", attrs=["bold"]))
        #         print(colored("{} entries were processed.".format(len(tm_df)), "yellow"))
        #         print("")

        #     else:
        #         # Generate topic model
        #         # Ask the user, how many topics they want to generate
        #         # print(colored("How many topics do you want to generate?", "blue", attrs=["bold"]))
        #         # print("")
        #         # Get user input
        #         # num_topics = input(colored("N Topics: ", "blue"))
        #         # Check if the user input is a number
        #         # if not num_topics.isdigit():
        #         #     print(colored("Please enter a valid number.", "red"))
        #         #     os._exit(1)
        #         print(colored("Generating Topic Model from {}...".format(args.text_choice) , "yellow"))
        #         # tm_gen = TopicModelling(data, args.text_choice, df_keywords, num_topics)
        #         tm_gen = TopicModelling(data, args.text_choice)
        #         tm_df = tm_gen.tm_generator()

        #         print(colored("Done generating topic model!", "green", attrs=["bold"]))
        #         # print(colored("The following topics were generated:", "yellow"))
        #         # print(tm_df)
        #         print("")
                
        # if args.open_alex:

        #     if user_input_rerun_openalex.lower() == "yes":
        #         print(colored("Searching for OpenAlex WorkIDs that match your publications..." , "yellow"))
        #         # tm_gen = TopicModelling(data, args.text_choice, df_keywords, num_topics)
        #         oalex_gen = OpenAlex(data, args.text_choice)
        #         openalex = oalex_gen.oalex_id_matcher()

        #         print(colored("Done searching for OpenAlex Work IDs!", "green", attrs=["bold"]))
        #         print(colored("{} entries were processed.".format(len(openalex)), "yellow"))
        #         print("")
                
        #     else:
        #         # Load the local OpenAlex file
        #         print(colored("The provided path is valid. Loading local openalex file inside the script...", "yellow"))
        #         # with load_bar(colored("Loading local topic model file...", "yellow")):
        #         #     openalex = pd.read_json(local_oalex_file, lines=True, orient="records", dtype={"id": "string"})
        #         # print(colored("Done loading local openalex file!", "green", attrs=["bold"]))
        #         # print(colored("{} entries were processed.".format(len(openalex)), "yellow"))
        #         print("")
            
            
        # if args.analyze:
        #     print(colored("Now you can analyze your data..." , "yellow"))
        #     print("")
            
        #     # Author Analysis
        #     if user_input_rerun_author_analysis.lower() == "yes":
        #         print(colored("Generating Author Analysis..." , "yellow"))
        #         author_analysis = AuthorAnalysis(local_oalex_file, args.text_choice)
        #         top_authors = author_analysis.top_authors()
        #         print(colored("Done generating author analysis!", "green", attrs=["bold"]))
        #         print("")
        #         print(top_authors)
        #         print("")
                    
        #     # Author Topic Model Visualization
        #     if os.path.isfile(args.text_choice + "_author_analysis.json") & os.path.isfile(args.text_choice + "_topic_model.json"):
        #         print(colored("Generating Author Topic Model Graph..." , "yellow"))
        #         authors_and_topics = AuthorAnalysis(local_oalex_file, args.text_choice)
        #         authors_and_topics.authors_and_topics()
        #         print(colored("Done generating a topic model visualization with the authors!", "green", attrs=["bold"]))
        #         print("")


        # Goodbye message
        print(colored("--------------------", "green", attrs=["bold"]))
        print(colored("Thank you for using PubGraph. Goodbye!", "green", attrs=["bold"]))
        print("")

        # Create a keywords dictionary
        # print(colored("Cleaning up the keywords and creating a dictionary...", "yellow"))
        # keywords_dict = generate_keywords_dict(df_keywords, args.text_choice, 20)

