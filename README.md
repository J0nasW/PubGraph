# PubGraph
A Knowledge Graph Generator for Scientific Publications

### Overview
PubGraph is a tool for generating a knowledge graph from scientific publications. It is designed to be used with the [PubCrawl](https://github.com/J0nasW/PubCrawl) but can also be used standalone. At the moment, you can only process the arXiv metatata JSON found at the Kaggle dataset [arXiv Metadata](https://www.kaggle.com/Cornell-University/arxiv). The tool will extract either abstracts or any other key (e.g., fulltexts) from the JSON file and in a first step generate keywords, topic models and OpenAlex WorkID matches.

Next up, it will use the generated information to analyze the publications and their authors. This part is still in development. 

## Installation
To start, clone the repository and install the package using the requirements file:

```bash
git clone https://github.com/J0nasW/PubGraph.git
cd pubgraph

pip install -r requirements.txt
```

It is strongly advised to use this library with a GPU enabled environment. If you do not have a GPU, you can still use the library but it will take a lot longer to process the data.

## Usage
PubGraph can be used in two ways: either as a standalone tool or as a module for PubCrawl.

### Standalone
To use PubGraph as a standalone tool, simply run the `pubgraph.py` script with the following arguments:

```bash
python main.py -s -i <input_file> -c <text_choice>
```

The input file should be a JSON file containing the publications to be processed. You can choose any key inside your JSON file to be analyzed, provided you state the flag -c and denote for example the key abstract with `abstract` (or fulltexts with `text`). The library will automatically create an output folder for any generated files and visualizations.

PubGraph is equipped with a lot of convenience features. For example, you can use the `-r` flag to only process a certain number of rows. This is very helpful for testing purposes. You can also use the `-f` flag to force the pipeline to rerun even if there is an existing output folder. This is useful if you want to rerun the pipeline with different parameters. Furthermore, you can use the `-a` flag to only run the analysis pipeline. This is useful if you want to rerun the analysis with different parameters.

The library is actively monitoring the directory and will detect existing output files. If you want to rerun the pipeline, you have to delete the output folder first or declare a rerun in the user input. Using the `-m` flag, you can also manually specify all input files from the generation pipeline. This is useful if you want to rerun the analysis with different parameters that are stored in another location.

### Examples

#### Standalone
Here are some examples of how to use PubGraph as a standalone tool:

##### Generation Pipeline
To run the generation pipeline on the first 1000 rows of the arXiv metadata JSON file while analyzing only the abstracts, use the following command:

```bash
python main.py -s -i arxiv-metadata-oai-snapshot.json -c abstract -r 1000
```

If you wish to run the generation pipeline on the first 1000 rows of fulltexts instead, first you should use [PubCrawl](https://github.com/J0nasW/PubCrawl) to download and process the fulltexts. Then, you can use the following command:

```bash
python main.py -s -i arxiv-metadata-oai-snapshot.json -c text -r 1000
```

You also have the chance of running the pipeline on the full dataset. However, this will take a lot of time and memory. If you wish to do so, simply remove the `-r` flag.

```bash
python main.py -s -i arxiv-metadata-oai-snapshot.json -c text
```

##### Analysis Pipeline
To run the analysis pipeline on the first 1000 rows of the arXiv metadata JSON file while analyzing only the abstracts, provide the `-a` flag:

```bash
python main.py -s -i arxiv-metadata-oai-snapshot.json -c abstract -r 1000 -a
```

If you have already run the generation pipeline the analysis pipeline will automatically detect the existing output files and use them. If you wish to rerun the analysis pipeline, you have to delete the output folder first or declare a rerun in the user input. You can also use the `-f` flag to force the pipeline to rerun even if there is an existing output folder.

```bash
python main.py -s -i arxiv-metadata-oai-snapshot.json -c abstract -r 1000 -a -f
```

### API
The package can be called using the following arguments:

| Argument | Type | Description |
| --- | --- | --- |
| -s, --standalone | bool | If set, the script will run as a standalone tool. |
| -i, --input <input_file> | str | The path to the input file. |
| -c, --text_choice <JSON_key> | str | Selection of key in JSON file that contains the text to be analyzed. |
| -r, --row <nr_rows> | int | Number of rows to be analyzed. Can be quite helpful for testing purposes. |
| -m, --manual_files | bool | Use manual file locations for the pipeline. |
| -f, --force | bool | Force rerun of the pipeline even if there is an existing output folder. |
| -a, --analyze | bool | Analyze the data. |

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.