# PubGraph
A Knowledge Graph Generator for Scientific Publications

### Overview
PubGraph is a tool for generating a knowledge graph from scientific publications. It is designed to be used with the [PubCrawl](https://github.com/J0nasW/PubCrawl) but can also be used independently. 

## Installation
To start, clone the repository and install the package using the requirements file:

```bash
git clone https://github.com/J0nasW/PubGraph.git
cd pubgraph

pip install -r requirements.txt
```

## Usage
PubGraph can be used in two ways: either as a standalone tool or as a module for PubCrawl.

### Standalone
To use PubGraph as a standalone tool, simply run the `pubgraph.py` script with the following arguments:

```bash
python pubgraph.py -s -i <input_file> -o <output_dir> 
```

The input file should be a JSON file containing the publications to be processed. It is assumed, that you denote abstracts with `abstracts` and fulltexts with `text` inside your input JSON file. The output file is the path to the output file (can be left empty to write to the current directory).