# Read keywords helper functions

def read_n_grams(file_name):
    with open(file_name, "r") as f:
        n_grams = f.read()
        n_grams = n_grams.split()
        n_grams = [n_grams[i] + " " + n_grams[i+1] for i in range(0, len(n_grams), 2)]
    return n_grams

# Read unigrams from one line in a txt file and split them after each word. Put them into a list
def read_unigrams(file_name):
    with open(file_name, "r") as f:
        unigrams = f.read()
        unigrams = unigrams.split()
    return unigrams