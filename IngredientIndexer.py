from collections import defaultdict, OrderedDict
from Posting import Posting
import pandas as pd
import pickle
import struct
import shelve

class IngredientIndexer:
    def __init__(self, ingredients_file):
        self.ingredients_file = ingredients_file
        self.chunk_size = 50000 #size of ingredients is 230,000
        self.index_recipe_library = {} # {index_id (str): recipe_id (int)} # .db can only map strings to int
        self.recipe_index_id = 0
        self.file_indexes = []
    
    def write_data(self, file, term, postings):
        term_in_bytes = term.encode("utf-8")
        postings_in_bytes = pickle.dumps(postings)
        header = struct.pack("II", len(term_in_bytes), len(postings_in_bytes))
        file.write(header)
        file.write(term_in_bytes)
        file.write(postings_in_bytes)
        return len(header) + len(term_in_bytes) + len(postings_in_bytes)
    
    def build(self):
        recipes = pd.read_csv("Raw_recipes.csv", chunksize=self.chunk_size)
        num_chunks = 0
        for recipe_chunk in recipes:
            index = {} # {ingredient (string): postings List<Posting>} # Posting(recipeid)
            ingredients = recipe_chunk[["id", "ingredients"]]
            for row in ingredients.itertuples(): # iterate through each recipe chunk
                # each row is [index, recipeid, ingredients]
                self.recipe_index_id += 1
                self.index_recipe_library[str(self.recipe_index_id)] = row[1]
                for ingredient in ingredients:
                    if ingredient not in index:
                        index[ingredient] = []
                    if type(ingredient) != str:
                        print(ingredient)
                    index[ingredient].append(Posting(self.recipe_index_id))
            sorted_index = OrderedDict(sorted(index.items()))
            filename = f"sorted_recipe_index_{num_chunks}.bin"
            self.file_indexes.append(filename)
            print(type(index))
            with open(filename, "wb") as file:
                for term, postings_list in index.items():
                    self.write_data(file, term, postings_list)
            num_chunks += 1
        print(len(self.index_recipe_library))
        with shelve.open("index_recipe_libary.db") as index_library:
            index_library.update(self.index_recipe_library)

if __name__ == "__main__":
    ingredient_indexer = IngredientIndexer("Raw_recipes.csv")
    ingredient_indexer.build()