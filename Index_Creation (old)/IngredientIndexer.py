from collections import OrderedDict
from Posting import Posting
from IndexFileBuffer import IndexFileBuffer
import pandas as pd
import pickle
import struct
import shelve
import sqlite3
import os

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
        recipes = pd.read_csv(self.ingredients_file, chunksize=self.chunk_size)
        num_chunks = 0
        conn = sqlite3.connect("Data/recipe_library.db")
        for i, recipe_chunk in enumerate(recipes):
            mode = "replace" if i == 0 else "append"
            recipe_chunk.to_sql("recipe_library", conn, if_exists=mode, index=False)
            index = {} # {ingredient (string): postings List<Posting>} # Posting(recipeid)
            ingredients = recipe_chunk[["id", "ingredients"]]
            for row in ingredients.itertuples(): # iterate through each recipe chunk
                # each row is [index, recipeid, ingredients]
                self.recipe_index_id += 1
                self.index_recipe_library[str(self.recipe_index_id)] = row[1]
                for ingredient in ingredients:
                    if ingredient not in index:
                        index[ingredient] = []
                    index[ingredient].append(Posting(self.recipe_index_id))
            sorted_index = OrderedDict(sorted(index.items()))
            filename = f"Data/sorted_recipe_index_{num_chunks}.bin"
            self.file_indexes.append(filename)
            with open(filename, "wb") as file:
                for term, postings_list in index.items():
                    self.write_data(file, term, postings_list)
            num_chunks += 1
        with shelve.open("Data/index_recipe_library.db") as index_library:
            index_library.update(self.index_recipe_library)
        # merge indexes
        while len(self.file_indexes) > 1:
            first_file = self.file_indexes.pop()
            second_file = self.file_indexes.pop()
            if len(self.file_indexes) == 2:
                merged_file = self.merge_files(first_file, second_file, True)
            else:
                merged_file = self.merge_files(first_file, second_file)
            self.file_indexes.append(merged_file)
        if self.file_indexes:
            os.rename(self.file_indexes[0], "Data/ingredient_index.bin")
        conn.close()
    
    def merge_files(self, first_file, second_file, final_merge=False):
        index1 = IndexFileBuffer(first_file, 10^7)
        index2 = IndexFileBuffer(second_file, 10^7)
        merged_filename =f"Data/merged_index_temp_{len(self.file_indexes)}.bin"
        term_to_buffer = {}
        located = 0
        with open(merged_filename, "wb") as merged_file:
            index1_record = index1.next()
            index2_record = index2.next()
            while index1_record is not None and index2_record is not None:
                index1_term, index1_postings = index1_record
                index2_term, index2_postings = index2_record
                if index1_term < index2_term:
                    term_to_buffer[index1_term] = located
                    size_written = self.write_data(merged_file, index1_term, index1_postings)
                    located += size_written
                    index1_record = index1.next()
                elif index1_term > index2_term:
                    term_to_buffer[index2_term] = located
                    size_written = self.write_data(merged_file, index2_term, index2_postings)
                    located += size_written
                    index2_record = index2.next()
                else:
                    sorted_postings = self.sort_postings(index1_postings, index2_postings)
                    term_to_buffer[index1_term] = located
                    size_written = self.write_data(merged_file, index1_term, sorted_postings)
                    located += size_written
                    index1_record = index1.next()
                    index2_record = index2.next()
            if index2_record is not None:
                while index2_record:
                    index2_term, index2_postings = index2_record
                    term_to_buffer[index2_term] = located
                    size_written = self.write_data(merged_file, index2_term, index2_postings)
                    located += size_written
                    index2_record = index2.next()
            elif index1_record is not None:
                while index1_record:
                    index1_term, index1_postings = index1_record
                    term_to_buffer[index1_term] = located
                    size_written = self.write_data(merged_file, index1_term, index1_postings)
                    located += size_written
                    index1_record = index1.next()
        with shelve.open("Data/term_to_buffer.db") as buffer_library:
            buffer_library.update(term_to_buffer)
        index1.close()
        index2.close()
        # clean trash files
        try:
            base_name_1 = first_file.rsplit('/', 1)[-1]
            base_name_2 = second_file.rsplit('/', 1)[-1]
            for f in os.listdir('Data'):
                if f.startswith(base_name_1) or f.startswith(base_name_2):
                    os.remove(f"Data/{f}")
        except OSError as e:
            print(f"Error deleting temporary index files {first_file} or {second_file}: {e}")
        return merged_filename

    
    def sort_postings(self, first_postings, second_postings):
        merged_postings = []
        left = 0
        right = 0
        first_postings_length = len(first_postings)
        second_postings_length = len(second_postings)
        while left < first_postings_length and right < second_postings_length:
            if first_postings[left].indexrecipeid > second_postings[right].indexrecipeid:
                merged_postings.append(first_postings[left])
                left += 1
            elif first_postings[left].indexrecipeid < second_postings[right].indexrecipeid:
                merged_postings.append(second_postings[right])
                right += 1
        if left == first_postings_length:
            merged_postings += second_postings[right:]
        else:
            merged_postings += first_postings[left:]
        return merged_postings
