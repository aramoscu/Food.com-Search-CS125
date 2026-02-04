from IngredientIndexer import IngredientIndexer
import os

def main():
    os.makedirs("Data", exist_ok=True)
    indexer = IngredientIndexer("Raw_recipes.csv") # csv file name depends on where its stored
    try:
        indexer.build()
    except MemoryError as e:
        print(f"MemoryError caught: {e}")

if __name__ == "__main__":
    main()
