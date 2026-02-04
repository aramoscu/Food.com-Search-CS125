from IngredientIndexer import IngredientIndexer

def main():
    indexer = IngredientIndexer("Raw_recipes.csv")
    try:
        indexer.build()
    except MemoryError as e:
        print(f"MemoryError caught: {e}")

if __name__ == "__main__":
    main()
