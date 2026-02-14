import os
import re
import ast
import sqlite3
import pandas as pd

DB_PATH = "Data/inverted_index.db"
CSV_PATH = "Raw_recipes.csv"   # adjust if needed
CHUNK_SIZE = 20_000

TOKEN_RE = re.compile(r"[a-z0-9]+")

def tokenize(text: str) -> list[str]:
    if not isinstance(text, str):
        return []
    return TOKEN_RE.findall(text.lower())

def parse_nutrition(nutrition_str: str):
    """
    raw_recipes.csv nutrition format (Food.com):
      [calories, total_fat, sugar, sodium, protein, saturated_fat, carbohydrates]
    Returns 7 floats or None if invalid.
    """
    if not isinstance(nutrition_str, str):
        return None
    try:
        arr = ast.literal_eval(nutrition_str)
        if not (isinstance(arr, (list, tuple)) and len(arr) == 7):
            return None
        vals = [float(x) for x in arr]
        return tuple(vals)
    except Exception:
        return None

def is_sane_nutrition(vals) -> bool:
    """
    Optional sanity bounds to prevent obviously broken values.
    Adjust if you want.
    """
    cal, fat, sugar, sodium, protein, sat_fat, carbs = vals

    # reject negatives
    if any(v < 0 for v in vals):
        return False

    # very loose bounds (keeps most real recipes)
    if cal > 2000:        #Limit for protein in a recipe
        return False
    if protein > 140:       #Limit for protein in a recipe
        return False
    if sodium > 5000:    #Limit for sodium in a recipe
        return False

    return True

def build():
    os.makedirs("Data", exist_ok=True)

    # Rebuild safely (no duplicates)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Only concrete columns — NO raw nutrition list-string stored
    cur.execute("""
        CREATE TABLE recipes (
            id INTEGER PRIMARY KEY,
            name TEXT,
            ingredients TEXT,
            calories REAL,
            total_fat REAL,
            sugar REAL,
            sodium REAL,
            protein REAL,
            saturated_fat REAL,
            carbohydrates REAL
        );
    """)

    cur.execute("""
        CREATE TABLE postings (
            term TEXT NOT NULL,
            docid INTEGER NOT NULL
        );
    """)
    cur.execute("CREATE INDEX idx_postings_term ON postings(term);")
    cur.execute("CREATE INDEX idx_postings_term_doc ON postings(term, docid);")

    # Optional numeric indexes for faster filters
    cur.execute("CREATE INDEX idx_recipes_protein ON recipes(protein);")
    cur.execute("CREATE INDEX idx_recipes_calories ON recipes(calories);")

    # Read only the columns you need.
    # NOTE: "nutrition" is read from CSV but NOT stored raw — only parsed values stored.
    usecols = ["id", "name", "ingredients", "nutrition"]

    total_recipes = 0
    total_postings = 0

    for chunk_i, chunk in enumerate(pd.read_csv(CSV_PATH, usecols=usecols, chunksize=CHUNK_SIZE)):
        chunk = chunk.dropna(subset=["id", "ingredients", "nutrition"])
        chunk["id"] = chunk["id"].astype(int)
        chunk["name"] = chunk["name"].fillna("")

        # Parse nutrition -> 7 numeric cols
        parsed = chunk["nutrition"].apply(parse_nutrition)
        chunk = chunk.loc[parsed.notna()].copy()
        parsed = parsed.loc[parsed.notna()]

        # Apply sanity check (optional but helps prevent “abstract/weird” values)
        sane_mask = parsed.apply(is_sane_nutrition)
        chunk = chunk.loc[sane_mask].copy()
        parsed = parsed.loc[sane_mask]

        # Expand into columns
        vals = list(parsed)
        chunk["calories"] = [v[0] for v in vals]
        chunk["total_fat"] = [v[1] for v in vals]
        chunk["sugar"] = [v[2] for v in vals]
        chunk["sodium"] = [v[3] for v in vals]
        chunk["protein"] = [v[4] for v in vals]
        chunk["saturated_fat"] = [v[5] for v in vals]
        chunk["carbohydrates"] = [v[6] for v in vals]

        # Insert recipes
        recipe_rows = list(zip(
            chunk["id"].tolist(),
            chunk["name"].tolist(),
            chunk["ingredients"].tolist(),
            chunk["calories"].tolist(),
            chunk["total_fat"].tolist(),
            chunk["sugar"].tolist(),
            chunk["sodium"].tolist(),
            chunk["protein"].tolist(),
            chunk["saturated_fat"].tolist(),
            chunk["carbohydrates"].tolist(),
        ))

        cur.executemany("""
            INSERT INTO recipes(
                id, name, ingredients,
                calories, total_fat, sugar, sodium, protein, saturated_fat, carbohydrates
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, recipe_rows)

        # Build postings from name + ingredients
        postings_batch = []
        for rid, name, ing in zip(chunk["id"], chunk["name"], chunk["ingredients"]):
            text = f"{name} {ing}"
            for term in set(tokenize(text)):
                postings_batch.append((term, int(rid)))

        cur.executemany("INSERT INTO postings(term, docid) VALUES (?, ?)", postings_batch)

        conn.commit()
        total_recipes += len(chunk)
        total_postings += len(postings_batch)

        print(f"Chunk {chunk_i}: recipes={len(chunk):,} postings={len(postings_batch):,}")

    conn.close()
    print("\n Done!")
    print(f"DB: {DB_PATH}")
    print(f"Total recipes inserted: {total_recipes:,}")
    print(f"Total postings inserted: {total_postings:,}")

if __name__ == "__main__":
    build()
