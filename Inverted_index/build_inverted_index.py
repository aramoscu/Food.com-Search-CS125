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
            description TEXT,
            minutes INTEGER,
            calories REAL,
            total_fat REAL,
            sugar REAL,
            sodium REAL,
            protein REAL,
            saturated_fat REAL,
            carbohydrates REAL,
            avg_rating REAL
        );
    """)
    
    cur.execute("""
        CREATE TABLE recipe_ingredients (
            recipe_id INTEGER,
            ingredient_name TEXT,
            FOREIGN KEY(recipe_id) references recipes(id)
        );
    """)

    cur.execute("""
        CREATE TABLE recipe_steps (
            recipe_id INTEGER,
            step_order INTEGER,
            step_text TEXT,
            FOREIGN KEY(recipe_id) references recipes(id)
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
    cur.execute("CREATE INDEX idx_ingredients_rid ON recipe_ingredients(recipe_id);")
    cur.execute("CREATE INDEX idx_steps_id ON recipe_steps(recipe_id);")

    # Optional numeric indexes for faster filters
    cur.execute("CREATE INDEX idx_recipes_protein ON recipes(protein);")
    cur.execute("CREATE INDEX idx_recipes_calories ON recipes(calories);")
    cur.execute("CREATE INDEX idx_recipes_rating ON recipes(avg_rating);")

    # Read only the columns you need.
    # NOTE: "nutrition" is read from CSV but NOT stored raw — only parsed values stored.
    usecols = ["id", "name", "ingredients", "minutes", "nutrition", "steps", "description"]

    total_recipes = 0
    total_postings = 0

    for chunk_i, chunk in enumerate(pd.read_csv(CSV_PATH, usecols=usecols, chunksize=CHUNK_SIZE)):
        chunk = chunk.dropna(subset=["id", "ingredients", "nutrition"])
        chunk["id"] = chunk["id"].astype(int)
        chunk["name"] = chunk["name"].fillna("")
        chunk["minutes"] = chunk["minutes"].fillna(0).astype(int)
        chunk["description"] = chunk["description"].fillna("No description available.")

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

        recipe_batch = []
        ing_batch = []
        step_batch = []
        postings_batch = []

        for i in range(len(chunk)):
            row = chunk.iloc[i]
            nutrition = parsed.iloc[i]
            rid = int(row["id"])

            recipe_batch.append((
                rid, row["name"], row["description"], int(row["minutes"]),
                nutrition[0], nutrition[1], nutrition[2], nutrition[3], nutrition[4],
                nutrition[5], nutrition[6], None
            ))

            try:
                raw_ings = ast.literal_eval(row["ingredients"])
                for ing_name in raw_ings:
                    ing_batch.append((rid, ing_name))
                    clean_ing = re.sub(r'\s+', ' ', ing_name.lower().strip())
                    postings_batch.append((clean_ing, rid))
            except:
                pass

            try:
                raw_steps = ast.literal_eval(row["steps"])
                for idx, step_text in enumerate(raw_steps):
                    step_batch.append((rid, idx, step_text))
            except:
                pass

        cur.executemany("INSERT INTO recipes VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", recipe_batch)
        cur.executemany("INSERT INTO recipe_ingredients VALUES (?,?)", ing_batch)
        cur.executemany("INSERT INTO recipe_steps VALUES (?,?,?)", step_batch)
        cur.executemany("INSERT INTO postings VALUES (?,?)", postings_batch)

        conn.commit()
        total_recipes += len(chunk)
        total_postings += len(postings_batch)

        print(f"Chunk {chunk_i}: recipes={len(chunk):,} postings={len(postings_batch):,}")

    # Compute average ratings from RAW_interactions.csv
    INTERACTIONS_PATH = "RAW_interactions.csv"
    if os.path.exists(INTERACTIONS_PATH):
        interactions_df = pd.read_csv(INTERACTIONS_PATH, usecols=["recipe_id", "rating"])
        # Filter out 0 ratings (often means unrated)
        interactions_df = interactions_df[interactions_df["rating"] > 0]
        # Group by recipe_id and compute mean rating
        avg_ratings = interactions_df.groupby("recipe_id")["rating"].mean().round(2).reset_index()
        avg_ratings.columns = ["recipe_id", "avg_rating"]

        # Update recipes table with ratings
        rating_updates = [(float(row["avg_rating"]), int(row["recipe_id"]))
                         for _, row in avg_ratings.iterrows()]
        cur.executemany("UPDATE recipes SET avg_rating = ? WHERE id = ?", rating_updates)
        conn.commit()
        print(f"Updated ratings for {len(rating_updates):,} recipes")
    else:
        print(f"Warning: {INTERACTIONS_PATH} not found, skipping ratings")

    conn.close()
    print("\n Done!")
    print(f"DB: {DB_PATH}")
    print(f"Total recipes inserted: {total_recipes:,}")
    print(f"Total postings inserted: {total_postings:,}")

if __name__ == "__main__":
    build()
