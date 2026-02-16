import sqlite3
from typing import List, Dict, Any

DB_PATH = "Data/recipe_library.db"


def get_connection():
    """Create and return a database connection."""
    return sqlite3.connect(DB_PATH)


def get_recipe_by_id(recipe_id: int) -> Dict[str, Any]:
    """Fetch a single recipe by ID."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM recipe_library WHERE id = ?", (recipe_id,))
    row = cursor.fetchone()

    if row is None:
        conn.close()
        return {}

    # Convert row to dictionary
    columns = [description[0] for description in cursor.description]
    recipe = dict(zip(columns, row))

    conn.close()
    return recipe


def get_recipes_by_ids(recipe_ids: List[int]) -> List[Dict[str, Any]]:
    """Fetch multiple recipes by a list of IDs."""
    if not recipe_ids:
        return []

    conn = get_connection()
    cursor = conn.cursor()

    placeholders = ",".join("?" for _ in recipe_ids)
    query = f"SELECT * FROM recipe_library WHERE id IN ({placeholders})"

    cursor.execute(query, recipe_ids)
    rows = cursor.fetchall()

    columns = [description[0] for description in cursor.description]
    recipes = [dict(zip(columns, row)) for row in rows]

    conn.close()
    return recipes


def search_recipe_title(keyword: str) -> List[Dict[str, Any]]:
    """Search recipes by title using LIKE."""
    conn = get_connection()
    cursor = conn.cursor()

    query = "SELECT * FROM recipe_library WHERE name LIKE ?"
    cursor.execute(query, (f"%{keyword}%",))

    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    recipes = [dict(zip(columns, row)) for row in rows]

    conn.close()
    return recipes
