# Searches the inverted index and applies nutrition filters.
#
# Usage examples:
#   python Inverted_index/search_handler.py

import sqlite3

DB_PATH = "Data/inverted_index.db"

def search(
    query: str,
    limit: int = 10,
    # nutrition filters (all optional)
    min_protein: float | None = None,
    max_calories: float | None = None,
    max_sugar: float | None = None,
    max_sodium: float | None = None,
):
    """
    AND-style query over tokens in `query`:
      query="chicken garlic" means docs containing BOTH tokens.
    Nutrition filters are applied after matching docids.
    Returns rows: (id, name, protein, calories, sugar, sodium)
    """
    terms = [t.strip().lower() for t in query.split() if t.strip()]
    if not terms:
        return []

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    placeholders = ",".join("?" for _ in terms)

    # 1) Candidate docids: postings intersection using GROUP BY/HAVING
    base = f"""
        SELECT p.docid
        FROM postings p
        WHERE p.term IN ({placeholders})
        GROUP BY p.docid
        HAVING COUNT(DISTINCT p.term) = ?
    """
    params: list[object] = [*terms, len(terms)]

    # 2) Join with recipes and apply numeric filters
    where_clauses = []
    if min_protein is not None:
        where_clauses.append("r.protein >= ?")
        params.append(float(min_protein))
    if max_calories is not None:
        where_clauses.append("r.calories <= ?")
        params.append(float(max_calories))
    if max_sugar is not None:
        where_clauses.append("r.sugar <= ?")
        params.append(float(max_sugar))
    if max_sodium is not None:
        where_clauses.append("r.sodium <= ?")
        params.append(float(max_sodium))

    filter_sql = ""
    if where_clauses:
        filter_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT r.id, r.name, r.protein, r.calories, r.sugar, r.sodium
        FROM ({base}) m
        JOIN recipes r ON r.id = m.docid
        {filter_sql}
        LIMIT ?
    """
    params.append(int(limit))

    cur.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows

def pretty_print(rows):
    if not rows:
        print("No results.")
        return
    for rid, name, protein, calories, sugar, sodium in rows:
        print(f"{rid} | {protein:.1f}g protein | {calories:.0f} cal | sugar {sugar:.1f} | sodium {sodium:.1f} | {name}")

if __name__ == "__main__":
    # Example 1: basic search
    rows = search("chicken garlic", limit=10)
    print("\n--- chicken garlic (no nutrition filter) ---")
    pretty_print(rows)

    # Example 2: high-protein filter
    rows = search("chicken garlic", min_protein=25, limit=10)
    print("\n--- chicken garlic + protein>=25 ---")
    pretty_print(rows)

    # Example 3: high-protein + calorie cap
    rows = search("chicken", min_protein=25, max_calories=600, limit=10)
    print("\n--- chicken + protein>=25 + calories<=600 ---")
    pretty_print(rows)
