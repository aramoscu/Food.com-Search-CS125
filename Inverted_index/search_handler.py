import sqlite3
from logic.ranking import calculate_score_for_recipes, get_user_profile

DB_PATH = "Data/inverted_index.db"

def build_nutrition_filter(min_protein, max_calories, max_sugar, max_sodium):
    where_clauses = []
    params = []
    if min_protein is not None:
        where_clauses.append("r.protein >= ?")
        params.append(min_protein)
    if max_calories is not None:
        where_clauses.append("r.calories <= ?")
        params.append(max_calories)
    if max_sugar is not None:
        where_clauses.append("r.sugar <= ?")
        params.append(max_sugar)
    if max_sodium is not None:
        where_clauses.append("r.sodium <= ?")
        params.append(max_sodium)
    filter_sql = (" WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    print(filter_sql)
    return filter_sql, params

def get_candidate_rows_for_user(query: str, user_id: int,
           limit: int = 10,
           min_protein: float = None,
           max_calories: float = None,
           max_sugar: float = None,
           max_sodium: float = None):
    terms = [t.strip().lower() for t in query.split(",") if t.strip()]
    if not terms:
        return []
    filter_sql, filter_params = build_nutrition_filter(min_protein, max_calories, max_sugar, max_sodium)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    placeholders = ",".join("?" for _ in terms)

    base = f"""
        SELECT DISTINCT p.docid
        FROM postings p
        WHERE p.term IN ({placeholders})
    """

    sql = f"""
        SELECT r.id, r.name, r.minutes, r.protein, r.calories, r.sugar, r.sodium, r.avg_rating, r.review_count, r.meal_type
        FROM ({base}) m
        JOIN recipes r ON r.id = m.docid
        {filter_sql}
    """
    cursor.execute(sql, terms + filter_params)
    rows = cursor.fetchall() # These are candidate recipes that contain 1 or more of the requested ingredients
    conn.close()
    rows = calculate_score_for_recipes(rows, user_id, terms)
    return rows[:limit]

def get_personalized_recommendations(user_id: int,
           min_protein: float = None,
           max_calories: float = None,
           max_sugar: float = None,
           max_sodium: float = None,
           limit: int = 10):
    user_profile = get_user_profile(user_id)
    tags = list(user_profile["liked_tags"])
    filter_sql, filter_params = build_nutrition_filter(min_protein, max_calories, max_sugar, max_sodium)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    if not tags:
        sql = f"""
        SELECT r.id, r.name, r.minutes, r.protein, r.calories, r.sugar, r.sodium, r.avg_rating, r.review_count, r.meal_type
        FROM recipes r
        {filter_sql}
        ORDER BY r.avg_rating DESC, r.review_count DESC
        LIMIT ?
        """
        cursor.execute(sql, filter_params + [limit])
    else:
        placeholders = ",".join("?" for _ in tags)
        tag_clause = (" AND " if filter_sql else " WHERE ") + f"t.tag IN ({placeholders})"
        sql = f"""
            SELECT DISTINCT r.id, r.name, r.minutes, r.protein, r.calories, r.sugar, r.sodium, r.avg_rating, r.review_count, r.meal_type
            FROM recipe_tags t
            JOIN recipes r ON r.id = t.recipe_id
            {filter_sql}
            {tag_clause}
        """
        cursor.execute(sql, filter_params + tags)
    rows = cursor.fetchall()
    conn.close()

    scored_results = calculate_score_for_recipes(rows, user_id, search_ingredients=None)
    return scored_results[:limit]
