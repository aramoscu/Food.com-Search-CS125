import sqlite3
from datetime import datetime
from collections import defaultdict

USER_DB = "User_Data/user.db"
RECIPE_DB = "Data/inverted_index.db"

def get_recipe_info_for_batch(recipe_ids: list):
    recipe_info = defaultdict(lambda: {"tags": set(), "ingredients": set()})
    if not recipe_ids:
        return {}
    conn = sqlite3.connect(RECIPE_DB)
    cursor = conn.cursor()

    max_recipe_ids = 900

    for i in range(0, len(recipe_ids), max_recipe_ids):
        chunk = recipe_ids[i:i+max_recipe_ids]
        placeholders = ', '.join(['?'] * len(chunk))
        tags_sql = f"""
        SELECT recipe_id, tag
        FROM recipe_tags
        WHERE recipe_id IN ({placeholders})
        """
        cursor.execute(tags_sql, chunk)
        tag_rows = cursor.fetchall()

        ingredients_sql = f"""
        SELECT recipe_id, ingredient_name
        FROM recipe_ingredients
        WHERE recipe_id IN ({placeholders})
        """
        cursor.execute(ingredients_sql, chunk)
        ingredient_rows = cursor.fetchall()

        for rid, tag in tag_rows:
            recipe_info[rid]["tags"].add(tag)

        for rid, i_name in ingredient_rows:
            recipe_info[rid]["ingredients"].add(i_name)
    conn.close()
    return dict(recipe_info)

def get_user_profile(user_id):
    user_profile = {
        "liked_tags": set(),
        "recent_clicks": {}
    }

    conn1 = sqlite3.connect(RECIPE_DB)
    cursor1 = conn1.cursor()
    conn2 = sqlite3.connect(USER_DB)
    cursor2 = conn2.cursor()

    # liked tags
    sql = """
    SELECT recipe_id
    FROM user_likes
    WHERE user_id = ?
    """
    cursor2.execute(sql, (user_id,))
    liked_ids = [r[0] for r in cursor2.fetchall()]
    if liked_ids:
        placeholders = ", ".join(["?"] * len(liked_ids))
        sql = f"""
        SELECT tag
        FROM recipe_tags
        WHERE recipe_id in ({placeholders})
        """
        cursor1.execute(sql, liked_ids)
        user_profile["liked_tags"] = {row[0] for row in cursor1.fetchall()}
    
    sql = """
    SELECT recipe_id, clicked_at
    FROM user_interaction
    WHERE user_id = ?
    """
    cursor2.execute(sql, (user_id,))
    click_data = cursor2.fetchall()
    if click_data:
        click_map = {}
        for rid, time_stamp in click_data:
            try:
                click_map[rid] = datetime.fromisoformat(time_stamp)
            except:
                click_map[rid] = time_stamp
        click_ids = list(click_map.keys())

        placeholders = ", ".join(["?"] * len(click_ids))
        sql = f"""
            SELECT recipe_id, tag
            FROM recipe_tags
            WHERE recipe_id in ({placeholders})
            """
        cursor1.execute(sql, click_ids)
        for rid, tag in cursor1.fetchall():
            if rid not in user_profile["recent_clicks"]:
                user_profile["recent_clicks"][rid] = {
                    "time": click_map[rid],
                    "tags": set()
                }
            user_profile["recent_clicks"][rid]["tags"].add(tag)

    conn1.close()
    conn2.close()
    return user_profile

def calculate_score_for_recipes(recipes: list, batch_info: dict, user_profile: dict, search_ingredients: list = None):
    recipe_ids = [recipe[0] for recipe in recipes]
    final_list = []
    for recipe in recipes:
        rid = recipe[0]
        info = batch_info.get(rid, {"tags": set(), "ingredients": set()})
        score = calculate_score(recipe, info["tags"], info["ingredients"], user_profile, search_ingredients)
        final_list.append((recipe, score))
    # sort final_list by score given and then return
    final_list.sort(key=lambda x: x[1], reverse=True)
    final_list = [item[0] for item in final_list]
    return final_list

def calculate_score(recipe: tuple, recipe_tags: set, recipe_ingredients: set, user_profile: dict, search_ingredients: list = None):
    score = 0
    current_time = datetime.now()

    if search_ingredients:
        # ingredient matching
        matches = recipe_ingredients & set(search_ingredients)
        score += (len(matches) ** 2) * 100
    
    current_meal_type = time_of_meal(current_time.time())
    if recipe[9] == current_meal_type:
        # time of day score
        score += 20
    
    # liked relevance score
    like_matches = len(recipe_tags & user_profile["liked_tags"])
    score += like_matches * 15

    # clicked relevance score
    for rid, click_data in user_profile["recent_clicks"].items():
        common_tags = len(recipe_tags & click_data["tags"])
        if isinstance(click_data["time"], datetime):
            hours_ago = (current_time - click_data["time"]).total_seconds() / 3600
            decay = 1 / (1 + hours_ago)
            score += (common_tags * 10) * decay

    # popularity score
    avg_rating = recipe[7] or 0
    rating_count = recipe[8] or 0
    score += (avg_rating * 5) + (rating_count / 50)
    return score

def time_of_meal(current_time):
    noon = "12:00:00"
    night = "17:00:00"
    format = "%H:%M:%S"
    dt_noon = datetime.strptime(noon, format).time()
    dt_night = datetime.strptime(night, format).time()
    current_meal_type = ""
    if current_time < dt_noon:
        current_meal_type = "breakfast"
    elif dt_noon <= current_time <= dt_night:
        current_meal_type = "lunch"
    else:
        current_meal_type = "dinner"
    return current_meal_type
