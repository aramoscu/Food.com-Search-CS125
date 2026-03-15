from flask import Flask, render_template, request, redirect, session, jsonify
from Inverted_index.search_handler import *
from User_Data.user_methods import *
from logic.ranking import time_of_meal
from datetime import datetime
import sqlite3

app = Flask(__name__)
app.secret_key = 'food-com-search-results'
DB_PATH = "Data/inverted_index.db"
USER_PATH = "User_Data/user.db"

@app.route('/login', methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        user = request.form.get("username")
        password = request.form.get("password")
        user_id = check_user_login(user, password)
        print(f"User {user} is trying to log in")
        if user_id:
            session["user_id"] = user_id
            return redirect('/search')
        else:
            error = f"Password is incorrect for {user}. Please try again with correct password or different username."
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    session.clear()
    return redirect('/login')

@app.route('/liked_recipes')
def open_liked():
    if 'user_id' not in session:
        return redirect('/login')
    user_id = session.get('user_id')
    conn = sqlite3.connect(USER_PATH)
    cursor = conn.cursor()
    
    cursor.execute(f"ATTACH DATABASE '{DB_PATH}' AS recipe_db")
    query = \
    """
        SELECT r.id, r.name, r.minutes, r.protein, r.calories, r.sugar, r.sodium, r.avg_rating, r.review_count
        FROM recipe_db.recipes r
        JOIN user_likes l ON r.id = l.recipe_id
        WHERE l.user_id = ?
    """
    cursor.execute(query, (user_id,))
    liked_list = cursor.fetchall()
    conn.close()

    return render_template('liked_recipes.html', recipes=liked_list)

@app.route('/search')
def search_page():
    current_time = datetime.now().time()
    current_meal_supported = f"It's time for {time_of_meal(current_time).capitalize()}"
    if 'user_id' not in session:
        return redirect('/login')
    query = request.args.get('q', '').strip()
    last_min_prot = request.args.get('min_protein')
    last_max_calories = request.args.get('max_calories')
    last_max_sugar = request.args.get('max_sugar')
    last_max_sodium = request.args.get('max_sodium')
    # pref = request.args.get('preference')
    results = []
    user_id = session.get("user_id")
    if query:
        results = get_candidate_rows_for_user(
            query=query,
            user_id=user_id,
            min_protein=float(last_min_prot) if last_min_prot else None,
            max_calories=float(last_max_calories) if last_max_calories else None,
            max_sugar=float(last_max_sugar) if last_max_sugar else None,
            max_sodium=float(last_max_sodium) if last_max_sodium else None
        )
    else:
        # This would be recipes that are returned with no query
        results = get_personalized_recommendations(user_id=user_id,
                                    min_protein=float(last_min_prot) if last_min_prot else None,
                                    max_calories=float(last_max_calories) if last_max_calories else None,
                                    max_sugar=float(last_max_sugar) if last_max_sugar else None,
                                    max_sodium=float(last_max_sodium) if last_max_sodium else None)
    liked_recipe_ids = []
    conn = sqlite3.connect("User_Data/user.db")
    cursor = conn.cursor()
    if user_id:
        cursor.execute("""
        SELECT recipe_id
        FROM user_likes
        WHERE user_id = ?
        """, (user_id,))
        rows = cursor.fetchall()
        liked_recipe_ids = [row[0] for row in rows]
    return render_template('results.html', recipes=results,
                           last_query=query, last_protein=last_min_prot,
                           last_calories=last_max_calories, last_sugar=last_max_sugar,
                           last_sodium=last_max_sodium, liked_recipe_ids=liked_recipe_ids, promoted=current_meal_supported)

@app.route('/recipe/<int:recipe_id>')
def recipe_detail(recipe_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name, description, minutes, calories, protein, sugar, sodium, avg_rating
        FROM recipes
        WHERE id = ?
    """, (recipe_id,))
    recipe = cursor.fetchone()
    
    cursor.execute("""
        SELECT ingredient_name
        FROM recipe_ingredients
        WHERE recipe_id = ?
    """, (recipe_id,))
    ingredients = [row[0] for row in cursor.fetchall()]

    cursor.execute("""
        SELECT step_text
        FROM recipe_steps
        WHERE recipe_id = ?
        ORDER BY step_order
    """, (recipe_id,))
    steps = [row[0] for row in cursor.fetchall()]
    conn.close()

    user_id = session.get("user_id")

    conn = sqlite3.connect(USER_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 from user_likes
        WHERE user_id = ? AND recipe_id = ?
    """, (user_id, recipe_id))
    user_has_liked = cursor.fetchone() is not None
    conn.close()

    if not user_id:
        redirect("/login")
    if not recipe:
        return "Recipe not found", 404
    
    add_user_interaction(user_id, recipe_id)
    return render_template('detail.html', recipe_id=recipe_id, recipe=recipe,
                           ingredients=ingredients, steps=steps, user_has_liked=user_has_liked)

@app.route('/like/<int:recipe_id>', methods=["POST"])
def like_recipe(recipe_id):
    user_id = session.get("user_id")
    if not user_id:
        return redirect('/login')
    status = add_user_like(user_id, recipe_id)
    return jsonify({"status": status})


if __name__ == "__main__":
    app.run(debug=True)