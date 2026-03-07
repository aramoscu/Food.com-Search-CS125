from flask import Flask, render_template, request, redirect
from Inverted_index.search_handler import search
import sqlite3

app = Flask(__name__)
DB_PATH = "Data/inverted_index.db"

@app.route('/login', methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = request.form.get("username")
        print(f"User {user} is trying to log in")
        return redirect('/search')
    return render_template('login.html')

@app.route('/search')
def search_page():
    query = request.args.get('q', '').strip()
    last_min_prot = request.args.get('min_protein')
    last_max_calories = request.args.get('max_calories')
    last_max_sugar = request.args.get('max_sugar')
    last_max_sodium = request.args.get('max_sodium')
    pref = request.args.get('preference')
    results = []
    if query:
        results = search(
            query=query,
            min_protein=float(last_min_prot) if last_min_prot else None,
            max_calories=float(last_max_calories) if last_max_calories else None,
            max_sugar=float(last_max_sugar) if last_max_sugar else None,
            max_sodium=float(last_max_sodium) if last_max_sodium else None,
            user_preference=pref if pref else None
        )
    return render_template('results.html', recipes=results,
                           last_query=query, last_protein=last_min_prot,
                           last_calories=last_max_calories, last_sugar=last_max_sugar,
                           last_sodium=last_max_sodium, last_pref=pref)

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

    if not recipe:
        return "Recipe not found", 404
    
    return render_template('detail.html', recipe=recipe,
                           ingredients=ingredients, steps=steps)

if __name__ == "__main__":
    app.run(debug=True)