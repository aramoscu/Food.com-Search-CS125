import sqlite3

def check_user_login(username, password):
    conn = sqlite3.connect("User_Data/user.db")
    cursor = conn.cursor()

    create_table_sql = \
    """
    CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL
    );
    """
    cursor.execute(create_table_sql)
    
    check_user_sql = f"""
    SELECT *
    FROM users
    WHERE username = ?
    """

    create_user_sql = f"""
    INSERT INTO users (username, password) VALUES (?, ?) RETURNING id
    """
    cursor.execute(check_user_sql, (username,))
    row = cursor.fetchone()
    u_id = 0
    if row:
        # user found
        if row[2] == password:
            # password is correct
            u_id = row[0]
    else:
        # no user found
        new_user = (username, password)
        cursor.execute(create_user_sql, new_user)
        row = cursor.fetchone()
        conn.commit()
        conn.close()
        u_id = row[0]
    conn.close()
    return u_id

def add_user_interaction(user_id, recipe_id):
    conn = sqlite3.connect("User_Data/user.db")
    cursor = conn.cursor()

    create_table_sql = \
    """
    CREATE TABLE IF NOT EXISTS user_interaction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    recipe_id INTEGER,
    clicked_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(user_id) REFERENCES users(id)
    );
    """
    cursor.execute(create_table_sql)

    create_interaction_sql = f"""
    INSERT INTO user_interaction(user_id, recipe_id) VALUES (?,?)
    """
    interaction = (user_id, recipe_id)
    cursor.execute(create_interaction_sql, interaction)
    conn.commit()
    conn.close()
    return