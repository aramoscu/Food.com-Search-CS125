import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report

def add_meal_type_classification():
    data = pd.read_csv("Raw_recipes.csv")
    lunch_keywords = 'lunch|sandwich|wrap|salad|burger|pizza|soup|bistro'
    is_breakfast = data['tags'].str.contains('breakfast', na=False)
    is_lunch = (
        ~is_breakfast & 
        (
            data['tags'].str.contains(lunch_keywords, na=False, case=False)
        )
    )
    is_dinner = ~is_breakfast & ~is_lunch & data['tags'].str.contains('dinner', na=False)

    labeled_df = data[is_breakfast | is_lunch | is_dinner].copy()
    labeled_df["class"] = "unlabeled"
    labeled_df.loc[is_breakfast, "class"] = "breakfast"
    labeled_df.loc[is_lunch, "class"] = "lunch"
    labeled_df.loc[is_dinner, "class"] = "dinner"

    unlabeled_df = data[~(is_breakfast | is_lunch | is_dinner)].copy()

    labeled_df["name"] = labeled_df["name"].fillna("")
    labeled_df["ingredients"] = labeled_df["ingredients"].fillna("")
    labeled_df["steps"] = labeled_df["steps"].fillna("")
    unlabeled_df["name"] = unlabeled_df["name"].fillna("")
    unlabeled_df["ingredients"] = unlabeled_df["ingredients"].fillna("")
    unlabeled_df["steps"] = unlabeled_df["steps"].fillna("")

    labeled_df["combined_text"] = labeled_df["name"] + " " + labeled_df["ingredients"] + " " + labeled_df["steps"]
    unlabeled_df["combined_text"] = unlabeled_df["name"] + " " + unlabeled_df["ingredients"] + " " + unlabeled_df["steps"]

    pipeline = Pipeline([
        ('tfidf', TfidfVectorizer(stop_words="english", max_features=5000)),
        ('rf', RandomForestClassifier(n_estimators=100, n_jobs=-1, random_state=42)),
    ])

    X = labeled_df["combined_text"]
    y = labeled_df["class"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    print("\nModel Performance:")
    print(classification_report(y_test, y_pred))

    unlabeled_df["class"] = pipeline.predict(unlabeled_df["combined_text"])
    final_df = pd.concat([labeled_df, unlabeled_df])
    final_df = final_df.drop(columns=["combined_text"])
    final_df.to_csv("Raw_recipes_meal_type.csv", index=False)
    return "Raw_recipes_meal_type.csv"
