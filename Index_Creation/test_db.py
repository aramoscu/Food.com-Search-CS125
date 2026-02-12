from db import get_recipe_by_id, search_recipe_title

# Test title search
results = search_recipe_title("chicken")
print(f"\nFound {len(results)} recipes with 'chicken' in name")

for r in results[:1]:
    #print("-", r.get("name"))
    print("-", r)
