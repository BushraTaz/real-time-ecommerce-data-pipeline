import requests
import pandas as pd

url = "https://fakestoreapi.com/products"
data = requests.get(url).json()

products = []

for p in data:
    products.append({
        "product_id": p["id"],
        "title": p["title"],
        "category": p["category"],
        "price": p["price"]
    })

df = pd.DataFrame(products)
df.to_csv("data/products.csv", index=False)

print("Products saved")