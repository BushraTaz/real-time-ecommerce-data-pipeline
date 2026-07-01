import pandas as pd
import random
import os

# ALWAYS ensure folders exist
os.makedirs("data/raw", exist_ok=True)

customers = []

for i in range(1, 101):
    customers.append({
        "customer_id": i,
        "name": f"customer_{i}",
        "country": "Sweden",
        "segment": random.choice(["basic", "premium", "vip"])
    })

df = pd.DataFrame(customers)

# IMPORTANT PATH
df.to_csv("data/raw/customers.csv", index=False)

print("Customers CSV created")