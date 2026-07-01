import pandas as pd
import psycopg2

# CONNECT TO DB
conn = psycopg2.connect(
     user="airflow",
     password="airflow",
     host="localhost",
     port="5433",
     database="ecommerce_platform"
)

cursor = conn.cursor()

# LOAD CSVs
customers = pd.read_csv("data/raw/customers.csv")
products = pd.read_csv("data/raw/products.csv")

print("Customers:", len(customers))
print("Products:", len(products))

# ------------------------
# INSERT CUSTOMERS
# ------------------------
for _, row in customers.iterrows():
    cursor.execute("""
        INSERT INTO customers (customer_id, name, country, segment)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING;
    """, (
        row.customer_id,
        row.name,
        row.country,
        row.segment
    ))

# ------------------------
# INSERT PRODUCTS
# ------------------------
for _, row in products.iterrows():
    cursor.execute("""
        INSERT INTO products (product_id, title, category, price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING;
    """, (
        row.product_id,
        row.title,
        row.category,
        row.price
    ))

conn.commit()
conn.close()

print("DATA LOADED SUCCESSFULLY")