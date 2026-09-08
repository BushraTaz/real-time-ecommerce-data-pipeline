import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    database="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)

cursor = conn.cursor()

# -------------------------
# AGGREGATION QUERY
# -------------------------
query = """
SELECT
    p.product_name,
    p.category,
    COUNT(f.id) AS total_events
FROM fact_events f
JOIN dim_products p
ON f.product_id = p.product_id
GROUP BY p.product_name, p.category
"""

cursor.execute(query)
rows = cursor.fetchall()

# -------------------------
# LOAD INTO GOLD TABLE
# -------------------------
for row in rows:
    product_name, category, total_events = row

    cursor.execute("""
        INSERT INTO fact_product_analytics
        (product_name, category, total_events, last_updated)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_name)
        DO UPDATE SET
            category = EXCLUDED.category,
            total_events = EXCLUDED.total_events,
            last_updated = EXCLUDED.last_updated
    """, (
        product_name,
        category,
        total_events,
        datetime.now()
    ))

conn.commit()

print("Analytics table updated")

cursor.close()
conn.close()