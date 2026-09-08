import psycopg2

conn = psycopg2.connect(
    database="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)

print("Analytics PostgreSQL connection OK")

conn.close()
