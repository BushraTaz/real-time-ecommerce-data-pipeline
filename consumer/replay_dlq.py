import psycopg2
import json
from consumer import process_event  # reuse your logic

conn = psycopg2.connect(
    database="airflow",
    user="airflow",
    password="airflow",
    host="localhost",
    port="5433"
)

cursor = conn.cursor()

cursor.execute("""
    SELECT id, event
    FROM dlq_events
    WHERE processed = FALSE
""")

rows = cursor.fetchall()

for row in rows:
    dlq_id, event = row
    event = json.loads(event)

    try:
        # reuse same processing logic (important interview pattern)
        fake_message = type("obj", (object,), {"value": event})
        process_event(fake_message)

        cursor.execute("""
            UPDATE dlq_events
            SET processed = TRUE
            WHERE id = %s
        """, (dlq_id,))

        conn.commit()

        print("Replayed:", dlq_id)

    except Exception as e:
        print("Still failing:", dlq_id, e)

cursor.close()
conn.close()