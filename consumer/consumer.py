from kafka import KafkaConsumer
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from psycopg2 import pool
import psycopg2
import os
import logging

# -------------------------
# LOGGING (PRODUCTION STYLE)
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("kafka-consumer")

# -------------------------
# METRICS
# -------------------------
metrics = {
    "processed": 0,
    "failed": 0,
    "skipped": 0
}

# -------------------------
# DB CONNECTION POOL
# -------------------------
db_pool = psycopg2.pool.SimpleConnectionPool(
    1, 20,
    database="airflow",
    user="airflow",
    password="airflow",
    host="postgres",
    port="5432"
)

# -------------------------
# KAFKA CONSUMER
# -------------------------
consumer = KafkaConsumer(
    'ecommerce-events',
    bootstrap_servers='kafka:29092',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    group_id='ecommerce-consumer-group',
value_deserializer=lambda x: x.decode('utf-8'))

logger.info("🚀 Enterprise consumer running...")

# -------------------------
# DLQ (PRODUCTION READY)
# -------------------------
def send_to_dlq(event, error):
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO dlq_events (event, error)
            VALUES (%s, %s)
        """, (
            json.dumps(event),
            str(error)
        ))

        conn.commit()
        cursor.close()
        db_pool.putconn(conn)

    except Exception as dlq_error:
        logger.error(f"DLQ FAILED: {dlq_error}")

# -------------------------
# PROCESS EVENT
# -------------------------
def process_event(message):

    try:
        event = json.loads(message.value)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON message: {message.value!r} | Error: {e}")
        metrics["failed"] += 1
        return
    conn = None
    cursor = None

    try:
        user_id = event.get("user_id")
        product_id = event.get("product_id")
        event_type = event.get("event")
        event_id = event.get("event_id")  # ✅ REQUIRED FIX

        if not (user_id and product_id and event_type and event_id):
            metrics["skipped"] += 1
            return

        # -------------------------
        # DATA LAKE (RAW STORAGE)
        # -------------------------
        now = datetime.now()
        path = f"data_lake/raw/year={now.year}/month={now.month}/day={now.day}"
        os.makedirs(path, exist_ok=True)

        file_path = f"{path}/events.jsonl"
        with open(file_path, "a") as f:
            f.write(json.dumps(event) + "\n")

        # -------------------------
        # DB CONNECTION
        # -------------------------
        conn = db_pool.getconn()
        cursor = conn.cursor()

        # -------------------------
        # ENRICHMENT STEP
        # -------------------------
        cursor.execute("""
            SELECT product_name, category, price
            FROM dim_products
            WHERE product_id = %s
        """, (product_id,))

        product = cursor.fetchone()

        if not product:
            logger.warning(f"Product not found: {product_id}")
            metrics["skipped"] += 1
            return

        product_name, category, price = product

        # -------------------------
        # FACT INSERT (IDEMPOTENT)
        # -------------------------
        cursor.execute("""
            INSERT INTO fact_events
            (event_id, user_id, product_id, event_type, event_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, (
            event_id,
            user_id,
            product_id,
            event_type,
            datetime.now()
        ))

        conn.commit()

        # Kafka commit AFTER success
        consumer.commit()

        metrics["processed"] += 1
        logger.info(f"Processed event={event_id} | {product_name} | {category}")

        # periodic metrics log
        if metrics["processed"] % 10 == 0:
            logger.info(f"METRICS: {metrics}")

    except Exception as e:
        metrics["failed"] += 1

        if conn:
            conn.rollback()

        send_to_dlq(event, e)
        logger.error(f"FAILED EVENT: {e}")

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)

# -------------------------
# THREAD POOL
# -------------------------
executor = ThreadPoolExecutor(max_workers=10)

logger.info("Consumer ready with pooling + DLQ + concurrency")

# -------------------------
# STREAM LOOP
# -------------------------
for message in consumer:
    executor.submit(process_event, message)