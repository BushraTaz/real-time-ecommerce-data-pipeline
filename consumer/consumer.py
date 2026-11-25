from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition
import json
from datetime import datetime
from psycopg2 import pool
import psycopg2
import os
import logging

# -------------------------
# LOGGING
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("kafka-consumer")

# -------------------------
# METRICS
# -------------------------
metrics = {"processed": 0, "failed": 0, "skipped": 0}

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
    "ecommerce-events",
    bootstrap_servers="kafka:29092",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id="ecommerce-consumer-group",
    value_deserializer=lambda value: value.decode("utf-8")
)

logger.info("Kafka consumer started")

# -------------------------
# DEAD-LETTER QUEUE
# -------------------------
def send_to_dlq(event, error):
    """Persist an event that cannot be processed; return whether it was saved."""
    conn = None
    cursor = None
    try:
        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO dlq_events (event, error)
            VALUES (%s, %s)
            """,
            (json.dumps(event), str(error))
        )
        conn.commit()
        return True
    except Exception:
        if conn:
            conn.rollback()
        logger.exception("Could not save event to DLQ")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


# -------------------------
# PROCESS ONE EVENT
# -------------------------
def process_event(message):
    """
    Process one Kafka message.
    Return True only when it is safely stored or deliberately recorded in the DLQ.
    """
    try:
        event = json.loads(message.value)
    except (json.JSONDecodeError, TypeError) as error:
        metrics["failed"] += 1
        logger.error("Invalid JSON message: %r | Error: %s", message.value, error)
        return send_to_dlq(message.value, error)

    conn = None
    cursor = None

    try:
        user_id = event.get("user_id")
        product_id = event.get("product_id")
        event_type = event.get("event")
        event_id = event.get("event_id")

        if not all((user_id, product_id, event_type, event_id)):
            error = "Event is missing one or more required fields"
            metrics["skipped"] += 1
            logger.warning("%s: %s", error, event)
            return send_to_dlq(event, error)

        now = datetime.now()
        path = f"data_lake/raw/year={now.year}/month={now.month}/day={now.day}"
        os.makedirs(path, exist_ok=True)
        with open(f"{path}/events.jsonl", "a", encoding="utf-8") as raw_file:
            raw_file.write(json.dumps(event) + "\n")

        conn = db_pool.getconn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT product_name, category, price
            FROM dim_products
            WHERE product_id = %s
            """,
            (product_id,)
        )
        product = cursor.fetchone()

        if not product:
            error = f"Product not found: {product_id}"
            metrics["skipped"] += 1
            logger.warning(error)
            conn.rollback()
            return send_to_dlq(event, error)

        product_name, category, _price = product
        cursor.execute(
            """
            INSERT INTO fact_events
                (event_id, user_id, product_id, event_type, event_time)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (event_id, user_id, product_id, event_type, datetime.now())
        )
        conn.commit()

        metrics["processed"] += 1
        logger.info(
            "Processed event=%s | %s | %s",
            event_id, product_name, category
        )
        if metrics["processed"] % 10 == 0:
            logger.info("METRICS: %s", metrics)
        return True

    except Exception as error:
        metrics["failed"] += 1
        if conn:
            conn.rollback()
        logger.exception("Failed to process event")
        return send_to_dlq(event, error)

    finally:
        if cursor:
            cursor.close()
        if conn:
            db_pool.putconn(conn)


# -------------------------
# STREAM LOOP
# -------------------------
# Keep KafkaConsumer operations on this main thread. Processing is sequential
# so an offset is committed only after its corresponding work has succeeded.
try:
    for message in consumer:
        if process_event(message):
            partition = TopicPartition(message.topic, message.partition)
            next_offset = OffsetAndMetadata(message.offset + 1, "")
            consumer.commit({partition: next_offset})
        else:
            logger.error(
                "Offset not committed; event remains available for retry: "
                "topic=%s partition=%s offset=%s",
                message.topic, message.partition, message.offset
            )
finally:
    consumer.close()
    db_pool.closeall()
