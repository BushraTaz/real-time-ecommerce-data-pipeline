from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

events = [
    "view_product",
    "add_to_cart",
    "purchase"
]

while True:

    event = {
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1, 20),
        "event": random.choice(events),
        "timestamp": time.time()
    }

    producer.send("ecommerce-events", value=event)

    print("Sent:", event)

    time.sleep(2)