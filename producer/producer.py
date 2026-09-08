from kafka import KafkaProducer
import json
import time
import random
import uuid

producer = KafkaProducer(
    bootstrap_servers="kafka:29092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

events = [
    "view_product",
    "add_to_cart",
    "purchase"
]

while True:

    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 100),
        "product_id": random.randint(1, 20),
        "event": random.choice(events),
        "timestamp": time.time()
    }

    producer.send("ecommerce-events", value=event)

    print("Sent:", event)

    time.sleep(2)

