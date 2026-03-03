import json
import time
import uuid
import random
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

producer = Producer({"bootstrap.servers": os.getenv("BOOTSTRAP")})

TOPIC_ORDERS = os.getenv("TEE_ORDERS")

COLORS = ["black", "white", "red", "pink"]
SIZES = ["S", "M", "L", "XL"]

while True:
    order_id = str(uuid.uuid4())

    if random.random() < 0.2:
        producer.produce(TOPIC_ORDERS, key=order_id, value="NOT A JSON")
        producer.flush()
        print(f"[PRODUCER] Sent invalid message order_id={order_id}")
        time.sleep(0.001)
        continue

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": order_id,
        "payload": {
            "product_name": "shirt",
            "color": random.choice(COLORS),
            "size": random.choice(SIZES),
            "quantity": random.randint(1, 3)
        }
    }

    producer.produce(
        TOPIC_ORDERS,
        key=order_id,
        value=json.dumps(event)
    )
    producer.flush()

    print(f"[PRODUCER] ORDER_CREATED order_id={order_id} "
          f"color={event['payload']['color']} size={event['payload']['size']} "
          f"qty={event['payload']['quantity']}")

    time.sleep(0.001)
