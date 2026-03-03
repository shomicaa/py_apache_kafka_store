import json
import uuid
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

BOOTSTRAP = os.getenv("BOOTSTRAP")
TOPIC_ORDERS = os.getenv("TEE_ORDERS")
TOPIC_INVENTORY = os.getenv("TEE_INVENTORY")
TOPIC_DLQ = os.getenv("TEE_DLQ")

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "inventory",
    "auto.offset.reset": "latest"
})

producer = Producer({"bootstrap.servers": BOOTSTRAP})

COLORS = ["black", "white", "red", "pink"]
SIZES = ["S", "M", "L", "XL"]
stock = {f"{color}_{size}": 50 for color in COLORS for size in SIZES}

print("[PRODUCER-CONSUMER] Inventory service started.")
print("[PRODUCER-CONSUMER] Initial stock:", stock)


def on_assign(consumer, partitions):
    for p in partitions:
        print(f"[PRODUCER-CONSUMER] Assigned partition {p.partition} of topic '{p.topic}'")


consumer.subscribe([TOPIC_ORDERS], on_assign=on_assign)


def send_to_dlq(reason: str, raw_value, order_id: str = None):
    dlq_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "DLQ_EVENT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": order_id,
        "payload": {
            "source_topic": TOPIC_ORDERS,
            "reason": reason,
            "raw_value": (raw_value.decode("utf-8", errors="replace") if raw_value else None)
        }
    }
    producer.produce(TOPIC_DLQ, value=json.dumps(dlq_event))
    producer.flush()
    print(f"[PRODUCER-CONSUMER] -> DLQ reason={reason} order_id={order_id}")


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    print(f"[PRODUCER-CONSUMER] Received message on partition {msg.partition()}")

    raw = msg.value()

    if not raw:
        send_to_dlq("EMPTY_VALUE", raw)
        continue

    try:
        event = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        send_to_dlq("INVALID_JSON", raw)
        continue

    try:
        order_id = event["order_id"]
        payload = event["payload"]
        color = payload["color"]
        size = payload["size"]
        quantity = int(payload["quantity"])
        product_name = payload["product_name"]
    except (KeyError, ValueError):
        send_to_dlq("MISSING_OR_BAD_FIELDS", raw, event.get("order_id"))
        continue

    stock_key = f"{color}_{size}"

    if stock.get(stock_key, 0) >= quantity:
        stock[stock_key] -= quantity
        outcome_type = "INVENTORY_RESERVED"
        reason = None
        print(f"[PRODUCER-CONSUMER] ORDER OK order_id={order_id} {color}/{size} qty={quantity} stock_after={stock[stock_key]}")
    else:
        outcome_type = "INVENTORY_REJECTED"
        reason = f"OUT_OF_STOCK have={stock.get(stock_key, 0)} need={quantity}"
        print(f"[PRODUCER-CONSUMER] ORDER FAIL order_id={order_id} {color}/{size} ({reason})")

    outcome_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": outcome_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": order_id,
        "payload": {
            "product_name": product_name,
            "color": color,
            "size": size,
            "quantity": quantity,
            "reason": reason,
            "stock_after": stock.get(stock_key, 0)
        }
    }

    producer.produce(
        TOPIC_INVENTORY,
        key=order_id,
        value=json.dumps(outcome_event)
    )
    producer.flush()
