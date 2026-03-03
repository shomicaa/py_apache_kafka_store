import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.getenv("BOOTSTRAP")
TOPIC_INVENTORY = os.getenv("TEE_INVENTORY")
TOPIC_DLQ = os.getenv("TEE_DLQ")

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shop.db")

con = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
con.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        event_id   TEXT PRIMARY KEY,
        event_type TEXT NOT NULL,
        order_id   TEXT NOT NULL,
        ts         TEXT NOT NULL,
        payload    TEXT NOT NULL
    )
""")
con.execute("""
    CREATE TABLE IF NOT EXISTS order_status (
        order_id     TEXT PRIMARY KEY,
        status       TEXT NOT NULL,
        product_name TEXT NOT NULL,
        quantity     INTEGER NOT NULL,
        reason       TEXT,
        updated_at   TEXT NOT NULL
    )
""")
con.commit()

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "orders-db",
    "auto.offset.reset": "earliest"
})

producer = Producer({"bootstrap.servers": BOOTSTRAP})

print("[CONSUMER] Order persistence service started.")


def on_assign(consumer, partitions):
    for p in partitions:
        print(f"[CONSUMER] Assigned partition {p.partition} of topic '{p.topic}'")


consumer.subscribe([TOPIC_INVENTORY], on_assign=on_assign)


def send_to_dlq(reason: str, raw_value, order_id: str = None):
    dlq_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "DLQ_EVENT",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "order_id": order_id,
        "payload": {
            "source_topic": TOPIC_INVENTORY,
            "reason": reason,
            "raw_value": (raw_value.decode("utf-8", errors="replace") if raw_value else None)
        }
    }
    producer.produce(TOPIC_DLQ, value=json.dumps(dlq_event))
    producer.flush()
    print(f"[CONSUMER] -> DLQ reason={reason} order_id={order_id}")


while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    print(f"[CONSUMER] Received message on partition {msg.partition()}")

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
        event_id = event["event_id"]
        event_type = event["event_type"]
        order_id = event["order_id"]
        timestamp = event["timestamp"]
        payload = event["payload"]
        product_name = payload["product_name"]
        quantity = int(payload["quantity"])
    except (KeyError, ValueError):
        send_to_dlq("MISSING_OR_BAD_FIELDS", raw, event.get("order_id"))
        continue

    reason = payload.get("reason")

    try:
        con.execute(
            "INSERT OR IGNORE INTO orders (event_id, event_type, order_id, ts, payload) VALUES (?, ?, ?, ?, ?)",
            (event_id, event_type, order_id, timestamp, json.dumps(payload))
        )
        con.execute("""
            INSERT INTO order_status (order_id, status, product_name, quantity, reason, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                status=excluded.status,
                reason=excluded.reason,
                updated_at=excluded.updated_at
        """, (order_id, event_type, product_name, quantity, reason, timestamp))
        con.commit()
        print(f"[CONSUMER] PERSISTED event_id={event_id} type={event_type} order_id={order_id}")
    except sqlite3.Error as e:
        print(f"[CONSUMER] DB ERROR event_id={event_id}: {e}")
