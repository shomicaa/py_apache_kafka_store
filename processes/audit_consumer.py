import json
import os
from dotenv import load_dotenv

import psycopg2
from confluent_kafka import Consumer

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

BOOTSTRAP = os.getenv("BOOTSTRAP")
TOPIC_ORDERS = os.getenv("TEE_ORDERS")
TOPIC_INVENTORY = os.getenv("TEE_INVENTORY")

con = psycopg2.connect(
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", 6111)),
    dbname=os.getenv("DB_NAME", "postgres"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASS", "postgres")
)
con.autocommit = False

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "audit",
    "auto.offset.reset": "earliest"
})

print("[AUDIT] Audit consumer started.")


def on_assign(consumer, partitions):
    for p in partitions:
        print(f"[AUDIT] Assigned partition {p.partition} of topic '{p.topic}'")


consumer.subscribe([TOPIC_ORDERS, TOPIC_INVENTORY], on_assign=on_assign)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    raw = msg.value()
    topic = msg.topic()

    print(f"[AUDIT] Received message on topic={topic} partition={msg.partition()}")

    if not raw:
        print(f"[AUDIT] Empty message on {topic}, skipping.")
        continue

    try:
        event = json.loads(raw.decode("utf-8"))
        event_id = event.get("event_id")
        event_type = event.get("event_type")
        order_id = event.get("order_id")
        timestamp = event.get("timestamp")
        payload = json.dumps(event.get("payload", {}))
    except json.JSONDecodeError:
        event_id, event_type, order_id, timestamp = None, "UNPARSEABLE", None, None
        payload = raw.decode("utf-8", errors="replace")

    try:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO audit_log (event_id, event_type, order_id, ts, payload, source_topic)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (event_id, event_type, order_id, timestamp, payload, topic)
            )
        con.commit()
        print(f"[AUDIT] Logged event_id={event_id} type={event_type} topic={topic}")
    except psycopg2.Error as e:
        con.rollback()
        print(f"[AUDIT] DB ERROR: {e}")
