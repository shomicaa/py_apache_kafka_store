import json
import os
from dotenv import load_dotenv

import psycopg2
from confluent_kafka import Consumer

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))

BOOTSTRAP = os.getenv("BOOTSTRAP")
TOPIC_DLQ = os.getenv("TEE_DLQ")

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
    "group.id": "audit-dlq",
    "auto.offset.reset": "earliest"
})

print("[AUDIT-DLQ] DLQ consumer started.")


def on_assign(consumer, partitions):
    for p in partitions:
        print(f"[AUDIT-DLQ] Assigned partition {p.partition} of topic '{p.topic}'")


consumer.subscribe([TOPIC_DLQ], on_assign=on_assign)

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue

    raw = msg.value()

    print(f"[AUDIT-DLQ] Received message on partition {msg.partition()}")

    if not raw:
        print("[AUDIT-DLQ] Empty message, skipping.")
        continue

    try:
        event = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError:
        print(f"[AUDIT-DLQ] Non-JSON DLQ message: {raw[:200]}")
        continue

    event_id = event.get("event_id")
    order_id = event.get("order_id")
    timestamp = event.get("timestamp")
    payload = event.get("payload", {})
    reason = payload.get("reason")
    source_topic = payload.get("source_topic")
    raw_value = payload.get("raw_value")

    try:
        with con.cursor() as cur:
            cur.execute(
                """
                INSERT INTO dlq_events (event_id, order_id, ts, reason, source_topic, raw_value, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (event_id, order_id, timestamp, reason, source_topic, raw_value, json.dumps(payload))
            )
        con.commit()
        print(f"[AUDIT-DLQ] Persisted event_id={event_id} reason={reason} order_id={order_id}")
    except psycopg2.Error as e:
        con.rollback()
        print(f"[AUDIT-DLQ] DB ERROR: {e}")
