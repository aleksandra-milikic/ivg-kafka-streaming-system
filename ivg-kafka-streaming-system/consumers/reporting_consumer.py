from kafka import KafkaConsumer
import json
import psycopg2
from psycopg2.extras import Json
import time

consumer = KafkaConsumer(
    "ivg.events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="reporting-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)


def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="postgres",
        port=5432
    )


conn = get_connection()
cursor = conn.cursor()


def save_event(event):
    global conn, cursor

    try:
        if not isinstance(event, dict):
            return

        event_id = event.get("event_id")
        if not event_id:
            print("Skipping event without event_id")
            return

        event_type = event.get("event_type")
        user_id = event.get("user_id")
        timestamp = event.get("timestamp")
        payload = event.get("payload") or {}

        cursor.execute("""
            INSERT INTO ivg_events (
                event_id,
                event_type,
                user_id,
                timestamp,
                payload
            )
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (event_id) DO NOTHING
        """, (
            event_id,
            event_type,
            user_id,
            timestamp,
            json.dumps(payload)
        ))

        conn.commit()

        print(f"SAVED: {event_id}")

    except Exception as e:
        print(f"DB ERROR: {e}")

        try:
            conn = get_connection()
            cursor = conn.cursor()
        except Exception as reconnect_error:
            print(f"RECONNECT FAILED: {reconnect_error}")


print("Reporting consumer started...")

while True:
    try:
        for message in consumer:
            save_event(message.value)

    except Exception as e:
        print(f"Consumer crashed loop: {e}")
        time.sleep(2)