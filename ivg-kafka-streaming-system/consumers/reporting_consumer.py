from kafka import KafkaConsumer
import json
import psycopg2


consumer = KafkaConsumer(
    'ivg.events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='reporting-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres"
)

cursor = conn.cursor()


def save_event(event):
    try:
        cursor.execute("""
            INSERT INTO ivg_events (event_id, event_type, user_id, timestamp, payload)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """, (
            event["event_id"],
            event["event_type"],
            event["user_id"],
            event["timestamp"],
            json.dumps(event["payload"])
        ))

        conn.commit()

        print("SAVED:", event["event_id"])

    except Exception as e:
        print("❌ DB ERROR:", e)
        conn.rollback()


print("Reporting consumer started...")

for message in consumer:
    event = message.value

    save_event(event)