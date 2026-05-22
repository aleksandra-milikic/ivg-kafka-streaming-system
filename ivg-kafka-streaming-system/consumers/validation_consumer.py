from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    "ivg.events",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="validation-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

DLQ_TOPIC = "ivg.dlq"
VALID_TOPIC = "ivg.valid"


def is_valid(event):
    if not isinstance(event, dict):
        return False

    required_fields = ["event_id", "event_type", "timestamp", "user_id", "payload"]

    for field in required_fields:
        if field not in event or event[field] is None:
            return False

    if not isinstance(event.get("payload"), dict):
        return False

    return True


print("Validation consumer started...")

for message in consumer:
    try:
        event = message.value

        event_id = event.get("event_id", "unknown")
        event_type = event.get("event_type", "unknown")

        print(f"RECEIVED: {event_type} | {event_id}")

        if is_valid(event):
            print(f"VALID EVENT → forwarding to {VALID_TOPIC}")

            producer.send(VALID_TOPIC, event)

        else:
            print(f"INVALID EVENT → sending to {DLQ_TOPIC}")

            producer.send(DLQ_TOPIC, {
                "reason": "MISSING_OR_INVALID_FIELDS",
                "raw_event": event
            })

        producer.flush()

    except Exception as e:
        print(f"Validation error: {e}")