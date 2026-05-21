from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'ivg.events',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='validation-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

dlq_producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DLQ_TOPIC = "ivg.dlq"


def is_valid(event):
    required_fields = ["event_id", "event_type", "timestamp", "user_id", "payload"]

    for field in required_fields:
        if field not in event:
            return False

    return True


print("Validation consumer started...")

for message in consumer:
    event = message.value

    print("RECEIVED:", event)

    if is_valid(event):
        print("VALID EVENT")

        pass

    else:
        print("INVALID EVENT → SENDING TO DLQ")

        dlq_producer.send(DLQ_TOPIC, {
            "reason": "MISSING_FIELDS",
            "raw_event": event
        })