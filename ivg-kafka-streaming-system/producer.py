from kafka import KafkaProducer
import json
import random
import uuid
from datetime import datetime, timezone
import time

TOPIC = "ivg.events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

EVENT_TYPES = [
    "artwork_viewed",
    "artwork_favorited",
    "search_performed",
    "recommendation_clicked",
    "artwork_created"
]

USERS = ["u1", "u2", "u3", "u4", "u5"]

ARTWORKS = [
    {"id": "a1", "title": "Starry Night", "tags": ["post-impressionism", "night", "swirl"]},
    {"id": "a2", "title": "The Scream", "tags": ["expressionism", "emotion", "anxiety"]},
    {"id": "a3", "title": "Mona Lisa", "tags": ["renaissance", "portrait", "mystery"]},
]


def generate_event():
    event_type = random.choice(EVENT_TYPES)
    user_id = random.choice(USERS)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": user_id,
        "payload": {}
    }

    if event_type in ["artwork_viewed", "artwork_favorited", "recommendation_clicked"]:
        art = random.choice(ARTWORKS)
        event["payload"] = {
            "artwork_id": art["id"],
            "artwork_title": art["title"],
            "tags": art["tags"]
        }

    if event_type == "artwork_viewed":
        event["payload"].update({
            "duration_seconds": random.randint(5, 180),
            "engagement_level": random.choice(["low", "medium", "high"])
        })

    elif event_type == "search_performed":
        event["payload"] = {
            "query": random.choice([
                "impressionist paintings Van Gogh style",
                "renaissance portrait Mona Lisa analysis",
                "surrealism Salvador Dalí clocks",
                "baroque portrait Vermeer",
                "modern art museum collection"
            ])
        }

    elif event_type == "artwork_created":
        event["payload"] = {
            "artwork_id": "new_" + str(random.randint(100, 999)),
            "title": random.choice([
                "Renaissance Study",
                "Impressionist Light Study",
                "Abstract Digital Piece"
            ]),
            "style_prompt": random.choice([
                "museum quality scan",
                "high resolution artwork",
                "digital restoration"
            ]),
            "tags": random.choice(ARTWORKS)["tags"]
        }

    if random.random() < 0.2:
        event.pop("user_id", None)
        print("Generated INVALID event")

    return event


def send_events():
    print("Producer started... sending events to Kafka")

    while True:
        event = generate_event()

        key = (event.get("user_id") or "unknown").encode("utf-8")

        producer.send(
            TOPIC,
            key=key,
            value=event
        )

        print(
            f"Sent: {event.get('event_type', 'UNKNOWN')} "
            f"| user={event.get('user_id', 'unknown')}"
        )

        time.sleep(1)


if __name__ == "__main__":
    send_events()