from kafka import KafkaProducer
import json
import random
import uuid
from datetime import datetime
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
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
    {
        "id": "a1",
        "title": "Starry Night",
        "tags": ["post-impressionism", "night-sky", "swirls"]
    },
    {
        "id": "a2",
        "title": "The Scream",
        "tags": ["expressionism", "emotion", "anxiety"]
    },
    {
        "id": "a3",
        "title": "Mona Lisa",
        "tags": ["renaissance", "portrait", "mystery"]
    },
    {
        "id": "a4",
        "title": "The Persistence of Memory",
        "tags": ["surrealism", "clocks", "dream"]
    },
    {
        "id": "a5",
        "title": "Girl with a Pearl Earring",
        "tags": ["baroque", "portrait", "light"]
    }
]


def generate_event():
    event_type = random.choice(EVENT_TYPES)
    user_id = random.choice(USERS)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat(),
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

    
    if event_type == "search_performed":
        event["payload"] = {
            "query": random.choice([
                "impressionist paintings Van Gogh style",
                "renaissance portrait Mona Lisa analysis",
                "surrealism Salvador Dalí melting clocks",
                "baroque portrait Vermeer Girl with Pearl Earring",
                "post-impressionism night sky Starry Night",
                "famous classical art museum collection",
                "Leonardo da Vinci renaissance artworks"
            ])
        }

    
    if event_type == "artwork_created":
        event["payload"] = {
            "artwork_id": "new_" + str(random.randint(100, 999)),
            "title": random.choice([
                "Restored Renaissance Portrait",
                "Impressionist Landscape Study",
                "Baroque Light Composition",
                "Classical Sculpture Digital Archive Entry",
                "Museum Catalog Reproduction"
            ]),
            "style_prompt": random.choice([
                "museum quality restoration scan",
                "high-resolution archival reproduction",
                "oil painting texture preserved",
                "historical art digitization",
                "fine art gallery presentation style"
            ]),
            "tags": random.choice(ARTWORKS)["tags"]
        }


    return event


def send_events():
    print("Producer started... sending AI art events to Kafka")

    while True:
        event = generate_event()

        producer.send(
            'ivg.events',
            key=event["user_id"].encode('utf-8'),
            value=event
        )

        print(f"Sent: {event['event_type']} | {event['payload'].get('artwork_title', event['payload'].get('query', 'N/A'))}")

        time.sleep(1)


if __name__ == "__main__":
    send_events()